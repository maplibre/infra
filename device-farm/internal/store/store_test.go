package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/maplibre/infra/device-farm/internal/model"
)

type fakeDynamoAPI struct {
	lastPut    *dynamodb.PutItemInput
	lastQuery  *dynamodb.QueryInput
	lastUpdate *dynamodb.UpdateItemInput
	lastDelete *dynamodb.DeleteItemInput
	queryPages [][]model.RunRecord
	putErr     error
	updateErr  error
	deleteErr  error
}

func (f *fakeDynamoAPI) PutItem(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.lastPut = params
	return &dynamodb.PutItemOutput{}, f.putErr
}

func (f *fakeDynamoAPI) GetItem(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func (f *fakeDynamoAPI) UpdateItem(_ context.Context, params *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.lastUpdate = params
	return &dynamodb.UpdateItemOutput{}, f.updateErr
}

func (f *fakeDynamoAPI) DeleteItem(_ context.Context, params *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.lastDelete = params
	return &dynamodb.DeleteItemOutput{}, f.deleteErr
}

func (f *fakeDynamoAPI) Query(_ context.Context, params *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.lastQuery = params
	if len(f.queryPages) == 0 {
		return &dynamodb.QueryOutput{}, nil
	}
	page := f.queryPages[0]
	f.queryPages = f.queryPages[1:]

	items := make([]map[string]types.AttributeValue, 0, len(page))
	for _, rec := range page {
		m, err := attributevalue.MarshalMap(rec)
		if err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	out := &dynamodb.QueryOutput{Items: items}
	if len(f.queryPages) > 0 {
		out.LastEvaluatedKey = map[string]types.AttributeValue{
			"run_arn": &types.AttributeValueMemberS{Value: "next"},
		}
	}
	return out, nil
}

func TestStoreQueriesAndTransitions(t *testing.T) {
	fake := &fakeDynamoAPI{
		queryPages: [][]model.RunRecord{
			{{RunARN: "arn:aws:devicefarm:us-west-2:1:run:p/1", State: model.StateActive}},
			{{RunARN: "arn:aws:devicefarm:us-west-2:1:run:p/2", State: model.StateActive}},
		},
	}
	st := New(fake, "runs-table")

	recs, err := st.QueryActiveRuns(context.Background())
	if err != nil {
		t.Fatalf("QueryActiveRuns() error = %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("len(recs) = %d, want 2 (paginated)", len(recs))
	}
	if aws.ToString(fake.lastQuery.IndexName) != StateIndex {
		t.Fatalf("IndexName = %q, want %q", aws.ToString(fake.lastQuery.IndexName), StateIndex)
	}

	fake.updateErr = &types.ConditionalCheckFailedException{Message: aws.String("conflict")}
	err = st.MarkSuperseded(context.Background(), "arn:aws:devicefarm:us-west-2:1:run:p/1", time.Now())
	if !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict, got %v", err)
	}
}

func TestClaimWorkflowRunLeaseLifecycle(t *testing.T) {
	fake := &fakeDynamoAPI{}
	st := New(fake, "runs-table")

	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	key := model.ClaimKey("maplibre/mln-rs", 123, 2)
	owner := "owner-token-1"

	if err := st.ClaimWorkflowRun(context.Background(), key, owner, now); err != nil {
		t.Fatalf("ClaimWorkflowRun() error = %v", err)
	}
	wantCond := "attribute_not_exists(run_arn) OR (#state = :claiming AND lease_expires_at < :now)"
	if aws.ToString(fake.lastPut.ConditionExpression) != wantCond {
		t.Fatalf("ConditionExpression = %q, want %q", aws.ToString(fake.lastPut.ConditionExpression), wantCond)
	}
	var item model.RunRecord
	if err := attributevalue.UnmarshalMap(fake.lastPut.Item, &item); err != nil {
		t.Fatalf("unmarshal claim item: %v", err)
	}
	if item.State != model.StateClaiming || item.ClaimOwner != owner {
		t.Fatalf("claim item state=%q owner=%q, want CLAIMING/%q", item.State, item.ClaimOwner, owner)
	}
	if item.LeaseExpiresAt != now.Add(ClaimLeaseTTL).Unix() {
		t.Fatalf("LeaseExpiresAt = %d, want %d", item.LeaseExpiresAt, now.Add(ClaimLeaseTTL).Unix())
	}
	if _, hasBranch := fake.lastPut.Item["branch_key"]; hasBranch {
		t.Fatal("claim item should omit branch_key")
	}
	if _, hasRepoSHA := fake.lastPut.Item["repo_sha"]; hasRepoSHA {
		t.Fatal("claim item should omit repo_sha")
	}

	fake.putErr = &types.ConditionalCheckFailedException{Message: aws.String("exists")}
	if err := st.ClaimWorkflowRun(context.Background(), key, owner, now); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("expected ErrAlreadyClaimed, got %v", err)
	}
	fake.putErr = nil

	if err := st.CompleteWorkflowClaim(context.Background(), key, owner, now.Add(time.Minute)); err != nil {
		t.Fatalf("CompleteWorkflowClaim() error = %v", err)
	}
	if aws.ToString(fake.lastUpdate.ConditionExpression) != "#state = :claiming AND claim_owner = :owner" {
		t.Fatalf("CompleteWorkflowClaim ConditionExpression = %q", aws.ToString(fake.lastUpdate.ConditionExpression))
	}
	if aws.ToString(fake.lastUpdate.UpdateExpression) != "SET #state = :claimed, updated_at = :updated_at, expires_at = :expires_at REMOVE lease_expires_at" {
		t.Fatalf("CompleteWorkflowClaim UpdateExpression = %q", aws.ToString(fake.lastUpdate.UpdateExpression))
	}

	fake.updateErr = &types.ConditionalCheckFailedException{Message: aws.String("wrong owner")}
	if err := st.CompleteWorkflowClaim(context.Background(), key, "other-owner", now); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("expected ErrStateConflict on mismatched CompleteWorkflowClaim, got %v", err)
	}
	fake.updateErr = nil

	if err := st.ReleaseWorkflowClaim(context.Background(), key, owner); err != nil {
		t.Fatalf("ReleaseWorkflowClaim() error = %v", err)
	}
	if aws.ToString(fake.lastDelete.ConditionExpression) != "#state = :claiming AND claim_owner = :owner" {
		t.Fatalf("ReleaseWorkflowClaim ConditionExpression = %q", aws.ToString(fake.lastDelete.ConditionExpression))
	}

	// Ownership mismatch or already finalized CLAIMED must not return an error on release.
	fake.deleteErr = &types.ConditionalCheckFailedException{Message: aws.String("taken over")}
	if err := st.ReleaseWorkflowClaim(context.Background(), key, "stale-owner"); err != nil {
		t.Fatalf("expected nil on ConditionalCheckFailedException in ReleaseWorkflowClaim, got %v", err)
	}
}
