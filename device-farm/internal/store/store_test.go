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
	queryPages [][]model.RunRecord
	putErr     error
	updateErr  error
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

func (f *fakeDynamoAPI) DeleteItem(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
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

func TestClaimWorkflowRun(t *testing.T) {
	fake := &fakeDynamoAPI{}
	st := New(fake, "runs-table")

	key := model.ClaimKey("maplibre/mln-rs", 123, 2)
	if err := st.ClaimWorkflowRun(context.Background(), key, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("ClaimWorkflowRun() error = %v", err)
	}
	if aws.ToString(fake.lastPut.ConditionExpression) != "attribute_not_exists(run_arn)" {
		t.Fatalf("ConditionExpression = %q, want attribute_not_exists(run_arn)", aws.ToString(fake.lastPut.ConditionExpression))
	}
	if _, hasBranch := fake.lastPut.Item["branch_key"]; hasBranch {
		t.Fatal("claim item should omit branch_key")
	}
	if _, hasRepoSHA := fake.lastPut.Item["repo_sha"]; hasRepoSHA {
		t.Fatal("claim item should omit repo_sha")
	}

	fake.putErr = &types.ConditionalCheckFailedException{Message: aws.String("exists")}
	err := st.ClaimWorkflowRun(context.Background(), key, time.Now())
	if !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("expected ErrAlreadyClaimed, got %v", err)
	}
}
