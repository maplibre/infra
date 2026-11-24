package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/maplibre/infra/device-farm/internal/model"
)

const (
	StateIndex     = "state-index"
	BranchKeyIndex = "branch_key-index"
	SHAIndex       = "sha-index"
	RecordTTL      = 30 * 24 * time.Hour
)

// ErrStateConflict is returned when a conditional state transition fails because
// the record is no longer in the expected state.
var ErrStateConflict = errors.New("run record state conflict")

// ErrAlreadyClaimed is returned when another invocation already claimed a workflow run attempt.
var ErrAlreadyClaimed = errors.New("workflow run attempt already claimed")

// DynamoAPI is the subset of the AWS DynamoDB client used by Store.
type DynamoAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// Store persists Device Farm run state in DynamoDB.
type Store struct {
	client    DynamoAPI
	tableName string
}

// New creates a Store backed by tableName.
func New(client DynamoAPI, tableName string) *Store {
	return &Store{client: client, tableName: tableName}
}

// ClaimWorkflowRun atomically claims a (repo, workflow_run_id, run_attempt) key in DynamoDB
// with a 30-day TTL so concurrent or duplicate SQS deliveries cannot schedule duplicate runs.
func (s *Store) ClaimWorkflowRun(ctx context.Context, claimKey string, now time.Time) error {
	nowUTC := now.UTC()
	av, err := attributevalue.MarshalMap(model.RunRecord{
		RunARN:    claimKey,
		State:     model.StateClaimed,
		CreatedAt: nowUTC.Format(time.RFC3339Nano),
		ExpiresAt: nowUTC.Add(RecordTTL).Unix(),
	})
	if err != nil {
		return fmt.Errorf("marshal claim record: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.tableName),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(run_arn)"),
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return ErrAlreadyClaimed
		}
		return fmt.Errorf("claim %s: %w", claimKey, err)
	}
	return nil
}

// PutRun writes or overwrites a RunRecord.
func (s *Store) PutRun(ctx context.Context, rec model.RunRecord) error {
	av, err := attributevalue.MarshalMap(rec)
	if err != nil {
		return fmt.Errorf("marshal run record: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      av,
	})
	if err != nil {
		return fmt.Errorf("put run %s: %w", rec.RunARN, err)
	}
	return nil
}

// GetRun retrieves a RunRecord by its primary key run_arn.
func (s *Store) GetRun(ctx context.Context, runARN string) (model.RunRecord, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"run_arn": &types.AttributeValueMemberS{Value: runARN},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return model.RunRecord{}, false, fmt.Errorf("get run %s: %w", runARN, err)
	}
	if len(out.Item) == 0 {
		return model.RunRecord{}, false, nil
	}
	var rec model.RunRecord
	if err := attributevalue.UnmarshalMap(out.Item, &rec); err != nil {
		return model.RunRecord{}, false, fmt.Errorf("unmarshal run %s: %w", runARN, err)
	}
	return rec, true, nil
}

// DeleteRun deletes a record by run_arn (used to clean up temporary pending tracking items).
func (s *Store) DeleteRun(ctx context.Context, runARN string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"run_arn": &types.AttributeValueMemberS{Value: runARN},
		},
	})
	if err != nil {
		return fmt.Errorf("delete run %s: %w", runARN, err)
	}
	return nil
}

// QueryActiveRuns lists all records currently in state ACTIVE via state-index.
func (s *Store) QueryActiveRuns(ctx context.Context) ([]model.RunRecord, error) {
	return s.queryAll(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.tableName),
		IndexName:              aws.String(StateIndex),
		KeyConditionExpression: aws.String("#s = :active"),
		ExpressionAttributeNames: map[string]string{
			"#s": "state",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":active": &types.AttributeValueMemberS{Value: string(model.StateActive)},
		},
	})
}

// QueryActiveByBranchKey returns ACTIVE runs matching branchKey via branch_key-index.
func (s *Store) QueryActiveByBranchKey(ctx context.Context, branchKey string) ([]model.RunRecord, error) {
	if branchKey == "" {
		return nil, nil
	}
	return s.queryAll(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.tableName),
		IndexName:              aws.String(BranchKeyIndex),
		KeyConditionExpression: aws.String("branch_key = :bk"),
		FilterExpression:       aws.String("#s = :active"),
		ExpressionAttributeNames: map[string]string{
			"#s": "state",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":bk":     &types.AttributeValueMemberS{Value: branchKey},
			":active": &types.AttributeValueMemberS{Value: string(model.StateActive)},
		},
	})
}

// QueryByRepoSHA returns all records for repo+sha ordered newest first via sha-index.
func (s *Store) QueryByRepoSHA(ctx context.Context, repo, sha string) ([]model.RunRecord, error) {
	return s.queryAll(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.tableName),
		IndexName:              aws.String(SHAIndex),
		KeyConditionExpression: aws.String("repo_sha = :rs"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":rs": &types.AttributeValueMemberS{Value: model.RepoSHAKey(repo, sha)},
		},
		ScanIndexForward: aws.Bool(false),
	})
}

// MarkSuperseded transitions an ACTIVE run to SUPERSEDED atomically.
func (s *Store) MarkSuperseded(ctx context.Context, runARN string, completedAt time.Time) error {
	return s.transitionFromActive(ctx, runARN, model.StateSuperseded, "STOPPED", completedAt)
}

// MarkCompleted transitions an ACTIVE run to COMPLETED atomically.
func (s *Store) MarkCompleted(ctx context.Context, runARN, result string, completedAt time.Time) error {
	return s.transitionFromActive(ctx, runARN, model.StateCompleted, result, completedAt)
}

// MarkFailed transitions an ACTIVE run to FAILED atomically.
func (s *Store) MarkFailed(ctx context.Context, runARN, result string, completedAt time.Time) error {
	return s.transitionFromActive(ctx, runARN, model.StateFailed, result, completedAt)
}

func (s *Store) transitionFromActive(ctx context.Context, runARN string, nextState model.RunState, result string, completedAt time.Time) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"run_arn": &types.AttributeValueMemberS{Value: runARN},
		},
		ConditionExpression: aws.String("attribute_exists(run_arn) AND #s = :active"),
		UpdateExpression:    aws.String("SET #s = :next, completed_at = :done, #r = :res"),
		ExpressionAttributeNames: map[string]string{
			"#s": "state",
			"#r": "result",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":active": &types.AttributeValueMemberS{Value: string(model.StateActive)},
			":next":   &types.AttributeValueMemberS{Value: string(nextState)},
			":done":   &types.AttributeValueMemberS{Value: completedAt.UTC().Format(time.RFC3339Nano)},
			":res":    &types.AttributeValueMemberS{Value: result},
		},
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return ErrStateConflict
		}
		return fmt.Errorf("transition run %s to %s: %w", runARN, nextState, err)
	}
	return nil
}

func (s *Store) queryAll(ctx context.Context, base *dynamodb.QueryInput) ([]model.RunRecord, error) {
	var results []model.RunRecord
	var lastKey map[string]types.AttributeValue
	for {
		in := *base
		in.ExclusiveStartKey = lastKey
		out, err := s.client.Query(ctx, &in)
		if err != nil {
			return nil, fmt.Errorf("query %s: %w", aws.ToString(base.IndexName), err)
		}
		var page []model.RunRecord
		if err := attributevalue.UnmarshalListOfMaps(out.Items, &page); err != nil {
			return nil, fmt.Errorf("unmarshal query results: %w", err)
		}
		results = append(results, page...)
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		lastKey = out.LastEvaluatedKey
	}
	return results, nil
}
