package model

import (
	"fmt"
	"strings"
)

// EventKind identifies the GitHub webhook event carried on SQS.
type EventKind string

const (
	EventWorkflowJob EventKind = "workflow_job"
	EventWorkflowRun EventKind = "workflow_run"
	EventCheckRun    EventKind = "check_run"
)

// JobMessage is the compact SQS payload passed from webhook to worker.
type JobMessage struct {
	Event          EventKind `json:"event"`
	Action         string    `json:"action"`
	DeliveryID     string    `json:"delivery_id,omitempty"`
	Repo           string    `json:"repo"`
	InstallationID int64     `json:"installation_id"`
	HeadSHA        string    `json:"head_sha"`
	HeadBranch     string    `json:"head_branch,omitempty"`
	HeadRepo       string    `json:"head_repo,omitempty"`
	WorkflowRunID  int64     `json:"workflow_run_id,omitempty"`
	WorkflowJobID  int64     `json:"workflow_job_id,omitempty"`
	JobName        string    `json:"job_name,omitempty"`
	JobConclusion  string    `json:"job_conclusion,omitempty"`
	RunConclusion  string    `json:"run_conclusion,omitempty"`
	CheckRunID     int64     `json:"check_run_id,omitempty"`
	ExternalID     string    `json:"external_id,omitempty"`
}

// RunState tracks the lifecycle of a DynamoDB run record.
type RunState string

const (
	// StatePending tracks a Check Run created during workflow_job queued/in_progress
	// before a Device Farm run has been scheduled.
	StatePending RunState = "PENDING"
	// StateActive marks a Device Farm run that the poller is watching.
	StateActive RunState = "ACTIVE"
	// StateCompleted marks a finished Device Farm run whose Check Run was settled.
	StateCompleted RunState = "COMPLETED"
	// StateSuperseded marks an active run stopped because a newer commit arrived.
	StateSuperseded RunState = "SUPERSEDED"
	// StateSkipped marks a run whose trigger job was skipped by CI.
	StateSkipped RunState = "SKIPPED"
	// StateBuildFailed marks a run whose trigger job failed/cancelled before Device Farm.
	StateBuildFailed RunState = "BUILD_FAILED"
	// StateFailed marks a run that failed unexpectedly (e.g. deleted from Device Farm).
	StateFailed RunState = "FAILED"
)

// RunRecord is stored in DynamoDB table maplibre-device-farm-runs.
type RunRecord struct {
	RunARN         string   `dynamodbav:"run_arn"`
	State          RunState `dynamodbav:"state"`
	CreatedAt      string   `dynamodbav:"created_at"`
	BranchKey      string   `dynamodbav:"branch_key,omitempty"`
	RepoSHA        string   `dynamodbav:"repo_sha"`
	Repo           string   `dynamodbav:"repo"`
	SHA            string   `dynamodbav:"sha"`
	HeadBranch     string   `dynamodbav:"head_branch,omitempty"`
	HeadRepo       string   `dynamodbav:"head_repo,omitempty"`
	CheckRunID     int64    `dynamodbav:"check_run_id"`
	CheckStatus    string   `dynamodbav:"check_status,omitempty"`
	WorkflowRunID  int64    `dynamodbav:"workflow_run_id"`
	InstallationID int64    `dynamodbav:"installation_id"`
	CompletedAt    string   `dynamodbav:"completed_at,omitempty"`
	Result         string   `dynamodbav:"result,omitempty"`
	ExpiresAt      int64    `dynamodbav:"expires_at"`
}

// PendingRunKey returns the synthetic DynamoDB primary key used before a Device Farm
// run ARN exists for a workflow run.
func PendingRunKey(repo string, workflowRunID int64) string {
	return fmt.Sprintf("pending:%s:%d", strings.ToLower(repo), workflowRunID)
}

// IsDeviceFarmARN reports whether runARN is a real AWS Device Farm run ARN.
func IsDeviceFarmARN(runARN string) bool {
	return strings.HasPrefix(runARN, "arn:aws:devicefarm:")
}

// RepoSHAKey formats the sha-index partition key.
func RepoSHAKey(repo, sha string) string {
	return strings.ToLower(repo) + ":" + sha
}

// BranchKey formats the branch_key-index partition key.
// Returns empty string if headBranch is empty (sparse GSI).
func BranchKey(repo, headRepo, headBranch string) string {
	if headBranch == "" {
		return ""
	}
	if headRepo == "" {
		headRepo = repo
	}
	return strings.ToLower(repo) + ":" + strings.ToLower(headRepo) + ":" + headBranch
}
