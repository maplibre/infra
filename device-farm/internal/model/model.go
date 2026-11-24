package model

import (
	"fmt"
	"strconv"
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
	Event              EventKind `json:"event"`
	Action             string    `json:"action"`
	DeliveryID         string    `json:"delivery_id,omitempty"`
	Repo               string    `json:"repo"`
	InstallationID     int64     `json:"installation_id"`
	HeadSHA            string    `json:"head_sha"`
	HeadBranch         string    `json:"head_branch,omitempty"`
	HeadRepo           string    `json:"head_repo,omitempty"`
	WorkflowRunID      int64     `json:"workflow_run_id,omitempty"`
	WorkflowRunAttempt int       `json:"workflow_run_attempt,omitempty"`
	WorkflowJobID      int64     `json:"workflow_job_id,omitempty"`
	JobName            string    `json:"job_name,omitempty"`
	JobConclusion      string    `json:"job_conclusion,omitempty"`
	RunConclusion      string    `json:"run_conclusion,omitempty"`
	CheckRunID         int64     `json:"check_run_id,omitempty"`
	ExternalID         string    `json:"external_id,omitempty"`
}

// RunState tracks the lifecycle of a DynamoDB run record.
type RunState string

const (
	// StateClaiming marks a short-lived ownership lease while a worker schedules a workflow attempt.
	StateClaiming RunState = "CLAIMING"
	// StateClaimed marks a finalized idempotency claim for a (repo, workflow_run_id, run_attempt).
	StateClaimed RunState = "CLAIMED"
	// StateActive marks a Device Farm run that the poller is watching.
	StateActive RunState = "ACTIVE"
	// StateCompleted marks a finished Device Farm run whose Check Run was settled.
	StateCompleted RunState = "COMPLETED"
	// StateFailed marks a run that failed unexpectedly (e.g. deleted from Device Farm).
	StateFailed RunState = "FAILED"
)

// RunRecord is stored in DynamoDB table maplibre-device-farm-runs.
type RunRecord struct {
	RunARN             string   `dynamodbav:"run_arn"`
	State              RunState `dynamodbav:"state"`
	ClaimOwner         string   `dynamodbav:"claim_owner,omitempty"`
	LeaseExpiresAt     int64    `dynamodbav:"lease_expires_at,omitempty"`
	CreatedAt          string   `dynamodbav:"created_at"`
	BranchKey          string   `dynamodbav:"branch_key,omitempty"`
	RepoSHA            string   `dynamodbav:"repo_sha,omitempty"`
	Repo               string   `dynamodbav:"repo,omitempty"`
	SHA                string   `dynamodbav:"sha,omitempty"`
	HeadBranch         string   `dynamodbav:"head_branch,omitempty"`
	HeadRepo           string   `dynamodbav:"head_repo,omitempty"`
	CheckRunID         int64    `dynamodbav:"check_run_id,omitempty"`
	WorkflowRunID      int64    `dynamodbav:"workflow_run_id,omitempty"`
	WorkflowRunAttempt int      `dynamodbav:"workflow_run_attempt,omitempty"`
	WorkflowCreatedAt  string   `dynamodbav:"workflow_created_at,omitempty"`
	RerunDeliveryID    string   `dynamodbav:"rerun_delivery_id,omitempty"`
	InstallationID     int64    `dynamodbav:"installation_id,omitempty"`
	CompletedAt        string   `dynamodbav:"completed_at,omitempty"`
	Result             string   `dynamodbav:"result,omitempty"`
	ExpiresAt          int64    `dynamodbav:"expires_at"`
}

// ClaimKey returns the DynamoDB primary key used to atomically claim scheduling
// of a specific (repo, workflowRunID, runAttempt).
func ClaimKey(repo string, workflowRunID int64, runAttempt int) string {
	if runAttempt <= 0 {
		runAttempt = 1
	}
	return fmt.Sprintf("claim#%s#%d#%d", strings.ToLower(repo), workflowRunID, runAttempt)
}

// RerunClaimKey returns the DynamoDB primary key used to deduplicate deliveries
// of a check_run.rerequested webhook by (repo, deliveryID).
func RerunClaimKey(repo, deliveryID string) string {
	return fmt.Sprintf("claim-rerun#%s#%s", strings.ToLower(repo), deliveryID)
}

// ExternalID formats the GitHub Check Run external_id as "<workflowRunID>:<runAttempt>".
func ExternalID(workflowRunID int64, runAttempt int) string {
	if workflowRunID <= 0 {
		return ""
	}
	if runAttempt <= 0 {
		runAttempt = 1
	}
	return fmt.Sprintf("%d:%d", workflowRunID, runAttempt)
}

// RerunExternalID formats the GitHub Check Run external_id for a check_run.rerequested
// delivery as "<workflowRunID>:<runAttempt>:<deliveryID>", falling back to ExternalID
// when deliveryID is empty.
func RerunExternalID(workflowRunID int64, runAttempt int, deliveryID string) string {
	if workflowRunID <= 0 {
		return ""
	}
	if deliveryID == "" {
		return ExternalID(workflowRunID, runAttempt)
	}
	if runAttempt <= 0 {
		runAttempt = 1
	}
	return fmt.Sprintf("%d:%d:%s", workflowRunID, runAttempt, deliveryID)
}

// ParseExternalID parses "<workflowRunID>", "<workflowRunID>:<runAttempt>", or
// "<workflowRunID>:<runAttempt>:<deliveryID>".
func ParseExternalID(raw string) (runID int64, attempt int, ok bool) {
	if raw == "" {
		return 0, 0, false
	}
	parts := strings.SplitN(raw, ":", 3)
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || id <= 0 {
		return 0, 0, false
	}
	if len(parts) == 1 {
		return id, 1, true
	}
	att, err := strconv.Atoi(parts[1])
	if err != nil || att <= 0 {
		return 0, 0, false
	}
	if len(parts) == 3 && parts[2] == "" {
		return 0, 0, false
	}
	return id, att, true
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
