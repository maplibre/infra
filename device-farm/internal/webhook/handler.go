package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/maplibre/infra/device-farm/internal/config"
	"github.com/maplibre/infra/device-farm/internal/github"
	"github.com/maplibre/infra/device-farm/internal/model"
	"github.com/maplibre/infra/device-farm/internal/secrets"
)

// SQSAPI is the subset of the AWS SQS client used by Handler.
type SQSAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// CredentialsProvider supplies the GitHub webhook secret.
type CredentialsProvider interface {
	Get(ctx context.Context) (secrets.AppCredentials, error)
}

// Handler handles GitHub webhooks received via a Lambda Function URL.
type Handler struct {
	cfg    config.Config
	creds  CredentialsProvider
	sqs    SQSAPI
	logger *slog.Logger
}

// NewHandler constructs a webhook Lambda handler.
func NewHandler(cfg config.Config, creds CredentialsProvider, sqsClient SQSAPI, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		cfg:    cfg,
		creds:  creds,
		sqs:    sqsClient,
		logger: logger,
	}
}

type webhookEnvelope struct {
	Action     string `json:"action"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
	WorkflowJob *struct {
		ID           int64  `json:"id"`
		RunID        int64  `json:"run_id"`
		RunAttempt   int    `json:"run_attempt"`
		WorkflowName string `json:"workflow_name"`
		HeadBranch   string `json:"head_branch"`
		HeadSHA      string `json:"head_sha"`
		Name         string `json:"name"`
		Conclusion   string `json:"conclusion"`
	} `json:"workflow_job"`
	WorkflowRun *struct {
		ID             int64  `json:"id"`
		RunAttempt     int    `json:"run_attempt"`
		Name           string `json:"name"`
		HeadBranch     string `json:"head_branch"`
		HeadSHA        string `json:"head_sha"`
		Conclusion     string `json:"conclusion"`
		HeadRepository struct {
			FullName string `json:"full_name"`
		} `json:"head_repository"`
	} `json:"workflow_run"`
	CheckRun *struct {
		ID         int64  `json:"id"`
		Name       string `json:"name"`
		HeadSHA    string `json:"head_sha"`
		ExternalID string `json:"external_id"`
		CheckSuite struct {
			HeadBranch string `json:"head_branch"`
		} `json:"check_suite"`
	} `json:"check_run"`
}

// Handle processes a single Lambda Function URL HTTP request from GitHub.
func (h *Handler) Handle(ctx context.Context, req events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	if req.RequestContext.HTTP.Method != "" && !strings.EqualFold(req.RequestContext.HTTP.Method, http.MethodPost) {
		return jsonResponse(http.StatusMethodNotAllowed, `{"error":"method not allowed"}`), nil
	}

	body, err := decodeBody(req.Body, req.IsBase64Encoded)
	if err != nil {
		return jsonResponse(http.StatusBadRequest, `{"error":"invalid request body"}`), nil
	}

	creds, err := h.creds.Get(ctx)
	if err != nil {
		h.logger.Error("failed to load webhook secret", "err", err)
		return jsonResponse(http.StatusInternalServerError, `{"error":"internal error"}`), nil
	}

	sig := headerValue(req.Headers, "X-Hub-Signature-256")
	if !github.VerifySignature(creds.WebhookSecret, body, sig) {
		h.logger.Warn("rejected webhook with invalid signature")
		return jsonResponse(http.StatusUnauthorized, `{"error":"invalid signature"}`), nil
	}

	eventName := headerValue(req.Headers, "X-GitHub-Event")
	deliveryID := headerValue(req.Headers, "X-GitHub-Delivery")

	if eventName == "ping" {
		return jsonResponse(http.StatusOK, `{"ok":true}`), nil
	}

	var env webhookEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		h.logger.Warn("failed to parse webhook JSON", "event", eventName, "delivery", deliveryID, "err", err)
		return jsonResponse(http.StatusBadRequest, `{"error":"invalid json"}`), nil
	}

	repoCfg, ok := h.cfg.LookupRepo(env.Repository.FullName)
	if !ok {
		return jsonResponse(http.StatusOK, `{"ignored":"unconfigured repository"}`), nil
	}

	msg, accept, reason := h.buildMessage(eventName, deliveryID, repoCfg, env)
	if !accept {
		h.logger.Info("ignored webhook event", "event", eventName, "action", env.Action, "repo", repoCfg.FullName, "reason", reason)
		return jsonResponse(http.StatusOK, fmt.Sprintf(`{"ignored":%q}`, reason)), nil
	}

	if msg.InstallationID <= 0 {
		h.logger.Warn("webhook missing installation id", "event", eventName, "delivery", deliveryID)
		return jsonResponse(http.StatusBadRequest, `{"error":"missing installation id"}`), nil
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		h.logger.Error("failed to marshal job message", "err", err)
		return jsonResponse(http.StatusInternalServerError, `{"error":"internal error"}`), nil
	}

	_, err = h.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(h.cfg.JobsQueueURL),
		MessageBody: aws.String(string(payload)),
	})
	if err != nil {
		h.logger.Error("failed to enqueue SQS job", "event", eventName, "delivery", deliveryID, "err", err)
		return jsonResponse(http.StatusInternalServerError, `{"error":"enqueue failed"}`), nil
	}

	h.logger.Info("enqueued job", "event", msg.Event, "action", msg.Action, "repo", msg.Repo, "sha", msg.HeadSHA, "delivery", deliveryID)
	return jsonResponse(http.StatusAccepted, `{"accepted":true}`), nil
}

func (h *Handler) buildMessage(eventName, deliveryID string, repoCfg config.RepoConfig, env webhookEnvelope) (model.JobMessage, bool, string) {
	switch eventName {
	case string(model.EventWorkflowJob):
		if env.WorkflowJob == nil {
			return model.JobMessage{}, false, "missing workflow_job"
		}
		if env.Action != "completed" {
			return model.JobMessage{}, false, "unhandled action"
		}
		if env.WorkflowJob.Name != repoCfg.TriggerJob {
			return model.JobMessage{}, false, "job name mismatch"
		}
		if env.WorkflowJob.WorkflowName != "" && env.WorkflowJob.WorkflowName != repoCfg.WorkflowName {
			return model.JobMessage{}, false, "workflow name mismatch"
		}
		if !h.cfg.BranchAllowed(env.WorkflowJob.HeadBranch) {
			return model.JobMessage{}, false, "branch not allowed"
		}
		return model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             env.Action,
			DeliveryID:         deliveryID,
			Repo:               repoCfg.FullName,
			InstallationID:     env.Installation.ID,
			HeadSHA:            env.WorkflowJob.HeadSHA,
			HeadBranch:         env.WorkflowJob.HeadBranch,
			WorkflowRunID:      env.WorkflowJob.RunID,
			WorkflowRunAttempt: env.WorkflowJob.RunAttempt,
			WorkflowJobID:      env.WorkflowJob.ID,
			JobName:            env.WorkflowJob.Name,
			JobConclusion:      env.WorkflowJob.Conclusion,
		}, true, ""

	case string(model.EventWorkflowRun):
		if env.WorkflowRun == nil {
			return model.JobMessage{}, false, "missing workflow_run"
		}
		if env.Action != "completed" {
			return model.JobMessage{}, false, "unhandled action"
		}
		if env.WorkflowRun.Name != repoCfg.WorkflowName {
			return model.JobMessage{}, false, "workflow name mismatch"
		}
		if !h.cfg.BranchAllowed(env.WorkflowRun.HeadBranch) {
			return model.JobMessage{}, false, "branch not allowed"
		}
		return model.JobMessage{
			Event:              model.EventWorkflowRun,
			Action:             env.Action,
			DeliveryID:         deliveryID,
			Repo:               repoCfg.FullName,
			InstallationID:     env.Installation.ID,
			HeadSHA:            env.WorkflowRun.HeadSHA,
			HeadBranch:         env.WorkflowRun.HeadBranch,
			HeadRepo:           env.WorkflowRun.HeadRepository.FullName,
			WorkflowRunID:      env.WorkflowRun.ID,
			WorkflowRunAttempt: env.WorkflowRun.RunAttempt,
			RunConclusion:      env.WorkflowRun.Conclusion,
		}, true, ""

	case string(model.EventCheckRun):
		if env.CheckRun == nil {
			return model.JobMessage{}, false, "missing check_run"
		}
		if env.Action != "rerequested" {
			return model.JobMessage{}, false, "unhandled action"
		}
		if env.CheckRun.Name != repoCfg.CheckName {
			return model.JobMessage{}, false, "check_run name mismatch"
		}
		if env.CheckRun.CheckSuite.HeadBranch != "" && !h.cfg.BranchAllowed(env.CheckRun.CheckSuite.HeadBranch) {
			return model.JobMessage{}, false, "branch not allowed"
		}
		return model.JobMessage{
			Event:          model.EventCheckRun,
			Action:         env.Action,
			DeliveryID:     deliveryID,
			Repo:           repoCfg.FullName,
			InstallationID: env.Installation.ID,
			HeadSHA:        env.CheckRun.HeadSHA,
			HeadBranch:     env.CheckRun.CheckSuite.HeadBranch,
			CheckRunID:     env.CheckRun.ID,
			ExternalID:     env.CheckRun.ExternalID,
		}, true, ""

	default:
		return model.JobMessage{}, false, "unhandled event"
	}
}

func decodeBody(body string, isBase64 bool) ([]byte, error) {
	if !isBase64 {
		return []byte(body), nil
	}
	return base64.StdEncoding.DecodeString(body)
}

func headerValue(headers map[string]string, name string) string {
	for k, v := range headers {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

func jsonResponse(status int, body string) events.LambdaFunctionURLResponse {
	return events.LambdaFunctionURLResponse{
		StatusCode: status,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: body,
	}
}
