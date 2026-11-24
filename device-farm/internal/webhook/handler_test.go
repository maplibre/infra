package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/maplibre/infra/device-farm/internal/config"
	"github.com/maplibre/infra/device-farm/internal/model"
	"github.com/maplibre/infra/device-farm/internal/secrets"
)

type fakeCreds struct{ secret string }

func (f fakeCreds) Get(context.Context) (secrets.AppCredentials, error) {
	return secrets.AppCredentials{WebhookSecret: f.secret}, nil
}

type fakeSQS struct {
	messages []string
}

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.messages = append(f.messages, aws.ToString(in.MessageBody))
	return &sqs.SendMessageOutput{MessageId: aws.String("msg-1")}, nil
}

func sign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func testConfig(allowedBranches string) config.Config {
	cfg, err := config.LoadFromLookup(func(k string) (string, bool) {
		m := map[string]string{
			"MODE":                  "webhook",
			"GITHUB_APP_SECRET_ARN": "arn:secret",
			"JOBS_QUEUE_URL":        "https://sqs.us-west-2.amazonaws.com/1/jobs",
			"ALLOWED_BRANCHES":      allowedBranches,
		}
		v, ok := m[k]
		return v, ok
	})
	if err != nil {
		panic(err)
	}
	return cfg
}

func TestWebhookHandler(t *testing.T) {
	const secret = "webhook-secret-123"

	t.Run("rejects invalid signature with 401", func(t *testing.T) {
		q := &fakeSQS{}
		h := NewHandler(testConfig(""), fakeCreds{secret: secret}, q, nil)

		resp, err := h.Handle(context.Background(), events.LambdaFunctionURLRequest{
			Headers: map[string]string{
				"x-github-event":      "ping",
				"x-hub-signature-256": "sha256=deadbeef",
			},
			Body: `{}`,
		})
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", resp.StatusCode)
		}
		if len(q.messages) != 0 {
			t.Fatalf("expected 0 SQS messages, got %d", len(q.messages))
		}
	})

	t.Run("ping returns 200 without enqueueing", func(t *testing.T) {
		q := &fakeSQS{}
		h := NewHandler(testConfig(""), fakeCreds{secret: secret}, q, nil)
		body := `{"zen":"hello"}`

		resp, err := h.Handle(context.Background(), events.LambdaFunctionURLRequest{
			Headers: map[string]string{
				"X-GitHub-Event":      "ping",
				"X-Hub-Signature-256": sign(secret, body),
			},
			Body: body,
		})
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.StatusCode != http.StatusOK || len(q.messages) != 0 {
			t.Fatalf("status=%d messages=%d", resp.StatusCode, len(q.messages))
		}
	})

	t.Run("ignores workflow_job queued and in_progress", func(t *testing.T) {
		q := &fakeSQS{}
		h := NewHandler(testConfig(""), fakeCreds{secret: secret}, q, nil)

		for _, action := range []string{"queued", "in_progress"} {
			body := `{
				"action": "` + action + `",
				"repository": {"full_name": "maplibre/mln-rs"},
				"installation": {"id": 555},
				"workflow_job": {
					"id": 100,
					"run_id": 200,
					"run_attempt": 1,
					"workflow_name": "ci",
					"head_branch": "main",
					"head_sha": "abc123",
					"name": "prepare-device-tests"
				}
			}`
			resp, err := h.Handle(context.Background(), events.LambdaFunctionURLRequest{
				Headers: map[string]string{
					"x-github-event":      "workflow_job",
					"x-hub-signature-256": sign(secret, body),
				},
				Body: body,
			})
			if err != nil {
				t.Fatalf("%s Handle() error = %v", action, err)
			}
			if resp.StatusCode != http.StatusOK || len(q.messages) != 0 {
				t.Fatalf("%s expected 200 ignored, got status=%d messages=%d", action, resp.StatusCode, len(q.messages))
			}
		}
	})

	t.Run("enqueues completed workflow_job with run_attempt and respects ALLOWED_BRANCHES", func(t *testing.T) {
		q := &fakeSQS{}
		h := NewHandler(testConfig("ci/device-farm-lambda"), fakeCreds{secret: secret}, q, nil)

		bodyAllowed := `{
			"action": "completed",
			"repository": {"full_name": "maplibre/mln-rs"},
			"installation": {"id": 555},
			"workflow_job": {
				"id": 100,
				"run_id": 200,
				"run_attempt": 3,
				"workflow_name": "ci",
				"head_branch": "ci/device-farm-lambda",
				"head_sha": "abc123",
				"name": "prepare-device-tests",
				"conclusion": "success"
			}
		}`
		resp, err := h.Handle(context.Background(), events.LambdaFunctionURLRequest{
			Headers: map[string]string{
				"x-github-event":      "workflow_job",
				"x-github-delivery":   "del-1",
				"x-hub-signature-256": sign(secret, bodyAllowed),
			},
			Body: bodyAllowed,
		})
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", resp.StatusCode)
		}
		if len(q.messages) != 1 {
			t.Fatalf("expected 1 SQS message, got %d", len(q.messages))
		}
		var msg model.JobMessage
		if err := json.Unmarshal([]byte(q.messages[0]), &msg); err != nil {
			t.Fatalf("unmarshal SQS message: %v", err)
		}
		if msg.Event != model.EventWorkflowJob || msg.WorkflowRunID != 200 || msg.WorkflowRunAttempt != 3 || msg.JobConclusion != "success" {
			t.Fatalf("unexpected msg: %+v", msg)
		}

		// Non-allowed branch is ignored with 200 OK.
		bodyBlocked := `{
			"action": "completed",
			"repository": {"full_name": "maplibre/mln-rs"},
			"installation": {"id": 555},
			"workflow_job": {
				"id": 101,
				"run_id": 201,
				"run_attempt": 1,
				"workflow_name": "ci",
				"head_branch": "other-branch",
				"head_sha": "def456",
				"name": "prepare-device-tests",
				"conclusion": "success"
			}
		}`
		resp, err = h.Handle(context.Background(), events.LambdaFunctionURLRequest{
			Headers: map[string]string{
				"x-github-event":      "workflow_job",
				"x-hub-signature-256": sign(secret, bodyBlocked),
			},
			Body: bodyBlocked,
		})
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.StatusCode != http.StatusOK || len(q.messages) != 1 {
			t.Fatalf("expected blocked branch to be ignored: status=%d messages=%d", resp.StatusCode, len(q.messages))
		}
	})

	t.Run("enqueues check_run rerequested and workflow_run completed with run_attempt", func(t *testing.T) {
		q := &fakeSQS{}
		h := NewHandler(testConfig(""), fakeCreds{secret: secret}, q, nil)

		for _, tc := range []struct {
			event string
			body  string
		}{
			{
				event: "workflow_run",
				body: `{
					"action": "completed",
					"repository": {"full_name": "maplibre/mln-rs"},
					"installation": {"id": 555},
					"workflow_run": {
						"id": 300,
						"run_attempt": 2,
						"name": "ci",
						"head_branch": "main",
						"head_sha": "111",
						"conclusion": "success",
						"head_repository": {"full_name": "maplibre/mln-rs"}
					}
				}`,
			},
			{
				event: "check_run",
				body: `{
					"action": "rerequested",
					"repository": {"full_name": "maplibre/mln-rs"},
					"installation": {"id": 555},
					"check_run": {
						"id": 400,
						"name": "device-test / android",
						"head_sha": "111",
						"external_id": "300:2",
						"check_suite": {"head_branch": "main"}
					}
				}`,
			},
		} {
			resp, err := h.Handle(context.Background(), events.LambdaFunctionURLRequest{
				Headers: map[string]string{
					"x-github-event":      tc.event,
					"x-hub-signature-256": sign(secret, tc.body),
				},
				Body: tc.body,
			})
			if err != nil {
				t.Fatalf("%s Handle() error = %v", tc.event, err)
			}
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("%s status = %d, want 202", tc.event, resp.StatusCode)
			}
		}
		if len(q.messages) != 2 {
			t.Fatalf("expected 2 SQS messages, got %d", len(q.messages))
		}
		var wfMsg model.JobMessage
		if err := json.Unmarshal([]byte(q.messages[0]), &wfMsg); err != nil {
			t.Fatal(err)
		}
		if wfMsg.WorkflowRunAttempt != 2 {
			t.Fatalf("WorkflowRunAttempt = %d, want 2", wfMsg.WorkflowRunAttempt)
		}
	})
}
