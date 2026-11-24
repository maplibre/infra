package worker

import (
	"archive/zip"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	dftypes "github.com/aws/aws-sdk-go-v2/service/devicefarm/types"

	"github.com/maplibre/infra/device-farm/internal/config"
	"github.com/maplibre/infra/device-farm/internal/devicefarm"
	"github.com/maplibre/infra/device-farm/internal/github"
	"github.com/maplibre/infra/device-farm/internal/model"
)

type fakeGH struct {
	mu           sync.Mutex
	nextCheckID  int64
	created      []github.CreateCheckRunRequest
	updated      map[int64][]github.UpdateCheckRunRequest
	listedChecks []github.CheckRun
	workflowRun  github.WorkflowRun
	jobs         []github.WorkflowJob
	artifacts    []github.Artifact
	downloadErr  error
	rawFileRef   string
	rawFileBytes []byte
}

func newFakeGH() *fakeGH {
	return &fakeGH{
		nextCheckID:  100,
		updated:      make(map[int64][]github.UpdateCheckRunRequest),
		rawFileBytes: []byte("version: 0.1\n"),
	}
}

func (f *fakeGH) CreateCheckRun(_ context.Context, _ int64, _, _ string, in github.CreateCheckRunRequest) (github.CheckRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextCheckID++
	f.created = append(f.created, in)
	return github.CheckRun{ID: f.nextCheckID, Name: in.Name, Status: in.Status, Conclusion: in.Conclusion}, nil
}

func (f *fakeGH) UpdateCheckRun(_ context.Context, _ int64, _, _ string, id int64, in github.UpdateCheckRunRequest) (github.CheckRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updated[id] = append(f.updated[id], in)
	return github.CheckRun{ID: id, Status: in.Status, Conclusion: in.Conclusion}, nil
}

func (f *fakeGH) ListCheckRunsForRef(_ context.Context, _ int64, _, _, _, _ string) ([]github.CheckRun, error) {
	return f.listedChecks, nil
}

func (f *fakeGH) GetWorkflowRun(_ context.Context, _ int64, _, _ string, _ int64) (github.WorkflowRun, error) {
	return f.workflowRun, nil
}

func (f *fakeGH) ListWorkflowRunJobs(_ context.Context, _ int64, _, _ string, _ int64) ([]github.WorkflowJob, error) {
	return f.jobs, nil
}

func (f *fakeGH) ListWorkflowRunArtifacts(_ context.Context, _ int64, _, _ string, _ int64, _ string) ([]github.Artifact, error) {
	return f.artifacts, nil
}

func (f *fakeGH) DownloadArtifact(_ context.Context, _ int64, _, _ string, _ int64, dstPath string, _ int64) error {
	if f.downloadErr != nil {
		return f.downloadErr
	}
	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(out)
	w, err := zw.Create("android-tests.zip")
	if err != nil {
		return err
	}
	_, _ = w.Write([]byte("opaque-inner-zip"))
	_ = zw.Close()
	return out.Close()
}

func (f *fakeGH) GetRawFile(_ context.Context, _ int64, _, _, _, ref string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rawFileRef = ref
	return f.rawFileBytes, nil
}

type fakeStore struct {
	mu      sync.Mutex
	records map[string]model.RunRecord
}

func newFakeStore() *fakeStore {
	return &fakeStore{records: make(map[string]model.RunRecord)}
}

func (s *fakeStore) PutRun(_ context.Context, rec model.RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.RunARN] = rec
	return nil
}

func (s *fakeStore) GetRun(_ context.Context, arn string) (model.RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[arn]
	return rec, ok, nil
}

func (s *fakeStore) DeleteRun(_ context.Context, arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, arn)
	return nil
}

func (s *fakeStore) QueryActiveByBranchKey(_ context.Context, branchKey string) ([]model.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.RunRecord
	for _, r := range s.records {
		if r.BranchKey == branchKey && r.State == model.StateActive {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *fakeStore) QueryByRepoSHA(_ context.Context, repo, sha string) ([]model.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := model.RepoSHAKey(repo, sha)
	var out []model.RunRecord
	for _, r := range s.records {
		if r.RepoSHA == key {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *fakeStore) MarkSuperseded(_ context.Context, arn string, completedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[arn]
	r.State = model.StateSuperseded
	r.CompletedAt = completedAt.UTC().Format(time.RFC3339Nano)
	s.records[arn] = r
	return nil
}

type fakeDF struct {
	mu          sync.Mutex
	uploads     []dftypes.UploadType
	stoppedARNs []string
	scheduledIn []devicefarm.ScheduleInput
	nextRunARN  string
}

func (d *fakeDF) UploadFile(_ context.Context, _, name string, uploadType dftypes.UploadType, _ string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.uploads = append(d.uploads, uploadType)
	return "arn:aws:devicefarm:us-west-2:373521797162:upload:proj/" + name, nil
}

func (d *fakeDF) ScheduleRun(_ context.Context, in devicefarm.ScheduleInput) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.scheduledIn = append(d.scheduledIn, in)
	return d.nextRunARN, nil
}

func (d *fakeDF) StopRun(_ context.Context, arn string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stoppedARNs = append(d.stoppedARNs, arn)
	return nil
}

func testWorkerCfg() config.Config {
	cfg, err := config.LoadFromLookup(func(k string) (string, bool) {
		m := map[string]string{
			"MODE":                  "worker",
			"GITHUB_APP_SECRET_ARN": "arn:secret",
			"RUNS_TABLE_NAME":       "runs",
		}
		v, ok := m[k]
		return v, ok
	})
	if err != nil {
		panic(err)
	}
	return cfg
}

func sqsEventOf(t *testing.T, msg model.JobMessage) events.SQSEvent {
	t.Helper()
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return events.SQSEvent{
		Records: []events.SQSMessage{{MessageId: "m1", Body: string(b)}},
	}
}

func TestWorkerSchedulesAndSupersedesOlderRun(t *testing.T) {
	gh := newFakeGH()
	gh.workflowRun = github.WorkflowRun{
		ID:         500,
		HeadBranch: "feature-branch",
		HeadSHA:    "2222222222222222222222222222222222222222",
	}
	gh.workflowRun.HeadRepository.FullName = "forkuser/mln-rs"
	gh.artifacts = []github.Artifact{
		{ID: 77, Name: "android-device-tests", Expired: false, CreatedAt: time.Now()},
	}

	st := newFakeStore()
	oldARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj/old-run"
	st.records[oldARN] = model.RunRecord{
		RunARN:         oldARN,
		State:          model.StateActive,
		BranchKey:      model.BranchKey("maplibre/mln-rs", "forkuser/mln-rs", "feature-branch"),
		RepoSHA:        model.RepoSHAKey("maplibre/mln-rs", "1111111111111111111111111111111111111111"),
		Repo:           "maplibre/mln-rs",
		CheckRunID:     42,
		InstallationID: 99,
	}

	df := &fakeDF{nextRunARN: "arn:aws:devicefarm:us-west-2:373521797162:run:proj/new-run"}
	h := NewHandler(testWorkerCfg(), gh, st, df, nil)

	resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
		Event:          model.EventWorkflowJob,
		Action:         "completed",
		Repo:           "maplibre/mln-rs",
		InstallationID: 99,
		HeadSHA:        "2222222222222222222222222222222222222222",
		HeadBranch:     "feature-branch",
		WorkflowRunID:  500,
		JobName:        "prepare-device-tests",
		JobConclusion:  "success",
	}))
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
	}

	// Verify old run was superseded and stopped.
	if len(df.stoppedARNs) != 1 || df.stoppedARNs[0] != oldARN {
		t.Fatalf("stoppedARNs = %v, want [%s]", df.stoppedARNs, oldARN)
	}
	if st.records[oldARN].State != model.StateSuperseded {
		t.Fatalf("old state = %s, want SUPERSEDED", st.records[oldARN].State)
	}
	if updates := gh.updated[42]; len(updates) != 1 || updates[0].Conclusion != "cancelled" {
		t.Fatalf("expected old check run 42 cancelled, got %+v", updates)
	}

	// Verify test spec was fetched from main (NEVER PR branch).
	if gh.rawFileRef != "main" {
		t.Fatalf("rawFileRef = %q, want main", gh.rawFileRef)
	}

	// Verify new run saved ACTIVE and Check Run updated with console URL.
	newRec, ok := st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/new-run"]
	if !ok || newRec.State != model.StateActive {
		t.Fatalf("expected new ACTIVE record, got %+v", newRec)
	}
	updates := gh.updated[newRec.CheckRunID]
	if len(updates) == 0 || !strings.Contains(updates[len(updates)-1].DetailsURL, "/projects/proj/runs/new-run") {
		t.Fatalf("expected console URL in check run update, got %+v", updates)
	}
}

func TestWorkerSkippedJobAndSafetyNetAndExpiredRerun(t *testing.T) {
	t.Run("skipped trigger job completes check with success", func(t *testing.T) {
		gh := newFakeGH()
		st := newFakeStore()
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:          model.EventWorkflowJob,
			Action:         "completed",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "aaaa",
			HeadBranch:     "docs-only",
			WorkflowRunID:  600,
			JobConclusion:  "skipped",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(gh.created) != 1 || gh.created[0].Conclusion != "success" || gh.created[0].Output.Title != "Not needed" {
			t.Fatalf("unexpected created check runs: %+v", gh.created)
		}
	})

	t.Run("workflow_run completed safety net settles missing check", func(t *testing.T) {
		gh := newFakeGH()
		gh.jobs = []github.WorkflowJob{{Name: "plan", Conclusion: "success"}}
		st := newFakeStore()
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:          model.EventWorkflowRun,
			Action:         "completed",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "bbbb",
			HeadBranch:     "docs-only",
			WorkflowRunID:  601,
			RunConclusion:  "success",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(gh.created) != 1 || gh.created[0].Conclusion != "success" {
			t.Fatalf("expected safety net to create success check run, got %+v", gh.created)
		}
	})

	t.Run("check_run rerequested with expired artifact reports helpful failure", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 700, HeadBranch: "main", HeadSHA: "cccc"}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 88, Name: "android-device-tests", Expired: true}}

		st := newFakeStore()
		st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/old"] = model.RunRecord{
			RunARN:         "arn:aws:devicefarm:us-west-2:373521797162:run:proj/old",
			State:          model.StateCompleted,
			RepoSHA:        model.RepoSHAKey("maplibre/mln-rs", "cccc"),
			Repo:           "maplibre/mln-rs",
			SHA:            "cccc",
			HeadBranch:     "main",
			CheckRunID:     50,
			WorkflowRunID:  700,
			InstallationID: 99,
		}
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:          model.EventCheckRun,
			Action:         "rerequested",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "cccc",
			CheckRunID:     50,
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		// New check run created in_progress then completed with failure about expired artifact.
		if len(gh.created) != 1 {
			t.Fatalf("expected 1 new check run, got %d", len(gh.created))
		}
		newID := gh.nextCheckID
		updates := gh.updated[newID]
		if len(updates) == 0 || updates[len(updates)-1].Conclusion != "failure" || !strings.Contains(updates[len(updates)-1].Output.Summary, "expired") {
			t.Fatalf("expected expired artifact failure update, got %+v", updates)
		}
	})
}
