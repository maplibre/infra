package worker

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/maplibre/infra/device-farm/internal/store"
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
	cr := github.CheckRun{
		ID:         f.nextCheckID,
		Name:       in.Name,
		ExternalID: in.ExternalID,
		Status:     in.Status,
		Conclusion: in.Conclusion,
	}
	f.listedChecks = append(f.listedChecks, cr)
	return cr, nil
}

func (f *fakeGH) UpdateCheckRun(_ context.Context, _ int64, _, _ string, id int64, in github.UpdateCheckRunRequest) (github.CheckRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updated[id] = append(f.updated[id], in)
	for i := range f.listedChecks {
		if f.listedChecks[i].ID == id {
			if in.Status != "" {
				f.listedChecks[i].Status = in.Status
			}
			if in.Conclusion != "" {
				f.listedChecks[i].Conclusion = in.Conclusion
			}
		}
	}
	return github.CheckRun{ID: id, Status: in.Status, Conclusion: in.Conclusion}, nil
}

func (f *fakeGH) ListCheckRunsForRef(_ context.Context, _ int64, _, _, _, _ string) ([]github.CheckRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]github.CheckRun, len(f.listedChecks))
	copy(out, f.listedChecks)
	return out, nil
}

func (f *fakeGH) GetWorkflowRun(_ context.Context, _ int64, _, _ string, _ int64) (github.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.workflowRun, nil
}

func (f *fakeGH) ListWorkflowRunJobs(_ context.Context, _ int64, _, _ string, _ int64) ([]github.WorkflowJob, error) {
	return f.jobs, nil
}

func (f *fakeGH) ListWorkflowRunArtifacts(_ context.Context, _ int64, _, _ string, _ int64, _ string) ([]github.Artifact, error) {
	return f.artifacts, nil
}

func (f *fakeGH) DownloadArtifact(_ context.Context, _ int64, _, _ string, _ int64, dstPath string, _ int64) error {
	f.mu.Lock()
	err := f.downloadErr
	f.mu.Unlock()
	if err != nil {
		return err
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

func (s *fakeStore) ClaimWorkflowRun(_ context.Context, claimKey string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.records[claimKey]; exists {
		return store.ErrAlreadyClaimed
	}
	s.records[claimKey] = model.RunRecord{
		RunARN:    claimKey,
		State:     model.StateClaimed,
		CreatedAt: now.UTC().Format(time.RFC3339Nano),
	}
	return nil
}

func (s *fakeStore) PutRun(_ context.Context, rec model.RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.RunARN] = rec
	return nil
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
	uploadErr   error
	stoppedARNs []string
	scheduledIn []devicefarm.ScheduleInput
	runCounter  int
	nextRunARN  string
}

func (d *fakeDF) UploadFile(_ context.Context, _, name string, uploadType dftypes.UploadType, _ string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.uploadErr != nil {
		return "", d.uploadErr
	}
	d.uploads = append(d.uploads, uploadType)
	return "arn:aws:devicefarm:us-west-2:373521797162:upload:proj/" + name, nil
}

func (d *fakeDF) ScheduleRun(_ context.Context, in devicefarm.ScheduleInput) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.scheduledIn = append(d.scheduledIn, in)
	d.runCounter++
	if d.nextRunARN != "" {
		return d.nextRunARN, nil
	}
	return fmt.Sprintf("arn:aws:devicefarm:us-west-2:373521797162:run:proj/run-%d", d.runCounter), nil
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
	t1 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(5 * time.Minute)

	gh := newFakeGH()
	gh.workflowRun = github.WorkflowRun{
		ID:         500,
		RunAttempt: 1,
		HeadBranch: "feature-branch",
		HeadSHA:    "2222222222222222222222222222222222222222",
		CreatedAt:  t2,
	}
	gh.workflowRun.HeadRepository.FullName = "forkuser/mln-rs"
	gh.artifacts = []github.Artifact{
		{ID: 77, Name: "android-device-tests", Expired: false, CreatedAt: t2},
	}

	st := newFakeStore()
	oldARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj/old-run"
	st.records[oldARN] = model.RunRecord{
		RunARN:            oldARN,
		State:             model.StateActive,
		BranchKey:         model.BranchKey("maplibre/mln-rs", "forkuser/mln-rs", "feature-branch"),
		RepoSHA:           model.RepoSHAKey("maplibre/mln-rs", "1111111111111111111111111111111111111111"),
		Repo:              "maplibre/mln-rs",
		SHA:               "1111111111111111111111111111111111111111",
		WorkflowRunID:     499,
		WorkflowCreatedAt: t1.Format(time.RFC3339),
		CheckRunID:        42,
		InstallationID:    99,
	}

	df := &fakeDF{nextRunARN: "arn:aws:devicefarm:us-west-2:373521797162:run:proj/new-run"}
	h := NewHandler(testWorkerCfg(), gh, st, df, nil)

	resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
		Event:              model.EventWorkflowJob,
		Action:             "completed",
		Repo:               "maplibre/mln-rs",
		InstallationID:     99,
		HeadSHA:            "2222222222222222222222222222222222222222",
		HeadBranch:         "feature-branch",
		WorkflowRunID:      500,
		WorkflowRunAttempt: 1,
		JobName:            "prepare-device-tests",
		JobConclusion:      "success",
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

	// Verify new run saved ACTIVE with WorkflowCreatedAt and external_id "<runID>:<attempt>".
	newRec, ok := st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/new-run"]
	if !ok || newRec.State != model.StateActive {
		t.Fatalf("expected new ACTIVE record, got %+v", newRec)
	}
	if newRec.WorkflowCreatedAt != t2.Format(time.RFC3339) || newRec.WorkflowRunAttempt != 1 {
		t.Fatalf("unexpected metadata on newRec: %+v", newRec)
	}
	if len(gh.created) != 1 || gh.created[0].ExternalID != "500:1" {
		t.Fatalf("expected external_id 500:1, got %+v", gh.created)
	}
	updates := gh.updated[newRec.CheckRunID]
	if len(updates) == 0 || !strings.Contains(updates[len(updates)-1].DetailsURL, "/projects/proj/runs/new-run") {
		t.Fatalf("expected console URL in check run update, got %+v", updates)
	}
}

func TestWorkerAtomicClaimDeduplicatesAndReleasesOnTransientError(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("concurrent duplicate deliveries schedule only once", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 800, RunAttempt: 1, HeadBranch: "main", HeadSHA: "aaa111", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 1, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		msg := model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "aaa111",
			HeadBranch:         "main",
			WorkflowRunID:      800,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}

		var wg sync.WaitGroup
		wg.Add(2)
		for range 2 {
			go func() {
				defer wg.Done()
				resp, err := h.Handle(context.Background(), sqsEventOf(t, msg))
				if err != nil || len(resp.BatchItemFailures) != 0 {
					t.Errorf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
				}
			}()
		}
		wg.Wait()

		if len(df.scheduledIn) != 1 {
			t.Fatalf("scheduled %d runs across concurrent deliveries, want 1", len(df.scheduledIn))
		}
	})

	t.Run("transient error releases claim so retry succeeds", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 801, RunAttempt: 1, HeadBranch: "main", HeadSHA: "bbb222", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 2, Name: "android-device-tests", CreatedAt: now}}
		gh.downloadErr = errors.New("temporary 503")

		st := newFakeStore()
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		msg := model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "bbb222",
			HeadBranch:         "main",
			WorkflowRunID:      801,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}

		// First attempt hits transient error -> reported in BatchItemFailures and claim released.
		resp, err := h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 1 {
			t.Fatalf("expected 1 batch failure on transient error, got err=%v failures=%v", err, resp.BatchItemFailures)
		}
		claimKey := model.ClaimKey("maplibre/mln-rs", 801, 1)
		if _, stillClaimed := st.records[claimKey]; stillClaimed {
			t.Fatal("claim was not released after transient error")
		}

		// Second attempt succeeds and reuses the open Check Run.
		gh.downloadErr = nil
		resp, err = h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("retry failed: err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 1 {
			t.Fatalf("scheduledIn = %d, want 1", len(df.scheduledIn))
		}
		if len(gh.created) != 1 {
			t.Fatalf("expected retry to reuse existing Check Run (1 created), got %d", len(gh.created))
		}
	})
}

func TestWorkerWorkflowRerunAttemptSchedulesFreshRun(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	gh := newFakeGH()
	gh.workflowRun = github.WorkflowRun{ID: 900, RunAttempt: 1, HeadBranch: "main", HeadSHA: "ccc333", CreatedAt: now}
	gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
	gh.artifacts = []github.Artifact{{ID: 10, Name: "android-device-tests", CreatedAt: now}}

	st := newFakeStore()
	df := &fakeDF{}
	h := NewHandler(testWorkerCfg(), gh, st, df, nil)

	// Attempt 1 schedules run-1.
	resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
		Event:              model.EventWorkflowJob,
		Action:             "completed",
		Repo:               "maplibre/mln-rs",
		InstallationID:     99,
		HeadSHA:            "ccc333",
		HeadBranch:         "main",
		WorkflowRunID:      900,
		WorkflowRunAttempt: 1,
		JobConclusion:      "success",
	}))
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("attempt 1 failed: err=%v failures=%v", err, resp.BatchItemFailures)
	}

	// Simulate attempt 1 check run completed, then user re-runs the GitHub Actions workflow (attempt 2).
	gh.listedChecks[0].Status = "completed"
	gh.workflowRun.RunAttempt = 2
	resp, err = h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
		Event:              model.EventWorkflowJob,
		Action:             "completed",
		Repo:               "maplibre/mln-rs",
		InstallationID:     99,
		HeadSHA:            "ccc333",
		HeadBranch:         "main",
		WorkflowRunID:      900,
		WorkflowRunAttempt: 2,
		JobConclusion:      "success",
	}))
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("attempt 2 failed: err=%v failures=%v", err, resp.BatchItemFailures)
	}
	if len(df.scheduledIn) != 2 {
		t.Fatalf("scheduledIn = %d, want 2 (attempt 1 and attempt 2)", len(df.scheduledIn))
	}
	if len(gh.created) != 2 || gh.created[0].ExternalID != "900:1" || gh.created[1].ExternalID != "900:2" {
		t.Fatalf("unexpected created check runs: %+v", gh.created)
	}
}

func TestWorkerStaleCommitDoesNotSupersedeNewerActiveRun(t *testing.T) {
	olderTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	newerTime := olderTime.Add(3 * time.Minute)

	gh := newFakeGH()
	gh.workflowRun = github.WorkflowRun{
		ID:         400,
		RunAttempt: 1,
		HeadBranch: "feature",
		HeadSHA:    "1111111older",
		CreatedAt:  olderTime,
	}
	gh.workflowRun.HeadRepository.FullName = "fork/mln-rs"
	gh.artifacts = []github.Artifact{{ID: 5, Name: "android-device-tests", CreatedAt: olderTime}}

	st := newFakeStore()
	newerARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj/newer-run"
	st.records[newerARN] = model.RunRecord{
		RunARN:            newerARN,
		State:             model.StateActive,
		BranchKey:         model.BranchKey("maplibre/mln-rs", "fork/mln-rs", "feature"),
		RepoSHA:           model.RepoSHAKey("maplibre/mln-rs", "9999999newer"),
		Repo:              "maplibre/mln-rs",
		SHA:               "9999999newer",
		WorkflowRunID:     401,
		WorkflowCreatedAt: newerTime.Format(time.RFC3339),
		CheckRunID:        55,
		InstallationID:    99,
	}

	df := &fakeDF{}
	h := NewHandler(testWorkerCfg(), gh, st, df, nil)

	resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
		Event:              model.EventWorkflowJob,
		Action:             "completed",
		Repo:               "maplibre/mln-rs",
		InstallationID:     99,
		HeadSHA:            "1111111older",
		HeadBranch:         "feature",
		WorkflowRunID:      400,
		WorkflowRunAttempt: 1,
		JobConclusion:      "success",
	}))
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
	}

	// Newer run must remain ACTIVE and NOT be stopped.
	if st.records[newerARN].State != model.StateActive {
		t.Fatalf("newer run state = %s, want ACTIVE", st.records[newerARN].State)
	}
	if len(df.stoppedARNs) != 0 || len(df.scheduledIn) != 0 {
		t.Fatalf("expected 0 stops and 0 schedules, got stops=%v schedules=%d", df.stoppedARNs, len(df.scheduledIn))
	}

	// Stale commit's Check Run is completed as cancelled mentioning the newer commit.
	if len(gh.created) != 1 || gh.created[0].Conclusion != "cancelled" || !strings.Contains(gh.created[0].Output.Summary, "9999999") {
		t.Fatalf("expected cancelled stale check run, got %+v", gh.created)
	}
}

func TestWorkerUploadRejectedSettlesCheckWithoutRetry(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	gh := newFakeGH()
	gh.workflowRun = github.WorkflowRun{ID: 950, RunAttempt: 1, HeadBranch: "main", HeadSHA: "ddd444", CreatedAt: now}
	gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
	gh.artifacts = []github.Artifact{{ID: 12, Name: "android-device-tests", CreatedAt: now}}

	st := newFakeStore()
	df := &fakeDF{
		uploadErr: &devicefarm.UploadRejectedError{
			Name:    "android-tests.zip",
			Message: "corrupted zip archive",
		},
	}
	h := NewHandler(testWorkerCfg(), gh, st, df, nil)

	resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
		Event:              model.EventWorkflowJob,
		Action:             "completed",
		Repo:               "maplibre/mln-rs",
		InstallationID:     99,
		HeadSHA:            "ddd444",
		HeadBranch:         "main",
		WorkflowRunID:      950,
		WorkflowRunAttempt: 1,
		JobConclusion:      "success",
	}))
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("expected permanent upload rejection not to retry, got err=%v failures=%v", err, resp.BatchItemFailures)
	}

	// Claim stays in place so duplicate deliveries don't retry permanently rejected package.
	claimKey := model.ClaimKey("maplibre/mln-rs", 950, 1)
	if _, ok := st.records[claimKey]; !ok {
		t.Fatal("expected claim to remain persisted on permanent upload rejection")
	}

	updates := gh.updated[gh.nextCheckID]
	if len(updates) == 0 {
		t.Fatal("expected Check Run update completing with failure")
	}
	last := updates[len(updates)-1]
	if last.Status != "completed" || last.Conclusion != "failure" || last.Output.Title != "Device Farm rejected upload" || !strings.Contains(last.Output.Summary, "corrupted zip archive") {
		t.Fatalf("unexpected final Check Run update: %+v", last)
	}
}

func TestWorkerSkippedJobAndSafetyNetAndExpiredRerun(t *testing.T) {
	t.Run("skipped trigger job completes check with success", func(t *testing.T) {
		gh := newFakeGH()
		st := newFakeStore()
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "aaaa",
			HeadBranch:         "docs-only",
			WorkflowRunID:      600,
			WorkflowRunAttempt: 1,
			JobConclusion:      "skipped",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(gh.created) != 1 || gh.created[0].Conclusion != "success" || gh.created[0].Output.Title != "Not needed" || gh.created[0].ExternalID != "600:1" {
			t.Fatalf("unexpected created check runs: %+v", gh.created)
		}
	})

	t.Run("workflow_run completed safety net settles missing attempt even when earlier attempt completed", func(t *testing.T) {
		gh := newFakeGH()
		gh.listedChecks = []github.CheckRun{
			{ID: 50, Name: "device-test / android", ExternalID: "601:1", Status: "completed", Conclusion: "failure"},
		}
		gh.jobs = []github.WorkflowJob{{Name: "plan", Conclusion: "success"}}
		st := newFakeStore()
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:              model.EventWorkflowRun,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "bbbb",
			HeadBranch:         "docs-only",
			WorkflowRunID:      601,
			WorkflowRunAttempt: 2,
			RunConclusion:      "success",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(gh.created) != 1 || gh.created[0].Conclusion != "success" || gh.created[0].ExternalID != "601:2" {
			t.Fatalf("expected safety net to create success check run for 601:2, got %+v", gh.created)
		}
	})

	t.Run("check_run rerequested with expired artifact reports helpful failure", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 700, RunAttempt: 1, HeadBranch: "main", HeadSHA: "cccc"}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 88, Name: "android-device-tests", Expired: true}}

		st := newFakeStore()
		st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/old"] = model.RunRecord{
			RunARN:             "arn:aws:devicefarm:us-west-2:373521797162:run:proj/old",
			State:              model.StateCompleted,
			RepoSHA:            model.RepoSHAKey("maplibre/mln-rs", "cccc"),
			Repo:               "maplibre/mln-rs",
			SHA:                "cccc",
			HeadBranch:         "main",
			CheckRunID:         50,
			WorkflowRunID:      700,
			WorkflowRunAttempt: 1,
			InstallationID:     99,
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
