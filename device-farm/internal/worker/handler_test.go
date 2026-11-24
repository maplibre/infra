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
	mu                 sync.Mutex
	nextCheckID        int64
	created            []github.CreateCheckRunRequest
	updated            map[int64][]github.UpdateCheckRunRequest
	updateErrByCheckID map[int64]error
	listedChecks       []github.CheckRun
	workflowRun        github.WorkflowRun
	jobs               []github.WorkflowJob
	artifacts          []github.Artifact
	downloadErr        error
	onDownload         func()
	rawFileRef         string
	rawFileBytes       []byte
	lastWorkflowRunID  int64
	lastJobsRunID      int64
	lastJobsAttempt    int
}

func newFakeGH() *fakeGH {
	return &fakeGH{
		nextCheckID:        100,
		updated:            make(map[int64][]github.UpdateCheckRunRequest),
		updateErrByCheckID: make(map[int64]error),
		rawFileBytes:       []byte("version: 0.1\n"),
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
	if err := f.updateErrByCheckID[id]; err != nil {
		return github.CheckRun{}, err
	}
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

func (f *fakeGH) GetWorkflowRun(_ context.Context, _ int64, _, _ string, runID int64) (github.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastWorkflowRunID = runID
	return f.workflowRun, nil
}

func (f *fakeGH) ListWorkflowRunJobs(_ context.Context, _ int64, _, _ string, runID int64, attempt int) ([]github.WorkflowJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastJobsRunID = runID
	f.lastJobsAttempt = attempt
	return f.jobs, nil
}

func (f *fakeGH) ListWorkflowRunArtifacts(_ context.Context, _ int64, _, _ string, _ int64, _ string) ([]github.Artifact, error) {
	return f.artifacts, nil
}

func (f *fakeGH) DownloadArtifact(_ context.Context, _ int64, _, _ string, _ int64, dstPath string, _ int64) error {
	f.mu.Lock()
	err := f.downloadErr
	hook := f.onDownload
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
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
	mu              sync.Mutex
	records         map[string]model.RunRecord
	getRunOverrides map[string]model.RunRecord
	putRunErr       error
	onPutRun        func()
	releaseCtxErrs  []error
	releaseCalls    int
	completeCalls   int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		records:         make(map[string]model.RunRecord),
		getRunOverrides: make(map[string]model.RunRecord),
	}
}

func (s *fakeStore) ClaimWorkflowRun(_ context.Context, claimKey, owner string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowUTC := now.UTC()
	if existing, exists := s.records[claimKey]; exists {
		if existing.State != model.StateClaiming || existing.LeaseExpiresAt >= nowUTC.Unix() {
			return store.ErrAlreadyClaimed
		}
	}
	s.records[claimKey] = model.RunRecord{
		RunARN:         claimKey,
		State:          model.StateClaiming,
		ClaimOwner:     owner,
		LeaseExpiresAt: nowUTC.Add(store.ClaimLeaseTTL).Unix(),
		CreatedAt:      nowUTC.Format(time.RFC3339Nano),
		ExpiresAt:      nowUTC.Add(store.RecordTTL).Unix(),
	}
	return nil
}

func (s *fakeStore) CompleteWorkflowClaim(_ context.Context, claimKey, owner string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeCalls++
	rec, exists := s.records[claimKey]
	if !exists || rec.State != model.StateClaiming || rec.ClaimOwner != owner {
		return store.ErrStateConflict
	}
	rec.State = model.StateClaimed
	rec.LeaseExpiresAt = 0
	rec.ExpiresAt = now.UTC().Add(store.RecordTTL).Unix()
	s.records[claimKey] = rec
	return nil
}

func (s *fakeStore) ReleaseWorkflowClaim(ctx context.Context, claimKey, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls++
	s.releaseCtxErrs = append(s.releaseCtxErrs, ctx.Err())
	rec, exists := s.records[claimKey]
	if !exists || rec.State != model.StateClaiming || rec.ClaimOwner != owner {
		return nil
	}
	delete(s.records, claimKey)
	return nil
}

func (s *fakeStore) PutRun(_ context.Context, rec model.RunRecord) error {
	s.mu.Lock()
	hook := s.onPutRun
	err := s.putRunErr
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.RunARN] = rec
	return nil
}

func (s *fakeStore) GetRun(_ context.Context, runARN string) (model.RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.getRunOverrides[runARN]; ok {
		return rec, true, nil
	}
	rec, ok := s.records[runARN]
	return rec, ok, nil
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
	stopCtxErrs []error
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

func (d *fakeDF) StopRun(ctx context.Context, arn string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stoppedARNs = append(d.stoppedARNs, arn)
	d.stopCtxErrs = append(d.stopCtxErrs, ctx.Err())
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

	// Claim is finalized to CLAIMED so duplicate deliveries don't retry permanently rejected package.
	claimKey := model.ClaimKey("maplibre/mln-rs", 950, 1)
	claimRec, ok := st.records[claimKey]
	if !ok || claimRec.State != model.StateClaimed || claimRec.LeaseExpiresAt != 0 {
		t.Fatalf("expected finalized CLAIMED record on permanent upload rejection, got %+v (exists=%v)", claimRec, ok)
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

func TestWorkerClaimLeaseTakeoverAndCrashDedup(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("expired CLAIMING lease allows retry takeover and finalizes to CLAIMED", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 810, RunAttempt: 1, HeadBranch: "main", HeadSHA: "eee555", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 21, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		claimKey := model.ClaimKey("maplibre/mln-rs", 810, 1)
		st.records[claimKey] = model.RunRecord{
			RunARN:         claimKey,
			State:          model.StateClaiming,
			ClaimOwner:     "crashed-worker",
			LeaseExpiresAt: now.Add(-10 * time.Second).Unix(),
		}

		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)
		h.now = func() time.Time { return now }

		msg := model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "eee555",
			HeadBranch:         "main",
			WorkflowRunID:      810,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}
		resp, err := h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 1 {
			t.Fatalf("scheduledIn = %d, want 1 after expired lease takeover", len(df.scheduledIn))
		}
		claimRec := st.records[claimKey]
		if claimRec.State != model.StateClaimed || claimRec.LeaseExpiresAt != 0 || claimRec.ClaimOwner == "crashed-worker" {
			t.Fatalf("expected finalized CLAIMED record with new owner, got %+v", claimRec)
		}

		// Subsequent delivery far in the future is permanently deduplicated by CLAIMED.
		h.now = func() time.Time { return now.Add(24 * time.Hour) }
		resp, err = h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("duplicate Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 1 {
			t.Fatalf("scheduledIn = %d, want 1 (CLAIMED must block duplicates)", len(df.scheduledIn))
		}
	})

	t.Run("active unexpired CLAIMING lease rejects concurrent delivery", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 811, RunAttempt: 1, HeadBranch: "main", HeadSHA: "fff666", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 22, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		claimKey := model.ClaimKey("maplibre/mln-rs", 811, 1)
		st.records[claimKey] = model.RunRecord{
			RunARN:         claimKey,
			State:          model.StateClaiming,
			ClaimOwner:     "active-worker",
			LeaseExpiresAt: now.Add(60 * time.Second).Unix(),
		}

		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)
		h.now = func() time.Time { return now }

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "fff666",
			HeadBranch:         "main",
			WorkflowRunID:      811,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 0 {
			t.Fatalf("expected 0 schedules while active CLAIMING lease is held, got %d", len(df.scheduledIn))
		}
	})

	t.Run("crash after PutRun deduplicates on expired lease takeover", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 812, RunAttempt: 1, HeadBranch: "main", HeadSHA: "123abc", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 23, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		claimKey := model.ClaimKey("maplibre/mln-rs", 812, 1)
		st.records[claimKey] = model.RunRecord{
			RunARN:         claimKey,
			State:          model.StateClaiming,
			ClaimOwner:     "crashed-after-putrun",
			LeaseExpiresAt: now.Add(-5 * time.Second).Unix(),
		}
		existingARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj/already-persisted"
		st.records[existingARN] = model.RunRecord{
			RunARN:             existingARN,
			State:              model.StateActive,
			RepoSHA:            model.RepoSHAKey("maplibre/mln-rs", "123abc"),
			Repo:               "maplibre/mln-rs",
			SHA:                "123abc",
			WorkflowRunID:      812,
			WorkflowRunAttempt: 1,
		}

		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)
		h.now = func() time.Time { return now }

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "123abc",
			HeadBranch:         "main",
			WorkflowRunID:      812,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 0 {
			t.Fatalf("expected 0 new runs when run already persisted before crash, got %d", len(df.scheduledIn))
		}
		if st.records[claimKey].State != model.StateClaimed {
			t.Fatalf("expected claim to be finalized to CLAIMED on dedup takeover, got %s", st.records[claimKey].State)
		}
	})
}

func TestWorkerCleanupUsesDetachedContext(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("releases claim with non-canceled context after download timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 820, RunAttempt: 1, HeadBranch: "main", HeadSHA: "aaa999", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 30, Name: "android-device-tests", CreatedAt: now}}
		gh.onDownload = cancel
		gh.downloadErr = context.Canceled

		st := newFakeStore()
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(ctx, sqsEventOf(t, model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "aaa999",
			HeadBranch:         "main",
			WorkflowRunID:      820,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}))
		if err != nil || len(resp.BatchItemFailures) != 1 {
			t.Fatalf("expected 1 batch failure, got err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if st.releaseCalls != 1 || st.releaseCtxErrs[0] != nil {
			t.Fatalf("expected ReleaseWorkflowClaim with non-canceled ctx, got calls=%d ctxErrs=%v", st.releaseCalls, st.releaseCtxErrs)
		}
		if _, stillClaimed := st.records[model.ClaimKey("maplibre/mln-rs", 820, 1)]; stillClaimed {
			t.Fatal("expected claim to be deleted after release")
		}
	})

	t.Run("stops orphaned Device Farm run and releases claim with detached context after PutRun failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 821, RunAttempt: 1, HeadBranch: "main", HeadSHA: "bbb999", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 31, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		st.onPutRun = cancel
		st.putRunErr = context.DeadlineExceeded
		df := &fakeDF{nextRunARN: "arn:aws:devicefarm:us-west-2:373521797162:run:proj/orphan-run"}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(ctx, sqsEventOf(t, model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "bbb999",
			HeadBranch:         "main",
			WorkflowRunID:      821,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}))
		if err != nil || len(resp.BatchItemFailures) != 1 {
			t.Fatalf("expected 1 batch failure, got err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.stoppedARNs) != 1 || df.stoppedARNs[0] != "arn:aws:devicefarm:us-west-2:373521797162:run:proj/orphan-run" {
			t.Fatalf("expected orphaned run to be stopped, got %v", df.stoppedARNs)
		}
		if len(df.stopCtxErrs) != 1 || df.stopCtxErrs[0] != nil {
			t.Fatalf("expected StopRun with non-canceled ctx, got %v", df.stopCtxErrs)
		}
		if st.releaseCalls != 1 || st.releaseCtxErrs[0] != nil {
			t.Fatalf("expected ReleaseWorkflowClaim with non-canceled ctx, got calls=%d ctxErrs=%v", st.releaseCalls, st.releaseCtxErrs)
		}
	})
}

func TestWorkerCheckRunRerequestedMultiWorkflowSHA(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("prefers msg.ExternalID over unrelated DynamoDB record on same SHA", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 701, RunAttempt: 2, HeadBranch: "main", HeadSHA: "shared-sha", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 40, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/other"] = model.RunRecord{
			RunARN:             "arn:aws:devicefarm:us-west-2:373521797162:run:proj/other",
			State:              model.StateCompleted,
			RepoSHA:            model.RepoSHAKey("maplibre/mln-rs", "shared-sha"),
			Repo:               "maplibre/mln-rs",
			SHA:                "shared-sha",
			CheckRunID:         999,
			WorkflowRunID:      9999,
			WorkflowRunAttempt: 1,
		}
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:          model.EventCheckRun,
			Action:         "rerequested",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "shared-sha",
			CheckRunID:     50,
			ExternalID:     "701:2",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if gh.lastWorkflowRunID != 701 {
			t.Fatalf("GetWorkflowRun called with %d, want 701 from ExternalID", gh.lastWorkflowRunID)
		}
		if len(df.scheduledIn) != 1 || len(gh.created) != 1 || gh.created[0].ExternalID != "701:2" {
			t.Fatalf("expected 1 rerun scheduled for 701:2, got scheduled=%d created=%+v", len(df.scheduledIn), gh.created)
		}
	})

	t.Run("does not fall back to unrelated CheckRunID on same SHA when ExternalID is empty", func(t *testing.T) {
		gh := newFakeGH()
		st := newFakeStore()
		st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/other"] = model.RunRecord{
			RunARN:             "arn:aws:devicefarm:us-west-2:373521797162:run:proj/other",
			State:              model.StateCompleted,
			RepoSHA:            model.RepoSHAKey("maplibre/mln-rs", "shared-sha"),
			Repo:               "maplibre/mln-rs",
			SHA:                "shared-sha",
			CheckRunID:         999,
			WorkflowRunID:      9999,
			WorkflowRunAttempt: 1,
		}
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:          model.EventCheckRun,
			Action:         "rerequested",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "shared-sha",
			CheckRunID:     50,
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 0 {
			t.Fatalf("expected 0 runs scheduled when CheckRunID does not match, got %d", len(df.scheduledIn))
		}
		if len(gh.created) != 1 || gh.created[0].Conclusion != "failure" || gh.created[0].Output.Title != "Unable to re-run" {
			t.Fatalf("expected Unable to re-run failure check run, got %+v", gh.created)
		}
	})

	t.Run("falls back to exact CheckRunID match when ExternalID is empty", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 702, RunAttempt: 1, HeadBranch: "main", HeadSHA: "shared-sha", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 41, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/other"] = model.RunRecord{
			RunARN:             "arn:aws:devicefarm:us-west-2:373521797162:run:proj/other",
			State:              model.StateCompleted,
			RepoSHA:            model.RepoSHAKey("maplibre/mln-rs", "shared-sha"),
			Repo:               "maplibre/mln-rs",
			SHA:                "shared-sha",
			CheckRunID:         999,
			WorkflowRunID:      9999,
			WorkflowRunAttempt: 1,
		}
		st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/exact"] = model.RunRecord{
			RunARN:             "arn:aws:devicefarm:us-west-2:373521797162:run:proj/exact",
			State:              model.StateCompleted,
			RepoSHA:            model.RepoSHAKey("maplibre/mln-rs", "shared-sha"),
			Repo:               "maplibre/mln-rs",
			SHA:                "shared-sha",
			CheckRunID:         50,
			WorkflowRunID:      702,
			WorkflowRunAttempt: 1,
		}
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:          model.EventCheckRun,
			Action:         "rerequested",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "shared-sha",
			CheckRunID:     50,
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if gh.lastWorkflowRunID != 702 {
			t.Fatalf("GetWorkflowRun called with %d, want 702 from exact CheckRunID match", gh.lastWorkflowRunID)
		}
		if len(df.scheduledIn) != 1 {
			t.Fatalf("expected 1 rerun scheduled, got %d", len(df.scheduledIn))
		}
	})
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
		if gh.lastJobsRunID != 601 || gh.lastJobsAttempt != 2 {
			t.Fatalf("ListWorkflowRunJobs called with runID=%d attempt=%d, want 601/2", gh.lastJobsRunID, gh.lastJobsAttempt)
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

func TestWorkerRerunDeliveryDeduplication(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("duplicate DeliveryID skipped while distinct DeliveryID schedules fresh rerun", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 750, RunAttempt: 1, HeadBranch: "main", HeadSHA: "rerun-sha", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 51, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)
		h.now = func() time.Time { return now }

		msg := model.JobMessage{
			Event:          model.EventCheckRun,
			Action:         "rerequested",
			DeliveryID:     "delivery-1",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "rerun-sha",
			CheckRunID:     50,
			ExternalID:     "750:1",
		}

		// First delivery schedules run-1 and finalizes claim-rerun#maplibre/mln-rs#delivery-1.
		resp, err := h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("first rerun failed: err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 1 {
			t.Fatalf("scheduledIn = %d, want 1", len(df.scheduledIn))
		}
		if len(gh.created) != 1 || gh.created[0].ExternalID != "750:1:delivery-1" {
			t.Fatalf("expected delivery-scoped external_id 750:1:delivery-1, got %+v", gh.created)
		}
		claimKey := model.RerunClaimKey("maplibre/mln-rs", "delivery-1")
		if rec := st.records[claimKey]; rec.State != model.StateClaimed || rec.LeaseExpiresAt != 0 {
			t.Fatalf("expected finalized CLAIMED rerun claim, got %+v", rec)
		}
		run1 := st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/run-1"]
		if run1.RerunDeliveryID != "delivery-1" {
			t.Fatalf("run1.RerunDeliveryID = %q, want delivery-1", run1.RerunDeliveryID)
		}

		// Duplicate SQS delivery with same DeliveryID is skipped cleanly (no second run, no self-supersede).
		resp, err = h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("duplicate rerun failed: err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 1 || len(df.stoppedARNs) != 0 {
			t.Fatalf("expected duplicate delivery skipped, got scheduled=%d stopped=%v", len(df.scheduledIn), df.stoppedARNs)
		}

		// Distinct DeliveryID is allowed to rerun again (superseding run-1 and scheduling run-2).
		msg.DeliveryID = "delivery-2"
		resp, err = h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("second distinct rerun failed: err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 2 {
			t.Fatalf("scheduledIn = %d, want 2 for distinct DeliveryID", len(df.scheduledIn))
		}
		if len(gh.created) != 2 || gh.created[1].ExternalID != "750:1:delivery-2" {
			t.Fatalf("expected second rerun external_id 750:1:delivery-2, got %+v", gh.created)
		}
		if st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/run-2"].RerunDeliveryID != "delivery-2" {
			t.Fatalf("expected run-2 RerunDeliveryID=delivery-2, got %+v", st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/run-2"])
		}
	})

	t.Run("transient error during rerun reuses existing open Check Run on retry", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 752, RunAttempt: 1, HeadBranch: "main", HeadSHA: "rerun-retry-sha", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 53, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		df := &fakeDF{uploadErr: errors.New("temporary upload 503")}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)
		h.now = func() time.Time { return now }

		msg := model.JobMessage{
			Event:          model.EventCheckRun,
			Action:         "rerequested",
			DeliveryID:     "delivery-retry",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "rerun-retry-sha",
			CheckRunID:     50,
			ExternalID:     "752:1:earlier-delivery",
		}

		// First attempt creates the open Scheduling Check Run, fails on upload, and releases the rerun claim.
		resp, err := h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 1 {
			t.Fatalf("expected 1 batch failure on transient upload error, got err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(gh.created) != 1 || gh.created[0].ExternalID != "752:1:delivery-retry" {
			t.Fatalf("expected 1 created check run with external_id 752:1:delivery-retry, got %+v", gh.created)
		}
		firstCheckID := gh.nextCheckID
		if _, stillClaimed := st.records[model.RerunClaimKey("maplibre/mln-rs", "delivery-retry")]; stillClaimed {
			t.Fatal("expected rerun claim to be released after transient upload error")
		}

		// SQS retry succeeds and reuses the exact same open Check Run ID instead of creating a duplicate.
		df.uploadErr = nil
		resp, err = h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("rerun retry failed: err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(gh.created) != 1 {
			t.Fatalf("expected retry to reuse open Check Run (1 created), got %d created", len(gh.created))
		}
		if len(df.scheduledIn) != 1 {
			t.Fatalf("scheduledIn = %d, want 1", len(df.scheduledIn))
		}
		runRec := st.records["arn:aws:devicefarm:us-west-2:373521797162:run:proj/run-1"]
		if runRec.CheckRunID != firstCheckID {
			t.Fatalf("persisted CheckRunID = %d, want reused %d", runRec.CheckRunID, firstCheckID)
		}
	})

	t.Run("crash after PutRun deduplicates rerun on expired lease takeover", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 751, RunAttempt: 1, HeadBranch: "main", HeadSHA: "rerun-crash-sha", CreatedAt: now}
		gh.workflowRun.HeadRepository.FullName = "maplibre/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 52, Name: "android-device-tests", CreatedAt: now}}

		st := newFakeStore()
		claimKey := model.RerunClaimKey("maplibre/mln-rs", "delivery-crash")
		st.records[claimKey] = model.RunRecord{
			RunARN:         claimKey,
			State:          model.StateClaiming,
			ClaimOwner:     "crashed-rerun-worker",
			LeaseExpiresAt: now.Add(-5 * time.Second).Unix(),
		}
		existingARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj/rerun-persisted"
		st.records[existingARN] = model.RunRecord{
			RunARN:             existingARN,
			State:              model.StateActive,
			RepoSHA:            model.RepoSHAKey("maplibre/mln-rs", "rerun-crash-sha"),
			Repo:               "maplibre/mln-rs",
			SHA:                "rerun-crash-sha",
			WorkflowRunID:      751,
			WorkflowRunAttempt: 1,
			RerunDeliveryID:    "delivery-crash",
		}

		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)
		h.now = func() time.Time { return now }

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:          model.EventCheckRun,
			Action:         "rerequested",
			DeliveryID:     "delivery-crash",
			Repo:           "maplibre/mln-rs",
			InstallationID: 99,
			HeadSHA:        "rerun-crash-sha",
			CheckRunID:     50,
			ExternalID:     "751:1",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.scheduledIn) != 0 {
			t.Fatalf("expected 0 new runs when rerun already persisted before crash, got %d", len(df.scheduledIn))
		}
		if st.records[claimKey].State != model.StateClaimed {
			t.Fatalf("expected rerun claim finalized to CLAIMED on takeover dedup, got %s", st.records[claimKey].State)
		}
	})
}

func TestWorkerSupersedeActiveRunsOrderingAndRetry(t *testing.T) {
	t1 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(5 * time.Minute)

	t.Run("UpdateCheckRun failure keeps superseded run ACTIVE and retries cleanly", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 510, RunAttempt: 1, HeadBranch: "feature", HeadSHA: "new-sha", CreatedAt: t2}
		gh.workflowRun.HeadRepository.FullName = "fork/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 61, Name: "android-device-tests", CreatedAt: t2}}
		gh.updateErrByCheckID[42] = errors.New("github 502")

		st := newFakeStore()
		oldARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj/old-run"
		st.records[oldARN] = model.RunRecord{
			RunARN:            oldARN,
			State:             model.StateActive,
			BranchKey:         model.BranchKey("maplibre/mln-rs", "fork/mln-rs", "feature"),
			RepoSHA:           model.RepoSHAKey("maplibre/mln-rs", "old-sha"),
			Repo:              "maplibre/mln-rs",
			SHA:               "old-sha",
			WorkflowRunID:     509,
			WorkflowCreatedAt: t1.Format(time.RFC3339),
			CheckRunID:        42,
			InstallationID:    99,
		}
		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		msg := model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "new-sha",
			HeadBranch:         "feature",
			WorkflowRunID:      510,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}

		// First attempt: UpdateCheckRun on check 42 fails -> returns batch failure, old run stays ACTIVE, claim released.
		resp, err := h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 1 {
			t.Fatalf("expected 1 batch failure when cancelling superseded check run fails, got err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if st.records[oldARN].State != model.StateActive {
			t.Fatalf("old run state = %s, want ACTIVE so retry/poller can settle check run", st.records[oldARN].State)
		}
		if len(df.stoppedARNs) != 1 || df.stoppedARNs[0] != oldARN {
			t.Fatalf("expected StopRun called before UpdateCheckRun, got %v", df.stoppedARNs)
		}
		if len(df.scheduledIn) != 0 {
			t.Fatalf("expected 0 schedules before retry, got %d", len(df.scheduledIn))
		}
		if _, stillClaimed := st.records[model.ClaimKey("maplibre/mln-rs", 510, 1)]; stillClaimed {
			t.Fatal("expected claim to be released after supersede UpdateCheckRun failure")
		}

		// Retry after GitHub recovers: old check run cancelled, old record marked SUPERSEDED, new run scheduled.
		delete(gh.updateErrByCheckID, 42)
		resp, err = h.Handle(context.Background(), sqsEventOf(t, msg))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("retry failed: err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if st.records[oldARN].State != model.StateSuperseded {
			t.Fatalf("old run state after retry = %s, want SUPERSEDED", st.records[oldARN].State)
		}
		if updates := gh.updated[42]; len(updates) != 1 || updates[0].Conclusion != "cancelled" {
			t.Fatalf("expected old check run 42 cancelled on retry, got %+v", updates)
		}
		if len(df.scheduledIn) != 1 {
			t.Fatalf("expected 1 run scheduled on retry, got %d", len(df.scheduledIn))
		}
	})

	t.Run("skips run that GetRun shows already COMPLETED", func(t *testing.T) {
		gh := newFakeGH()
		gh.workflowRun = github.WorkflowRun{ID: 511, RunAttempt: 1, HeadBranch: "feature", HeadSHA: "new-sha-2", CreatedAt: t2}
		gh.workflowRun.HeadRepository.FullName = "fork/mln-rs"
		gh.artifacts = []github.Artifact{{ID: 62, Name: "android-device-tests", CreatedAt: t2}}

		st := newFakeStore()
		finishedARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj/just-finished"
		baseRec := model.RunRecord{
			RunARN:            finishedARN,
			State:             model.StateActive, // stale GSI view
			BranchKey:         model.BranchKey("maplibre/mln-rs", "fork/mln-rs", "feature"),
			RepoSHA:           model.RepoSHAKey("maplibre/mln-rs", "old-sha-2"),
			Repo:              "maplibre/mln-rs",
			SHA:               "old-sha-2",
			WorkflowRunID:     508,
			WorkflowCreatedAt: t1.Format(time.RFC3339),
			CheckRunID:        43,
			InstallationID:    99,
		}
		st.records[finishedARN] = baseRec
		completedRec := baseRec
		completedRec.State = model.StateCompleted
		st.getRunOverrides[finishedARN] = completedRec

		df := &fakeDF{}
		h := NewHandler(testWorkerCfg(), gh, st, df, nil)

		resp, err := h.Handle(context.Background(), sqsEventOf(t, model.JobMessage{
			Event:              model.EventWorkflowJob,
			Action:             "completed",
			Repo:               "maplibre/mln-rs",
			InstallationID:     99,
			HeadSHA:            "new-sha-2",
			HeadBranch:         "feature",
			WorkflowRunID:      511,
			WorkflowRunAttempt: 1,
			JobConclusion:      "success",
		}))
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("Handle() err=%v failures=%v", err, resp.BatchItemFailures)
		}
		if len(df.stoppedARNs) != 0 {
			t.Fatalf("expected 0 StopRun calls for already COMPLETED run, got %v", df.stoppedARNs)
		}
		if len(gh.updated[43]) != 0 {
			t.Fatalf("expected completed check run 43 untouched, got %+v", gh.updated[43])
		}
		if st.records[finishedARN].State == model.StateSuperseded {
			t.Fatal("already COMPLETED run must not be marked SUPERSEDED")
		}
		if len(df.scheduledIn) != 1 {
			t.Fatalf("expected 1 new run scheduled, got %d", len(df.scheduledIn))
		}
	})
}
