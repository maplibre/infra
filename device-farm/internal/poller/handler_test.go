package poller

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dftypes "github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/maplibre/infra/device-farm/internal/config"
	"github.com/maplibre/infra/device-farm/internal/devicefarm"
	"github.com/maplibre/infra/device-farm/internal/github"
	"github.com/maplibre/infra/device-farm/internal/model"
)

type fakeGH struct {
	updates   []github.UpdateCheckRunRequest
	updateErr error
}

func (f *fakeGH) UpdateCheckRun(_ context.Context, _ int64, _, _ string, _ int64, in github.UpdateCheckRunRequest) (github.CheckRun, error) {
	if f.updateErr != nil {
		return github.CheckRun{}, f.updateErr
	}
	f.updates = append(f.updates, in)
	return github.CheckRun{}, nil
}

type fakeStore struct {
	active    []model.RunRecord
	completed map[string]string
}

func (s *fakeStore) QueryActiveRuns(context.Context) ([]model.RunRecord, error) {
	return s.active, nil
}

func (s *fakeStore) MarkCompleted(_ context.Context, arn, result string, _ time.Time) error {
	if s.completed == nil {
		s.completed = make(map[string]string)
	}
	s.completed[arn] = result
	return nil
}

func (s *fakeStore) MarkFailed(_ context.Context, arn, result string, _ time.Time) error {
	if s.completed == nil {
		s.completed = make(map[string]string)
	}
	s.completed[arn] = "FAILED:" + result
	return nil
}

type fakeDF struct {
	run       *dftypes.Run
	getRunErr error
	jobs      []dftypes.Job
	artifacts map[string][]dftypes.Artifact
}

func (d *fakeDF) GetRun(_ context.Context, _ string) (*dftypes.Run, error) {
	if d.getRunErr != nil {
		return nil, d.getRunErr
	}
	return d.run, nil
}

func (d *fakeDF) ListJobs(_ context.Context, _ string) ([]dftypes.Job, error) {
	return d.jobs, nil
}

func (d *fakeDF) ListFileArtifacts(_ context.Context, jobARN string) ([]dftypes.Artifact, error) {
	return d.artifacts[jobARN], nil
}

type uploadedObject struct {
	contentType        string
	contentDisposition string
	body               []byte
}

type fakeS3 struct {
	objects map[string]uploadedObject
}

func (s *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if s.objects == nil {
		s.objects = make(map[string]uploadedObject)
	}
	b, _ := io.ReadAll(in.Body)
	s.objects[aws.ToString(in.Key)] = uploadedObject{
		contentType:        aws.ToString(in.ContentType),
		contentDisposition: aws.ToString(in.ContentDisposition),
		body:               b,
	}
	return &s3.PutObjectOutput{}, nil
}

func buildCustomerArtifactZip(t *testing.T, junitXML string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("Host_Machine_Files/$DEVICEFARM_LOG_DIR/junit.xml")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte(junitXML))
	_ = zw.Close()
	return buf.Bytes()
}

func testPollerCfg() config.Config {
	cfg, err := config.LoadFromLookup(func(k string) (string, bool) {
		m := map[string]string{
			"MODE":                  "poller",
			"GITHUB_APP_SECRET_ARN": "arn:secret",
			"RUNS_TABLE_NAME":       "runs",
			"RESULTS_BUCKET_NAME":   "results-bucket",
			"RESULTS_BASE_URL":      "https://d123.cloudfront.net",
		}
		v, ok := m[k]
		return v, ok
	})
	if err != nil {
		panic(err)
	}
	return cfg
}

func TestPollerCompletesRunAndUploadsResults(t *testing.T) {
	junitXML := `<?xml version="1.0"?><testsuites tests="2" failures="0"><testsuite><testcase classname="a" name="t1"/><testcase classname="a" name="t2"/></testsuite></testsuites>`
	zipBytes := buildCustomerArtifactZip(t, junitXML)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/pixel/output.txt":
			_, _ = w.Write([]byte("line 1\nline 2\n"))
		case "/pixel/artifact.zip":
			_, _ = w.Write(zipBytes)
		case "/s25/output.txt":
			_, _ = w.Write([]byte("adb error\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	runARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj-id/run-id"
	st := &fakeStore{
		active: []model.RunRecord{
			{
				RunARN:         runARN,
				State:          model.StateActive,
				Repo:           "maplibre/mln-rs",
				SHA:            "abcdef123456",
				CheckRunID:     999,
				InstallationID: 77,
			},
		},
	}
	df := &fakeDF{
		run: &dftypes.Run{
			Arn:    aws.String(runARN),
			Status: dftypes.ExecutionStatusCompleted,
			Result: dftypes.ExecutionResultFailed,
		},
		jobs: []dftypes.Job{
			{
				Arn:           aws.String("job-s25"),
				Device:        &dftypes.Device{Name: aws.String("Samsung Galaxy S25")},
				Result:        dftypes.ExecutionResultErrored,
				DeviceMinutes: &dftypes.DeviceMinutes{Total: aws.Float64(1.2)},
			},
			{
				Arn:           aws.String("job-pixel"),
				Device:        &dftypes.Device{Name: aws.String("Google Pixel 9")},
				Result:        dftypes.ExecutionResultPassed,
				DeviceMinutes: &dftypes.DeviceMinutes{Total: aws.Float64(3.5)},
			},
		},
		artifacts: map[string][]dftypes.Artifact{
			"job-pixel": {
				{Type: dftypes.ArtifactTypeTestspecOutput, Url: aws.String(srv.URL + "/pixel/output.txt")},
				{Type: dftypes.ArtifactTypeCustomerArtifact, Url: aws.String(srv.URL + "/pixel/artifact.zip")},
			},
			"job-s25": {
				{Type: dftypes.ArtifactTypeTestspecOutput, Url: aws.String(srv.URL + "/s25/output.txt")},
			},
		},
	}
	gh := &fakeGH{}
	s3c := &fakeS3{}

	h := NewHandler(testPollerCfg(), gh, st, df, s3c, nil)
	if err := h.Handle(context.Background()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify S3 uploads have attachment Content-Disposition and right keys.
	pixelOutKey := "mln-rs/abcdef123456/run-id/Google-Pixel-9/TESTSPEC_OUTPUT.txt"
	pixelZipKey := "mln-rs/abcdef123456/run-id/Google-Pixel-9/CUSTOMER_ARTIFACT.zip"
	pixelJUnitKey := "mln-rs/abcdef123456/run-id/Google-Pixel-9/junit.xml"
	s25OutKey := "mln-rs/abcdef123456/run-id/Samsung-Galaxy-S25/TESTSPEC_OUTPUT.txt"

	for _, key := range []string{pixelOutKey, pixelZipKey, pixelJUnitKey, s25OutKey} {
		obj, ok := s3c.objects[key]
		if !ok {
			t.Fatalf("missing uploaded S3 key %s", key)
		}
		if !strings.HasPrefix(obj.contentDisposition, "attachment;") {
			t.Fatalf("key %s Content-Disposition = %q, want attachment", key, obj.contentDisposition)
		}
	}

	// Verify Check Run completed with failure, CloudFront links, and both phones.
	if len(gh.updates) != 1 {
		t.Fatalf("expected 1 check run update, got %d", len(gh.updates))
	}
	u := gh.updates[0]
	if u.Status != "completed" || u.Conclusion != "failure" {
		t.Fatalf("status=%q conclusion=%q", u.Status, u.Conclusion)
	}
	if !strings.Contains(u.Output.Summary, "https://d123.cloudfront.net/"+pixelOutKey) {
		t.Fatalf("summary missing CloudFront link:\n%s", u.Output.Summary)
	}
	if !strings.Contains(u.Output.Text, "2 of 2 tests passed.") || !strings.Contains(u.Output.Text, "adb error") {
		t.Fatalf("text missing expected sections:\n%s", u.Output.Text)
	}

	// Verify DynamoDB marked COMPLETED.
	if st.completed[runARN] != "FAILED" {
		t.Fatalf("completed state = %q, want FAILED", st.completed[runARN])
	}
}

func TestPollerLeavesRunningRunAlone(t *testing.T) {
	st := &fakeStore{
		active: []model.RunRecord{{RunARN: "arn:aws:devicefarm:us-west-2:1:run:p/r", Repo: "maplibre/mln-rs"}},
	}
	df := &fakeDF{run: &dftypes.Run{Status: dftypes.ExecutionStatusRunning}}
	gh := &fakeGH{}
	s3c := &fakeS3{}

	h := NewHandler(testPollerCfg(), gh, st, df, s3c, nil)
	if err := h.Handle(context.Background()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(gh.updates) != 0 || len(s3c.objects) != 0 || len(st.completed) != 0 {
		t.Fatal("expected running run to be left untouched")
	}
}

func TestPollerNotFoundRetriesUntilCheckRunUpdated(t *testing.T) {
	runARN := "arn:aws:devicefarm:us-west-2:373521797162:run:proj-id/missing-run"
	st := &fakeStore{
		active: []model.RunRecord{
			{
				RunARN:         runARN,
				State:          model.StateActive,
				Repo:           "maplibre/mln-rs",
				SHA:            "abcdef123456",
				CheckRunID:     321,
				InstallationID: 77,
			},
		},
	}
	df := &fakeDF{getRunErr: devicefarm.ErrRunNotFound}
	gh := &fakeGH{updateErr: errors.New("github 502")}
	s3c := &fakeS3{}

	h := NewHandler(testPollerCfg(), gh, st, df, s3c, nil)

	// First poll: UpdateCheckRun fails transiently -> Handle returns error and leaves record ACTIVE.
	if err := h.Handle(context.Background()); err == nil {
		t.Fatal("expected error when UpdateCheckRun fails on ErrRunNotFound")
	}
	if len(st.completed) != 0 {
		t.Fatalf("expected record to remain ACTIVE after Check Run update error, got completed=%v", st.completed)
	}

	// Next poll: UpdateCheckRun succeeds -> Check Run marked failure and DynamoDB marked FAILED:NOT_FOUND.
	gh.updateErr = nil
	if err := h.Handle(context.Background()); err != nil {
		t.Fatalf("Handle() retry error = %v", err)
	}
	if len(gh.updates) != 1 || gh.updates[0].Status != "completed" || gh.updates[0].Conclusion != "failure" {
		t.Fatalf("unexpected check run updates: %+v", gh.updates)
	}
	if st.completed[runARN] != "FAILED:NOT_FOUND" {
		t.Fatalf("completed state = %q, want FAILED:NOT_FOUND", st.completed[runARN])
	}
}
