package devicefarm

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
)

type fakeDeviceFarmAPI struct {
	uploadURL      string
	getUploadCalls int
	rejectMessage  string
	lastScheduleIn *devicefarm.ScheduleRunInput
}

func (f *fakeDeviceFarmAPI) CreateUpload(_ context.Context, _ *devicefarm.CreateUploadInput, _ ...func(*devicefarm.Options)) (*devicefarm.CreateUploadOutput, error) {
	return &devicefarm.CreateUploadOutput{
		Upload: &types.Upload{
			Arn: aws.String("arn:aws:devicefarm:us-west-2:373521797162:upload:proj/up1"),
			Url: aws.String(f.uploadURL),
		},
	}, nil
}

func (f *fakeDeviceFarmAPI) GetUpload(_ context.Context, _ *devicefarm.GetUploadInput, _ ...func(*devicefarm.Options)) (*devicefarm.GetUploadOutput, error) {
	f.getUploadCalls++
	if f.rejectMessage != "" {
		return &devicefarm.GetUploadOutput{
			Upload: &types.Upload{
				Status:  types.UploadStatusFailed,
				Message: aws.String(f.rejectMessage),
			},
		}, nil
	}
	status := types.UploadStatusProcessing
	if f.getUploadCalls >= 2 {
		status = types.UploadStatusSucceeded
	}
	return &devicefarm.GetUploadOutput{
		Upload: &types.Upload{Status: status},
	}, nil
}

func (f *fakeDeviceFarmAPI) ScheduleRun(_ context.Context, in *devicefarm.ScheduleRunInput, _ ...func(*devicefarm.Options)) (*devicefarm.ScheduleRunOutput, error) {
	f.lastScheduleIn = in
	return &devicefarm.ScheduleRunOutput{
		Run: &types.Run{Arn: aws.String("arn:aws:devicefarm:us-west-2:373521797162:run:proj/run1")},
	}, nil
}

func (f *fakeDeviceFarmAPI) StopRun(_ context.Context, _ *devicefarm.StopRunInput, _ ...func(*devicefarm.Options)) (*devicefarm.StopRunOutput, error) {
	return &devicefarm.StopRunOutput{}, nil
}

func (f *fakeDeviceFarmAPI) GetRun(_ context.Context, _ *devicefarm.GetRunInput, _ ...func(*devicefarm.Options)) (*devicefarm.GetRunOutput, error) {
	return &devicefarm.GetRunOutput{}, nil
}

func (f *fakeDeviceFarmAPI) ListJobs(_ context.Context, _ *devicefarm.ListJobsInput, _ ...func(*devicefarm.Options)) (*devicefarm.ListJobsOutput, error) {
	return &devicefarm.ListJobsOutput{}, nil
}

func (f *fakeDeviceFarmAPI) ListArtifacts(_ context.Context, _ *devicefarm.ListArtifactsInput, _ ...func(*devicefarm.Options)) (*devicefarm.ListArtifactsOutput, error) {
	return &devicefarm.ListArtifactsOutput{}, nil
}

func TestConsoleURLAndRunID(t *testing.T) {
	arn := "arn:aws:devicefarm:us-west-2:373521797162:run:c5c3f5f6-e21b-41c4-a235-489d30075aeb/11111111-2222-3333-4444-555555555555"
	wantURL := "https://us-west-2.console.aws.amazon.com/devicefarm/home?region=us-west-2#/mobile/projects/c5c3f5f6-e21b-41c4-a235-489d30075aeb/runs/11111111-2222-3333-4444-555555555555"
	if got := ConsoleURL(arn); got != wantURL {
		t.Fatalf("ConsoleURL = %q, want %q", got, wantURL)
	}
	if got := RunIDFromARN(arn); got != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("RunIDFromARN = %q, want run id", got)
	}
}

func TestUploadFileAndScheduleRun(t *testing.T) {
	var uploadedBody string
	var gotContentLength int64
	var gotContentType string

	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		gotContentLength = r.ContentLength
		gotContentType = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		uploadedBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer s3Server.Close()

	fake := &fakeDeviceFarmAPI{uploadURL: s3Server.URL + "/presigned"}
	client := NewClient(fake)
	client.pollInterval = time.Millisecond

	tmpFile := filepath.Join(t.TempDir(), "android.yml")
	if err := os.WriteFile(tmpFile, []byte("version: 0.1\n"), 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	uploadARN, err := client.UploadFile(context.Background(), "arn:proj", "android.yml", types.UploadTypeAppiumWebNodeTestSpec, tmpFile)
	if err != nil {
		t.Fatalf("UploadFile() error = %v", err)
	}
	if uploadARN != "arn:aws:devicefarm:us-west-2:373521797162:upload:proj/up1" {
		t.Fatalf("unexpected uploadARN %q", uploadARN)
	}
	if uploadedBody != "version: 0.1\n" || gotContentLength != 13 || gotContentType != "" {
		t.Fatalf("unexpected upload PUT: body=%q len=%d contentType=%q", uploadedBody, gotContentLength, gotContentType)
	}
	if fake.getUploadCalls != 2 {
		t.Fatalf("getUploadCalls = %d, want 2", fake.getUploadCalls)
	}

	runARN, err := client.ScheduleRun(context.Background(), ScheduleInput{
		ProjectARN:        "arn:proj",
		RunName:           "mln-rs abcdef1 main",
		Devices:           []string{"arn:dev1", "arn:dev2"},
		TestPackageARN:    "arn:pkg",
		TestSpecARN:       "arn:spec",
		JobTimeoutMinutes: 60,
	})
	if err != nil {
		t.Fatalf("ScheduleRun() error = %v", err)
	}
	if runARN != "arn:aws:devicefarm:us-west-2:373521797162:run:proj/run1" {
		t.Fatalf("runARN = %q", runARN)
	}
	if aws.ToBool(fake.lastScheduleIn.ExecutionConfiguration.VideoCapture) != false {
		t.Fatal("expected VideoCapture=false")
	}
	if aws.ToInt32(fake.lastScheduleIn.DeviceSelectionConfiguration.MaxDevices) != 2 {
		t.Fatal("expected MaxDevices=2")
	}
}

func TestUploadFileReturnsTypedRejectionError(t *testing.T) {
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer s3Server.Close()

	fake := &fakeDeviceFarmAPI{
		uploadURL:     s3Server.URL + "/presigned",
		rejectMessage: "invalid zip archive",
	}
	client := NewClient(fake)
	client.pollInterval = time.Millisecond

	tmpFile := filepath.Join(t.TempDir(), "android-tests.zip")
	if err := os.WriteFile(tmpFile, []byte("not-a-zip"), 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	_, err := client.UploadFile(context.Background(), "arn:proj", "android-tests.zip", types.UploadTypeAppiumWebNodeTestPackage, tmpFile)
	if !errors.Is(err, ErrUploadRejected) {
		t.Fatalf("expected ErrUploadRejected, got %v", err)
	}
	var rejected *UploadRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("expected *UploadRejectedError, got %T", err)
	}
	if rejected.Name != "android-tests.zip" || rejected.Message != "invalid zip archive" {
		t.Fatalf("unexpected rejected error: %+v", rejected)
	}
}
