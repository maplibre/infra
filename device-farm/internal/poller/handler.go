package poller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dftypes "github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/maplibre/infra/device-farm/internal/config"
	"github.com/maplibre/infra/device-farm/internal/devicefarm"
	"github.com/maplibre/infra/device-farm/internal/github"
	"github.com/maplibre/infra/device-farm/internal/model"
	"github.com/maplibre/infra/device-farm/internal/report"
	"github.com/maplibre/infra/device-farm/internal/store"
)

const (
	maxOutputFileBytes   = 64 << 20  // 64 MiB
	maxArtifactFileBytes = 256 << 20 // 256 MiB
)

// GitHubAPI is the subset of the GitHub client needed by poller.
type GitHubAPI interface {
	UpdateCheckRun(ctx context.Context, installationID int64, owner, repo string, checkRunID int64, in github.UpdateCheckRunRequest) (github.CheckRun, error)
}

// StoreAPI is the subset of DynamoDB store operations needed by poller.
type StoreAPI interface {
	QueryActiveRuns(ctx context.Context) ([]model.RunRecord, error)
	MarkCompleted(ctx context.Context, runARN, result string, completedAt time.Time) error
	MarkFailed(ctx context.Context, runARN, result string, completedAt time.Time) error
}

// DeviceFarmAPI is the subset of Device Farm operations needed by poller.
type DeviceFarmAPI interface {
	GetRun(ctx context.Context, runARN string) (*dftypes.Run, error)
	ListJobs(ctx context.Context, runARN string) ([]dftypes.Job, error)
	ListFileArtifacts(ctx context.Context, jobARN string) ([]dftypes.Artifact, error)
}

// S3API is the subset of S3 operations needed by poller.
type S3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Handler polls active Device Farm runs, uploads results to S3, and settles GitHub Check Runs.
type Handler struct {
	cfg        config.Config
	gh         GitHubAPI
	store      StoreAPI
	df         DeviceFarmAPI
	s3         S3API
	httpClient *http.Client
	now        func() time.Time
	logger     *slog.Logger
}

// NewHandler constructs a poller Lambda handler.
func NewHandler(cfg config.Config, gh GitHubAPI, st StoreAPI, df DeviceFarmAPI, s3Client S3API, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		cfg:        cfg,
		gh:         gh,
		store:      st,
		df:         df,
		s3:         s3Client,
		httpClient: &http.Client{Timeout: 60 * time.Second},
		now:        time.Now,
		logger:     logger,
	}
}

// Handle checks all ACTIVE runs in DynamoDB and finishes any that have COMPLETED.
func (h *Handler) Handle(ctx context.Context) error {
	active, err := h.store.QueryActiveRuns(ctx)
	if err != nil {
		return err
	}
	if len(active) == 0 {
		return nil
	}

	var errs []error
	for _, rec := range active {
		if err := h.pollRun(ctx, rec); err != nil {
			h.logger.Error("failed to poll active run", "run_arn", rec.RunARN, "err", err)
			errs = append(errs, fmt.Errorf("run %s: %w", rec.RunARN, err))
		}
	}
	return errors.Join(errs...)
}

func (h *Handler) pollRun(ctx context.Context, rec model.RunRecord) error {
	repo, ok := h.cfg.LookupRepo(rec.Repo)
	if !ok {
		h.logger.Warn("active run references unconfigured repo", "repo", rec.Repo, "run_arn", rec.RunARN)
		return nil
	}

	run, err := h.df.GetRun(ctx, rec.RunARN)
	if err != nil {
		if errors.Is(err, devicefarm.ErrRunNotFound) {
			now := h.now().UTC()
			if rec.CheckRunID > 0 {
				if _, updateErr := h.gh.UpdateCheckRun(ctx, rec.InstallationID, repo.Owner, repo.Name, rec.CheckRunID, github.UpdateCheckRunRequest{
					Status:      "completed",
					Conclusion:  "failure",
					CompletedAt: now.Format(time.RFC3339),
					Output: &github.CheckRunOutput{
						Title:   "Device Farm run not found",
						Summary: "The AWS Device Farm run could not be found (it may have been deleted).",
					},
				}); updateErr != nil {
					return fmt.Errorf("update check run %d for missing run: %w", rec.CheckRunID, updateErr)
				}
			}
			if markErr := h.store.MarkFailed(ctx, rec.RunARN, "NOT_FOUND", now); markErr != nil && !errors.Is(markErr, store.ErrStateConflict) {
				return markErr
			}
			return nil
		}
		return err
	}

	if run.Status != dftypes.ExecutionStatusCompleted {
		return nil
	}

	tmpDir, err := os.MkdirTemp("", "device-farm-poller-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	jobs, err := h.df.ListJobs(ctx, rec.RunARN)
	if err != nil {
		return err
	}
	sort.Slice(jobs, func(i, j int) bool {
		ni := jobDeviceName(jobs[i])
		nj := jobDeviceName(jobs[j])
		if ni != nj {
			return ni < nj
		}
		return aws.ToString(jobs[i].Arn) < aws.ToString(jobs[j].Arn)
	})

	runID := devicefarm.RunIDFromARN(rec.RunARN)
	slugCounts := make(map[string]int, len(jobs))
	phones := make([]report.PhoneReport, 0, len(jobs))

	for _, job := range jobs {
		phoneName := jobDeviceName(job)
		baseSlug := report.PhoneSlug(phoneName)
		slugCounts[baseSlug]++
		phoneSlug := baseSlug
		if slugCounts[baseSlug] > 1 {
			phoneSlug = fmt.Sprintf("%s-%d", baseSlug, slugCounts[baseSlug])
		}

		phoneDir := filepath.Join(tmpDir, phoneSlug)
		if err := os.MkdirAll(phoneDir, 0o700); err != nil {
			return err
		}

		phoneRep, err := h.collectJob(ctx, repo, rec, runID, phoneName, phoneSlug, phoneDir, job)
		if err != nil {
			return err
		}
		phones = append(phones, phoneRep)
	}

	consoleURL := devicefarm.ConsoleURL(rec.RunARN)
	checkOut := report.BuildCheckOutput(run.Result, consoleURL, phones)
	conclusion := report.MapRunResult(run.Result)
	now := h.now().UTC()

	if rec.CheckRunID > 0 {
		_, err = h.gh.UpdateCheckRun(ctx, rec.InstallationID, repo.Owner, repo.Name, rec.CheckRunID, github.UpdateCheckRunRequest{
			Status:      "completed",
			Conclusion:  conclusion,
			CompletedAt: now.Format(time.RFC3339),
			DetailsURL:  consoleURL,
			Output: &github.CheckRunOutput{
				Title:   checkOut.Title,
				Summary: checkOut.Summary,
				Text:    checkOut.Text,
			},
		})
		if err != nil {
			return fmt.Errorf("update check run %d: %w", rec.CheckRunID, err)
		}
	}

	if err := h.store.MarkCompleted(ctx, rec.RunARN, string(run.Result), now); err != nil && !errors.Is(err, store.ErrStateConflict) {
		return err
	}

	h.logger.Info("completed device farm run", "repo", rec.Repo, "sha", rec.SHA, "run_arn", rec.RunARN, "result", run.Result, "conclusion", conclusion)
	return nil
}

func (h *Handler) collectJob(ctx context.Context, repo config.RepoConfig, rec model.RunRecord, runID, phoneName, phoneSlug, phoneDir string, job dftypes.Job) (report.PhoneReport, error) {
	var minutes float64
	if job.DeviceMinutes != nil && job.DeviceMinutes.Total != nil {
		minutes = *job.DeviceMinutes.Total
	}
	pr := report.PhoneReport{
		PhoneName:     phoneName,
		PhoneSlug:     phoneSlug,
		Result:        string(job.Result),
		DeviceMinutes: minutes,
	}

	artifacts, err := h.df.ListFileArtifacts(ctx, aws.ToString(job.Arn))
	if err != nil {
		return pr, err
	}

	var outputURL, artifactURL string
	for _, a := range artifacts {
		u := aws.ToString(a.Url)
		if u == "" {
			continue
		}
		switch a.Type {
		case dftypes.ArtifactTypeTestspecOutput:
			if outputURL == "" {
				outputURL = u
			}
		case dftypes.ArtifactTypeCustomerArtifact:
			if artifactURL == "" {
				artifactURL = u
			}
		}
	}

	keyPrefix := fmt.Sprintf("%s/%s/%s/%s", repo.Name, rec.SHA, runID, phoneSlug)

	var testspecBytes []byte
	if outputURL != "" {
		outputPath := filepath.Join(phoneDir, "TESTSPEC_OUTPUT.txt")
		if err := h.downloadToFile(ctx, outputURL, outputPath, maxOutputFileBytes); err != nil {
			h.logger.Warn("failed to download TESTSPEC_OUTPUT", "phone", phoneName, "err", err)
		} else {
			key := keyPrefix + "/TESTSPEC_OUTPUT.txt"
			if err := h.uploadFileToS3(ctx, key, outputPath, "text/plain; charset=utf-8", "TESTSPEC_OUTPUT.txt"); err != nil {
				return pr, err
			}
			pr.OutputURL = h.cloudfrontURL(key)
			testspecBytes, _ = os.ReadFile(outputPath)
		}
	}

	if artifactURL != "" {
		zipPath := filepath.Join(phoneDir, "CUSTOMER_ARTIFACT.zip")
		if err := h.downloadToFile(ctx, artifactURL, zipPath, maxArtifactFileBytes); err != nil {
			h.logger.Warn("failed to download CUSTOMER_ARTIFACT", "phone", phoneName, "err", err)
		} else {
			key := keyPrefix + "/CUSTOMER_ARTIFACT.zip"
			if err := h.uploadFileToS3(ctx, key, zipPath, "application/zip", "CUSTOMER_ARTIFACT.zip"); err != nil {
				return pr, err
			}
			pr.ArtifactURL = h.cloudfrontURL(key)

			junitRaw, found, junitErr := report.ExtractJUnitFromZip(zipPath, report.DefaultJUnitMaxBytes)
			if junitErr != nil {
				h.logger.Warn("failed to extract junit.xml", "phone", phoneName, "err", junitErr)
			} else if found {
				junitKey := keyPrefix + "/junit.xml"
				if err := h.uploadBytesToS3(ctx, junitKey, junitRaw, "application/xml", "junit.xml"); err != nil {
					return pr, err
				}
				pr.JUnitURL = h.cloudfrontURL(junitKey)

				parsed, parseErr := report.ParseJUnit(junitRaw)
				if parseErr != nil {
					h.logger.Warn("failed to parse junit.xml", "phone", phoneName, "err", parseErr)
				} else {
					pr.JUnit = &parsed
				}
			}
		}
	}

	if pr.JUnit == nil {
		pr.OutputTailLines = report.TailLines(testspecBytes, report.DefaultTailLines)
	}

	return pr, nil
}

func (h *Handler) downloadToFile(ctx context.Context, rawURL, dstPath string, maxBytes int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download returned HTTP %d", resp.StatusCode)
	}

	f, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	n, copyErr := io.Copy(f, io.LimitReader(resp.Body, maxBytes+1))
	closeErr := f.Close()
	if copyErr != nil {
		os.Remove(dstPath)
		return copyErr
	}
	if closeErr != nil {
		os.Remove(dstPath)
		return closeErr
	}
	if n > maxBytes {
		os.Remove(dstPath)
		return fmt.Errorf("artifact exceeds max size %d bytes", maxBytes)
	}
	return nil
}

func (h *Handler) uploadFileToS3(ctx context.Context, key, filePath, contentType, filename string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = h.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:             aws.String(h.cfg.ResultsBucketName),
		Key:                aws.String(key),
		Body:               f,
		ContentType:        aws.String(contentType),
		ContentDisposition: aws.String(fmt.Sprintf(`attachment; filename="%s"`, filename)),
	})
	if err != nil {
		return fmt.Errorf("put s3://%s/%s: %w", h.cfg.ResultsBucketName, key, err)
	}
	return nil
}

func (h *Handler) uploadBytesToS3(ctx context.Context, key string, data []byte, contentType, filename string) error {
	_, err := h.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:             aws.String(h.cfg.ResultsBucketName),
		Key:                aws.String(key),
		Body:               bytes.NewReader(data),
		ContentType:        aws.String(contentType),
		ContentDisposition: aws.String(fmt.Sprintf(`attachment; filename="%s"`, filename)),
	})
	if err != nil {
		return fmt.Errorf("put s3://%s/%s: %w", h.cfg.ResultsBucketName, key, err)
	}
	return nil
}

func (h *Handler) cloudfrontURL(key string) string {
	parts := strings.Split(key, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return h.cfg.ResultsBaseURL + "/" + strings.Join(parts, "/")
}

func jobDeviceName(job dftypes.Job) string {
	if job.Device != nil && aws.ToString(job.Device.Name) != "" {
		return aws.ToString(job.Device.Name)
	}
	if name := aws.ToString(job.Name); name != "" {
		return name
	}
	return "Unknown device"
}
