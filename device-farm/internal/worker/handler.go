package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	dftypes "github.com/aws/aws-sdk-go-v2/service/devicefarm/types"

	"github.com/maplibre/infra/device-farm/internal/config"
	"github.com/maplibre/infra/device-farm/internal/devicefarm"
	"github.com/maplibre/infra/device-farm/internal/github"
	"github.com/maplibre/infra/device-farm/internal/model"
	"github.com/maplibre/infra/device-farm/internal/report"
	"github.com/maplibre/infra/device-farm/internal/store"
)

const maxArtifactZipBytes = 512 << 20 // 512 MiB

// GitHubAPI is the subset of the GitHub client needed by worker.
type GitHubAPI interface {
	CreateCheckRun(ctx context.Context, installationID int64, owner, repo string, in github.CreateCheckRunRequest) (github.CheckRun, error)
	UpdateCheckRun(ctx context.Context, installationID int64, owner, repo string, checkRunID int64, in github.UpdateCheckRunRequest) (github.CheckRun, error)
	ListCheckRunsForRef(ctx context.Context, installationID int64, owner, repo, ref, checkName string) ([]github.CheckRun, error)
	GetWorkflowRun(ctx context.Context, installationID int64, owner, repo string, runID int64) (github.WorkflowRun, error)
	ListWorkflowRunJobs(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]github.WorkflowJob, error)
	ListWorkflowRunArtifacts(ctx context.Context, installationID int64, owner, repo string, runID int64, name string) ([]github.Artifact, error)
	DownloadArtifact(ctx context.Context, installationID int64, owner, repo string, artifactID int64, dstPath string, maxBytes int64) error
	GetRawFile(ctx context.Context, installationID int64, owner, repo, filePath, ref string) ([]byte, error)
}

// StoreAPI is the subset of DynamoDB store operations needed by worker.
type StoreAPI interface {
	ClaimWorkflowRun(ctx context.Context, claimKey string, now time.Time) error
	PutRun(ctx context.Context, rec model.RunRecord) error
	DeleteRun(ctx context.Context, runARN string) error
	QueryActiveByBranchKey(ctx context.Context, branchKey string) ([]model.RunRecord, error)
	QueryByRepoSHA(ctx context.Context, repo, sha string) ([]model.RunRecord, error)
	MarkSuperseded(ctx context.Context, runARN string, completedAt time.Time) error
}

// DeviceFarmAPI is the subset of Device Farm operations needed by worker.
type DeviceFarmAPI interface {
	UploadFile(ctx context.Context, projectARN, name string, uploadType dftypes.UploadType, filePath string) (string, error)
	ScheduleRun(ctx context.Context, in devicefarm.ScheduleInput) (string, error)
	StopRun(ctx context.Context, runARN string) error
}

// Handler consumes SQS job messages and schedules AWS Device Farm runs.
type Handler struct {
	cfg    config.Config
	gh     GitHubAPI
	store  StoreAPI
	df     DeviceFarmAPI
	now    func() time.Time
	logger *slog.Logger
}

// NewHandler constructs a worker Lambda handler.
func NewHandler(cfg config.Config, gh GitHubAPI, st StoreAPI, df DeviceFarmAPI, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		cfg:    cfg,
		gh:     gh,
		store:  st,
		df:     df,
		now:    time.Now,
		logger: logger,
	}
}

// Handle processes an SQS batch using ReportBatchItemFailures semantics.
func (h *Handler) Handle(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, rec := range event.Records {
		if err := h.processRecord(ctx, rec); err != nil {
			h.logger.Error("worker failed to process SQS message", "message_id", rec.MessageId, "err", err)
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
				ItemIdentifier: rec.MessageId,
			})
		}
	}
	return resp, nil
}

func (h *Handler) processRecord(ctx context.Context, rec events.SQSMessage) error {
	var msg model.JobMessage
	if err := json.Unmarshal([]byte(rec.Body), &msg); err != nil {
		// Malformed messages cannot succeed on retry; log and drop.
		h.logger.Error("dropping malformed SQS message", "message_id", rec.MessageId, "err", err)
		return nil
	}

	repoCfg, ok := h.cfg.LookupRepo(msg.Repo)
	if !ok {
		h.logger.Info("ignoring unconfigured repo in worker", "repo", msg.Repo)
		return nil
	}

	switch msg.Event {
	case model.EventWorkflowJob:
		return h.handleWorkflowJob(ctx, repoCfg, msg)
	case model.EventWorkflowRun:
		return h.handleWorkflowRun(ctx, repoCfg, msg)
	case model.EventCheckRun:
		return h.handleCheckRunRerequested(ctx, repoCfg, msg)
	default:
		h.logger.Warn("ignoring unknown event kind", "event", msg.Event)
		return nil
	}
}

func (h *Handler) handleWorkflowJob(ctx context.Context, repo config.RepoConfig, msg model.JobMessage) error {
	if msg.Action != "completed" {
		return nil
	}
	if msg.HeadBranch != "" && !h.cfg.BranchAllowed(msg.HeadBranch) {
		return nil
	}

	if msg.JobConclusion == "success" {
		return h.scheduleDeviceFarmRun(ctx, repo, msg, false)
	}

	extID := model.ExternalID(msg.WorkflowRunID, msg.WorkflowRunAttempt)
	if settled, _, err := h.findCheckRunForExternalID(ctx, repo, msg.InstallationID, msg.HeadSHA, extID); err != nil {
		return err
	} else if settled {
		return nil
	}

	switch msg.JobConclusion {
	case "skipped":
		_, err := h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, 0,
			"success", "Not needed", "Not needed: the plan left the Android tests out")
		return err
	case "cancelled":
		_, err := h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, 0,
			"cancelled", "Cancelled", "CI couldn't build the Android test package.")
		return err
	default:
		// failure, timed_out, action_required, stale, etc.
		_, err := h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, 0,
			"failure", "Build failed", "CI couldn't build the Android test package.")
		return err
	}
}

func (h *Handler) handleWorkflowRun(ctx context.Context, repo config.RepoConfig, msg model.JobMessage) error {
	if msg.Action != "completed" {
		return nil
	}
	if msg.HeadBranch != "" && !h.cfg.BranchAllowed(msg.HeadBranch) {
		return nil
	}

	attempt := normalizeAttempt(msg.WorkflowRunAttempt)
	extID := model.ExternalID(msg.WorkflowRunID, attempt)

	settled, existingOpenID, err := h.findCheckRunForExternalID(ctx, repo, msg.InstallationID, msg.HeadSHA, extID)
	if err != nil {
		return err
	}
	if settled {
		return nil
	}

	// If Device Farm already has an active or completed run for this exact attempt, do nothing.
	alreadyRan, err := h.hasRunForAttempt(ctx, repo.FullName, msg.HeadSHA, msg.WorkflowRunID, attempt)
	if err != nil {
		return err
	}
	if alreadyRan {
		return nil
	}

	jobs, err := h.gh.ListWorkflowRunJobs(ctx, msg.InstallationID, repo.Owner, repo.Name, msg.WorkflowRunID)
	if err != nil {
		return err
	}

	var triggerJob *github.WorkflowJob
	for i := range jobs {
		if jobs[i].Name == repo.TriggerJob {
			triggerJob = &jobs[i]
			break
		}
	}

	if triggerJob == nil {
		switch msg.RunConclusion {
		case "cancelled":
			_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, existingOpenID,
				"cancelled", "Cancelled", "CI workflow was cancelled before building the Android test package.")
			return err
		case "failure", "timed_out":
			_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, existingOpenID,
				"failure", "CI failed", "CI failed before building the Android test package.")
			return err
		default:
			_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, existingOpenID,
				"success", "Not needed", "Not needed: the plan left the Android tests out")
			return err
		}
	}

	switch triggerJob.Conclusion {
	case "skipped":
		_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, existingOpenID,
			"success", "Not needed", "Not needed: the plan left the Android tests out")
		return err
	case "cancelled":
		_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, existingOpenID,
			"cancelled", "Cancelled", "CI couldn't build the Android test package.")
		return err
	case "success":
		// Safety net recovery if the workflow_job completed webhook was dropped.
		// scheduleDeviceFarmRun atomically claims (repo, runID, attempt), so if
		// workflow_job already scheduled it, this becomes a no-op.
		return h.scheduleDeviceFarmRun(ctx, repo, msg, false)
	default:
		_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, extID, existingOpenID,
			"failure", "Build failed", "CI couldn't build the Android test package.")
		return err
	}
}

func (h *Handler) handleCheckRunRerequested(ctx context.Context, repo config.RepoConfig, msg model.JobMessage) error {
	records, err := h.store.QueryByRepoSHA(ctx, repo.FullName, msg.HeadSHA)
	if err != nil {
		return err
	}

	var matched *model.RunRecord
	for i := range records {
		if records[i].WorkflowRunID == 0 {
			continue
		}
		if msg.CheckRunID != 0 && records[i].CheckRunID == msg.CheckRunID {
			matched = &records[i]
			break
		}
		if matched == nil {
			matched = &records[i]
		}
	}

	var workflowRunID int64
	var runAttempt int
	if matched != nil {
		workflowRunID = matched.WorkflowRunID
		runAttempt = matched.WorkflowRunAttempt
		if msg.HeadBranch == "" {
			msg.HeadBranch = matched.HeadBranch
		}
		if msg.HeadRepo == "" {
			msg.HeadRepo = matched.HeadRepo
		}
	} else if parsedID, parsedAttempt, ok := model.ParseExternalID(msg.ExternalID); ok {
		workflowRunID = parsedID
		runAttempt = parsedAttempt
	}

	if workflowRunID == 0 {
		_, err := h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, "", 0,
			"failure", "Unable to re-run",
			"No CI workflow run record was found for this commit (records expire after 30 days). Re-run the CI workflow to rebuild the Android test package.")
		return err
	}

	msg.WorkflowRunID = workflowRunID
	msg.WorkflowRunAttempt = runAttempt
	return h.scheduleDeviceFarmRun(ctx, repo, msg, true)
}

func (h *Handler) scheduleDeviceFarmRun(ctx context.Context, repo config.RepoConfig, msg model.JobMessage, isRerun bool) error {
	// Fetch authoritative workflow_run metadata first to resolve head_repository,
	// run_attempt, and created_at before claiming or superseding anything.
	wfRun, err := h.gh.GetWorkflowRun(ctx, msg.InstallationID, repo.Owner, repo.Name, msg.WorkflowRunID)
	if err != nil {
		return fmt.Errorf("get workflow run %d: %w", msg.WorkflowRunID, err)
	}

	runAttempt := msg.WorkflowRunAttempt
	if runAttempt <= 0 {
		runAttempt = wfRun.RunAttempt
	}
	runAttempt = normalizeAttempt(runAttempt)

	headBranch := msg.HeadBranch
	if wfRun.HeadBranch != "" {
		headBranch = wfRun.HeadBranch
	}
	if !h.cfg.BranchAllowed(headBranch) {
		return nil
	}

	headSHA := msg.HeadSHA
	if headSHA == "" {
		headSHA = wfRun.HeadSHA
	}
	headRepo := wfRun.HeadRepository.FullName
	if headRepo == "" {
		headRepo = msg.HeadRepo
	}
	if headRepo == "" {
		headRepo = repo.FullName
	}
	workflowCreatedAt := wfRun.CreatedAt.UTC()
	extID := model.ExternalID(msg.WorkflowRunID, runAttempt)

	// Atomically claim (repo, workflowRunID, runAttempt) before creating checks,
	// superseding runs, or uploading packages so concurrent/duplicate SQS deliveries
	// cannot double-schedule the same workflow attempt.
	claimKey := model.ClaimKey(repo.FullName, msg.WorkflowRunID, runAttempt)
	claimed := false
	if !isRerun {
		if err := h.store.ClaimWorkflowRun(ctx, claimKey, h.now().UTC()); err != nil {
			if errors.Is(err, store.ErrAlreadyClaimed) {
				h.logger.Info("skipping duplicate schedule; workflow attempt already claimed",
					"repo", repo.FullName, "workflow_run_id", msg.WorkflowRunID, "run_attempt", runAttempt)
				return nil
			}
			return err
		}
		claimed = true
	}

	releaseClaim := func() {
		if !claimed {
			return
		}
		if delErr := h.store.DeleteRun(ctx, claimKey); delErr != nil {
			h.logger.Warn("failed to release scheduling claim after transient error", "claim_key", claimKey, "err", delErr)
		}
		claimed = false
	}

	// Check active runs on the same branch before creating checks or stopping anything.
	branchKey := model.BranchKey(repo.FullName, headRepo, headBranch)
	var activeOnBranch []model.RunRecord
	if branchKey != "" {
		activeOnBranch, err = h.store.QueryActiveByBranchKey(ctx, branchKey)
		if err != nil {
			releaseClaim()
			return err
		}
	}

	// If a newer workflow run is already ACTIVE on this branch, this incoming workflow
	// is stale: do NOT supersede the newer run; cancel this stale commit's check instead.
	if !isRerun {
		if newer := findNewerActiveRun(activeOnBranch, workflowCreatedAt, msg.WorkflowRunID); newer != nil {
			_, err := h.completeCheckRun(ctx, repo, msg.InstallationID, headSHA, extID, 0,
				"cancelled", "Superseded",
				fmt.Sprintf("Superseded by newer commit %s", shortSHA(newer.SHA)))
			if err != nil {
				releaseClaim()
				return err
			}
			h.logger.Info("skipped scheduling stale workflow run",
				"repo", repo.FullName, "sha", headSHA, "newer_run_arn", newer.RunARN)
			return nil
		}
	}

	// On SQS retry after a transient failure, reuse any open Check Run for this attempt.
	var existingCheckRunID int64
	if !isRerun {
		_, existingCheckRunID, err = h.findCheckRunForExternalID(ctx, repo, msg.InstallationID, headSHA, extID)
		if err != nil {
			releaseClaim()
			return err
		}
	}

	checkRunID, err := h.upsertProgressCheckRun(ctx, repo, msg.InstallationID, headSHA, extID, existingCheckRunID,
		"in_progress", "", "Scheduling", "Scheduling on AWS Device Farm...")
	if err != nil {
		releaseClaim()
		return err
	}

	if err := h.supersedeActiveRuns(ctx, repo, activeOnBranch, headSHA, workflowCreatedAt, msg.WorkflowRunID, isRerun); err != nil {
		releaseClaim()
		return err
	}

	artifacts, err := h.gh.ListWorkflowRunArtifacts(ctx, msg.InstallationID, repo.Owner, repo.Name, msg.WorkflowRunID, repo.ArtifactName)
	if err != nil {
		releaseClaim()
		return err
	}

	var chosen *github.Artifact
	for i := range artifacts {
		a := &artifacts[i]
		if a.Name != repo.ArtifactName || a.Expired {
			continue
		}
		if chosen == nil || a.CreatedAt.After(chosen.CreatedAt) {
			chosen = a
		}
	}
	if chosen == nil {
		_, err := h.completeCheckRun(ctx, repo, msg.InstallationID, headSHA, extID, checkRunID,
			"failure", "Artifact expired",
			fmt.Sprintf("The CI artifact `%s` for this commit is missing or expired. Re-run the CI workflow to rebuild the Android test package.", repo.ArtifactName))
		if err != nil {
			releaseClaim()
			return err
		}
		return nil
	}

	tmpDir, err := os.MkdirTemp("", "device-farm-worker-*")
	if err != nil {
		releaseClaim()
		return err
	}
	defer os.RemoveAll(tmpDir)

	outerZipPath := filepath.Join(tmpDir, "artifact.zip")
	if err := h.gh.DownloadArtifact(ctx, msg.InstallationID, repo.Owner, repo.Name, chosen.ID, outerZipPath, maxArtifactZipBytes); err != nil {
		if errors.Is(err, github.ErrArtifactExpired) {
			_, checkErr := h.completeCheckRun(ctx, repo, msg.InstallationID, headSHA, extID, checkRunID,
				"failure", "Artifact expired",
				fmt.Sprintf("The CI artifact `%s` for this commit has expired. Re-run the CI workflow to rebuild the Android test package.", repo.ArtifactName))
			if checkErr != nil {
				releaseClaim()
				return checkErr
			}
			return nil
		}
		releaseClaim()
		return err
	}

	// Extract only the inner package zip without unpacking or executing anything inside it.
	pkgPath := filepath.Join(tmpDir, repo.PackageFilename)
	if err := report.ExtractPackageFromArtifactZip(outerZipPath, repo.PackageFilename, pkgPath, maxArtifactZipBytes); err != nil {
		if errors.Is(err, report.ErrPackageNotFound) {
			_, checkErr := h.completeCheckRun(ctx, repo, msg.InstallationID, headSHA, extID, checkRunID,
				"failure", "Invalid artifact",
				fmt.Sprintf("The CI artifact `%s` did not contain `%s`.", repo.ArtifactName, repo.PackageFilename))
			if checkErr != nil {
				releaseClaim()
				return checkErr
			}
			return nil
		}
		releaseClaim()
		return err
	}
	// Free outer zip space in /tmp before uploading.
	_ = os.Remove(outerZipPath)

	// Always fetch the Device Farm test spec from the default branch (main), NEVER from PR branch.
	specBytes, err := h.gh.GetRawFile(ctx, msg.InstallationID, repo.Owner, repo.Name, repo.TestSpecPath, repo.DefaultBranch)
	if err != nil {
		releaseClaim()
		return fmt.Errorf("fetch test spec %s@%s: %w", repo.TestSpecPath, repo.DefaultBranch, err)
	}
	specFilename := path.Base(repo.TestSpecPath)
	specPath := filepath.Join(tmpDir, specFilename)
	if err := os.WriteFile(specPath, specBytes, 0o600); err != nil {
		releaseClaim()
		return err
	}

	// Upload test spec and test package concurrently.
	var (
		specARN, pkgARN string
		specErr, pkgErr error
		wg              sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		specARN, specErr = h.df.UploadFile(ctx, repo.ProjectARN, specFilename, dftypes.UploadTypeAppiumWebNodeTestSpec, specPath)
	}()
	go func() {
		defer wg.Done()
		pkgARN, pkgErr = h.df.UploadFile(ctx, repo.ProjectARN, repo.PackageFilename, dftypes.UploadTypeAppiumWebNodeTestPackage, pkgPath)
	}()
	wg.Wait()

	if uploadErr := firstError(specErr, pkgErr); uploadErr != nil {
		var rejected *devicefarm.UploadRejectedError
		if errors.As(uploadErr, &rejected) {
			summary := rejected.Error()
			_, checkErr := h.completeCheckRun(ctx, repo, msg.InstallationID, headSHA, extID, checkRunID,
				"failure", "Device Farm rejected upload", summary)
			if checkErr != nil {
				releaseClaim()
				return checkErr
			}
			return nil
		}
		releaseClaim()
		return uploadErr
	}

	runName := fmt.Sprintf("%s %s %s", repo.Name, shortSHA(headSHA), headBranch)
	runARN, err := h.df.ScheduleRun(ctx, devicefarm.ScheduleInput{
		ProjectARN:        repo.ProjectARN,
		RunName:           runName,
		Devices:           repo.Devices,
		TestPackageARN:    pkgARN,
		TestSpecARN:       specARN,
		JobTimeoutMinutes: repo.JobTimeoutMinutes,
	})
	if err != nil {
		releaseClaim()
		return err
	}

	now := h.now().UTC()
	wfCreatedStr := ""
	if !workflowCreatedAt.IsZero() {
		wfCreatedStr = workflowCreatedAt.Format(time.RFC3339)
	}
	rec := model.RunRecord{
		RunARN:             runARN,
		State:              model.StateActive,
		CreatedAt:          now.Format(time.RFC3339Nano),
		BranchKey:          branchKey,
		RepoSHA:            model.RepoSHAKey(repo.FullName, headSHA),
		Repo:               repo.FullName,
		SHA:                headSHA,
		HeadBranch:         headBranch,
		HeadRepo:           headRepo,
		CheckRunID:         checkRunID,
		WorkflowRunID:      msg.WorkflowRunID,
		WorkflowRunAttempt: runAttempt,
		WorkflowCreatedAt:  wfCreatedStr,
		InstallationID:     msg.InstallationID,
		ExpiresAt:          now.Add(store.RecordTTL).Unix(),
	}
	if err := h.store.PutRun(ctx, rec); err != nil {
		// Stop orphaned Device Farm run and release claim so SQS can cleanly retry.
		if stopErr := h.df.StopRun(ctx, runARN); stopErr != nil {
			h.logger.Warn("failed to stop orphaned Device Farm run after PutRun failure", "run_arn", runARN, "err", stopErr)
		}
		releaseClaim()
		return err
	}

	// Keep the claim permanently now that the run is scheduled and persisted.
	claimed = false

	consoleURL := devicefarm.ConsoleURL(runARN)
	summary := "Running Android tests on AWS Device Farm."
	if consoleURL != "" {
		summary += fmt.Sprintf("\n\n[View run in AWS Device Farm console](%s)", consoleURL)
	}
	if _, err := h.upsertProgressCheckRun(ctx, repo, msg.InstallationID, headSHA, extID, checkRunID,
		"in_progress", consoleURL, "Running on AWS Device Farm", summary); err != nil {
		h.logger.Warn("failed to update check run with console URL after scheduling", "run_arn", runARN, "err", err)
	}

	h.logger.Info("scheduled device farm run",
		"repo", repo.FullName, "sha", headSHA, "run_arn", runARN, "check_run_id", checkRunID, "run_attempt", runAttempt)
	return nil
}

func (h *Handler) supersedeActiveRuns(ctx context.Context, repo config.RepoConfig, active []model.RunRecord, newSHA string, incomingCreatedAt time.Time, incomingRunID int64, isRerun bool) error {
	now := h.now().UTC()
	for _, earlier := range active {
		if !model.IsDeviceFarmARN(earlier.RunARN) {
			continue
		}
		if !isRerun && isNewerWorkflowRun(earlier, incomingCreatedAt, incomingRunID) {
			continue
		}
		if err := h.store.MarkSuperseded(ctx, earlier.RunARN, now); err != nil {
			if errors.Is(err, store.ErrStateConflict) {
				continue
			}
			return err
		}
		if err := h.df.StopRun(ctx, earlier.RunARN); err != nil {
			h.logger.Warn("failed to stop superseded Device Farm run", "run_arn", earlier.RunARN, "err", err)
		}
		if earlier.CheckRunID > 0 && earlier.InstallationID > 0 {
			summary := fmt.Sprintf("Superseded by newer commit %s", shortSHA(newSHA))
			if earlier.SHA == newSHA {
				summary = "Superseded by newer run"
			}
			_, err := h.gh.UpdateCheckRun(ctx, earlier.InstallationID, repo.Owner, repo.Name, earlier.CheckRunID, github.UpdateCheckRunRequest{
				Status:      "completed",
				Conclusion:  "cancelled",
				CompletedAt: now.Format(time.RFC3339),
				DetailsURL:  devicefarm.ConsoleURL(earlier.RunARN),
				Output: &github.CheckRunOutput{
					Title:   "Superseded",
					Summary: summary,
				},
			})
			if err != nil {
				h.logger.Warn("failed to mark superseded check run cancelled", "check_run_id", earlier.CheckRunID, "err", err)
			}
		}
	}
	return nil
}

// findNewerActiveRun returns the newest ACTIVE run on the branch that is strictly
// newer than (incomingCreatedAt, incomingRunID), or nil if none is newer.
func findNewerActiveRun(active []model.RunRecord, incomingCreatedAt time.Time, incomingRunID int64) *model.RunRecord {
	var newest *model.RunRecord
	for i := range active {
		r := &active[i]
		if !model.IsDeviceFarmARN(r.RunARN) {
			continue
		}
		if !isNewerWorkflowRun(*r, incomingCreatedAt, incomingRunID) {
			continue
		}
		if newest == nil {
			newest = r
			continue
		}
		// Pick the newest among newer runs for the cancellation message.
		nTime, _ := time.Parse(time.RFC3339Nano, newest.WorkflowCreatedAt)
		if isNewerWorkflowRun(*r, nTime, newest.WorkflowRunID) {
			newest = r
		}
	}
	return newest
}

// isNewerWorkflowRun reports whether existing represents a strictly newer workflow
// run than (incomingCreatedAt, incomingRunID).
func isNewerWorkflowRun(existing model.RunRecord, incomingCreatedAt time.Time, incomingRunID int64) bool {
	if existing.WorkflowCreatedAt != "" && !incomingCreatedAt.IsZero() {
		if existingTime, err := time.Parse(time.RFC3339Nano, existing.WorkflowCreatedAt); err == nil {
			if existingTime.After(incomingCreatedAt) {
				return true
			}
			if existingTime.Before(incomingCreatedAt) {
				return false
			}
			return existing.WorkflowRunID > incomingRunID
		}
	}
	if existing.WorkflowRunID > 0 && incomingRunID > 0 {
		return existing.WorkflowRunID > incomingRunID
	}
	return false
}

func (h *Handler) hasRunForAttempt(ctx context.Context, repoFullName, sha string, workflowRunID int64, runAttempt int) (bool, error) {
	if workflowRunID == 0 || sha == "" {
		return false, nil
	}
	records, err := h.store.QueryByRepoSHA(ctx, repoFullName, sha)
	if err != nil {
		return false, err
	}
	wantAttempt := normalizeAttempt(runAttempt)
	for _, r := range records {
		if !model.IsDeviceFarmARN(r.RunARN) {
			continue
		}
		if r.WorkflowRunID != workflowRunID || normalizeAttempt(r.WorkflowRunAttempt) != wantAttempt {
			continue
		}
		if r.State == model.StateActive || r.State == model.StateCompleted {
			return true, nil
		}
	}
	return false, nil
}

// findCheckRunForExternalID checks whether a Check Run for repo.CheckName with
// matching external_id is already completed, or returns the ID of an open one.
func (h *Handler) findCheckRunForExternalID(ctx context.Context, repo config.RepoConfig, installationID int64, sha, extID string) (completed bool, openID int64, err error) {
	if extID == "" || sha == "" {
		return false, 0, nil
	}
	runs, err := h.gh.ListCheckRunsForRef(ctx, installationID, repo.Owner, repo.Name, sha, repo.CheckName)
	if err != nil {
		return false, 0, err
	}
	for _, cr := range runs {
		if cr.Name != repo.CheckName || cr.ExternalID != extID {
			continue
		}
		if cr.Status == "completed" {
			return true, 0, nil
		}
		if openID == 0 {
			openID = cr.ID
		}
	}
	return false, openID, nil
}

func (h *Handler) upsertProgressCheckRun(ctx context.Context, repo config.RepoConfig, installationID int64, headSHA, extID string, existingID int64, status, detailsURL, title, summary string) (int64, error) {
	out := &github.CheckRunOutput{Title: title, Summary: summary}
	if existingID > 0 {
		cr, err := h.gh.UpdateCheckRun(ctx, installationID, repo.Owner, repo.Name, existingID, github.UpdateCheckRunRequest{
			Status:     status,
			DetailsURL: detailsURL,
			ExternalID: extID,
			Output:     out,
		})
		if err != nil {
			return 0, err
		}
		return cr.ID, nil
	}
	cr, err := h.gh.CreateCheckRun(ctx, installationID, repo.Owner, repo.Name, github.CreateCheckRunRequest{
		Name:       repo.CheckName,
		HeadSHA:    headSHA,
		Status:     status,
		StartedAt:  h.now().UTC().Format(time.RFC3339),
		DetailsURL: detailsURL,
		ExternalID: extID,
		Output:     out,
	})
	if err != nil {
		return 0, err
	}
	return cr.ID, nil
}

func (h *Handler) completeCheckRun(ctx context.Context, repo config.RepoConfig, installationID int64, headSHA, extID string, existingID int64, conclusion, title, summary string) (int64, error) {
	now := h.now().UTC().Format(time.RFC3339)
	out := &github.CheckRunOutput{Title: title, Summary: summary}
	if existingID > 0 {
		cr, err := h.gh.UpdateCheckRun(ctx, installationID, repo.Owner, repo.Name, existingID, github.UpdateCheckRunRequest{
			Status:      "completed",
			Conclusion:  conclusion,
			CompletedAt: now,
			ExternalID:  extID,
			Output:      out,
		})
		if err != nil {
			return 0, err
		}
		return cr.ID, nil
	}
	cr, err := h.gh.CreateCheckRun(ctx, installationID, repo.Owner, repo.Name, github.CreateCheckRunRequest{
		Name:        repo.CheckName,
		HeadSHA:     headSHA,
		Status:      "completed",
		StartedAt:   now,
		CompletedAt: now,
		Conclusion:  conclusion,
		ExternalID:  extID,
		Output:      out,
	})
	if err != nil {
		return 0, err
	}
	return cr.ID, nil
}

func normalizeAttempt(attempt int) int {
	if attempt <= 0 {
		return 1
	}
	return attempt
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
