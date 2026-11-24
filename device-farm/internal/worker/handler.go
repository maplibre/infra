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
	"strconv"
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
	PutRun(ctx context.Context, rec model.RunRecord) error
	GetRun(ctx context.Context, runARN string) (model.RunRecord, bool, error)
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
	if msg.HeadBranch != "" && !h.cfg.BranchAllowed(msg.HeadBranch) {
		return nil
	}

	pendingKey := model.PendingRunKey(repo.FullName, msg.WorkflowRunID)
	existing, hasPending, err := h.store.GetRun(ctx, pendingKey)
	if err != nil {
		return err
	}

	switch msg.Action {
	case "queued", "in_progress":
		// Ignore stale queued/in_progress messages if this workflow run has already moved on.
		if hasPending && existing.State != model.StatePending {
			return nil
		}
		if hasPending && existing.CheckStatus == "in_progress" && msg.Action == "queued" {
			return nil
		}
		alreadyScheduled, err := h.hasScheduledRun(ctx, repo.FullName, msg.HeadSHA, msg.WorkflowRunID)
		if err != nil {
			return err
		}
		if alreadyScheduled {
			return nil
		}

		title := "Queued"
		summary := "Waiting for CI to build the Android test package."
		if msg.Action == "in_progress" {
			title = "Building test package"
			summary = "CI is building the Android test package."
		}

		checkRunID, err := h.upsertProgressCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, existing.CheckRunID, msg.Action, "", title, summary)
		if err != nil {
			return err
		}

		now := h.now().UTC()
		createdAt := existing.CreatedAt
		if createdAt == "" {
			createdAt = now.Format(time.RFC3339Nano)
		}
		return h.store.PutRun(ctx, model.RunRecord{
			RunARN:         pendingKey,
			State:          model.StatePending,
			CreatedAt:      createdAt,
			BranchKey:      model.BranchKey(repo.FullName, msg.HeadRepo, msg.HeadBranch),
			RepoSHA:        model.RepoSHAKey(repo.FullName, msg.HeadSHA),
			Repo:           repo.FullName,
			SHA:            msg.HeadSHA,
			HeadBranch:     msg.HeadBranch,
			HeadRepo:       msg.HeadRepo,
			CheckRunID:     checkRunID,
			CheckStatus:    msg.Action,
			WorkflowRunID:  msg.WorkflowRunID,
			InstallationID: msg.InstallationID,
			ExpiresAt:      now.Add(store.RecordTTL).Unix(),
		})

	case "completed":
		checkRunID := existing.CheckRunID
		if checkRunID == 0 {
			checkRunID, err = h.findOpenCheckRunID(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID)
			if err != nil {
				return err
			}
		}

		switch msg.JobConclusion {
		case "skipped":
			checkRunID, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, checkRunID,
				"success", "Not needed", "Not needed: the plan left the Android tests out")
			if err != nil {
				return err
			}
			return h.saveTerminalPending(ctx, repo, msg, checkRunID, model.StateSkipped, "SKIPPED")

		case "cancelled":
			checkRunID, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, checkRunID,
				"cancelled", "Cancelled", "CI couldn't build the Android test package.")
			if err != nil {
				return err
			}
			return h.saveTerminalPending(ctx, repo, msg, checkRunID, model.StateBuildFailed, "CANCELLED")

		case "success":
			return h.scheduleDeviceFarmRun(ctx, repo, msg, checkRunID, false)

		default:
			// failure, timed_out, action_required, stale, etc.
			checkRunID, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, checkRunID,
				"failure", "Build failed", "CI couldn't build the Android test package.")
			if err != nil {
				return err
			}
			return h.saveTerminalPending(ctx, repo, msg, checkRunID, model.StateBuildFailed, msg.JobConclusion)
		}
	}

	return nil
}

func (h *Handler) handleWorkflowRun(ctx context.Context, repo config.RepoConfig, msg model.JobMessage) error {
	if msg.HeadBranch != "" && !h.cfg.BranchAllowed(msg.HeadBranch) {
		return nil
	}

	checkRuns, err := h.gh.ListCheckRunsForRef(ctx, msg.InstallationID, repo.Owner, repo.Name, msg.HeadSHA, repo.CheckName)
	if err != nil {
		return err
	}

	var existingOpenID int64
	for _, cr := range checkRuns {
		if cr.Name != repo.CheckName {
			continue
		}
		if cr.Status == "completed" {
			return nil
		}
		if existingOpenID == 0 {
			existingOpenID = cr.ID
		}
	}

	// If Device Farm is already running (or finished) for this workflow run, do nothing.
	alreadyScheduled, err := h.hasScheduledRun(ctx, repo.FullName, msg.HeadSHA, msg.WorkflowRunID)
	if err != nil {
		return err
	}
	if alreadyScheduled {
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
			_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, existingOpenID,
				"cancelled", "Cancelled", "CI workflow was cancelled before building the Android test package.")
			return err
		case "failure", "timed_out":
			_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, existingOpenID,
				"failure", "CI failed", "CI failed before building the Android test package.")
			return err
		default:
			_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, existingOpenID,
				"success", "Not needed", "Not needed: the plan left the Android tests out")
			return err
		}
	}

	switch triggerJob.Conclusion {
	case "skipped":
		_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, existingOpenID,
			"success", "Not needed", "Not needed: the plan left the Android tests out")
		return err
	case "cancelled":
		_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, existingOpenID,
			"cancelled", "Cancelled", "CI couldn't build the Android test package.")
		return err
	case "success":
		// Safety net recovery if the workflow_job completed webhook was dropped.
		return h.scheduleDeviceFarmRun(ctx, repo, msg, existingOpenID, false)
	default:
		_, err = h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, msg.WorkflowRunID, existingOpenID,
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
	if matched != nil {
		workflowRunID = matched.WorkflowRunID
		if msg.HeadBranch == "" {
			msg.HeadBranch = matched.HeadBranch
		}
		if msg.HeadRepo == "" {
			msg.HeadRepo = matched.HeadRepo
		}
	} else if msg.ExternalID != "" {
		if parsed, err := strconv.ParseInt(msg.ExternalID, 10, 64); err == nil && parsed > 0 {
			workflowRunID = parsed
		}
	}

	if workflowRunID == 0 {
		_, err := h.completeCheckRun(ctx, repo, msg.InstallationID, msg.HeadSHA, 0, 0,
			"failure", "Unable to re-run",
			"No CI workflow run record was found for this commit (records expire after 30 days). Re-run the CI workflow to rebuild the Android test package.")
		return err
	}

	msg.WorkflowRunID = workflowRunID
	// Create a new Check Run for the re-run so previous run history is preserved.
	return h.scheduleDeviceFarmRun(ctx, repo, msg, 0, true)
}

func (h *Handler) scheduleDeviceFarmRun(ctx context.Context, repo config.RepoConfig, msg model.JobMessage, existingCheckRunID int64, isRerun bool) error {
	if !isRerun {
		alreadyScheduled, err := h.hasScheduledRun(ctx, repo.FullName, msg.HeadSHA, msg.WorkflowRunID)
		if err != nil {
			return err
		}
		if alreadyScheduled {
			h.logger.Info("skipping duplicate schedule for workflow run", "repo", repo.FullName, "workflow_run_id", msg.WorkflowRunID)
			return nil
		}
	}

	// Fetch authoritative workflow_run metadata to learn head_repository (critical for fork PRs).
	wfRun, err := h.gh.GetWorkflowRun(ctx, msg.InstallationID, repo.Owner, repo.Name, msg.WorkflowRunID)
	if err != nil {
		return fmt.Errorf("get workflow run %d: %w", msg.WorkflowRunID, err)
	}

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

	checkRunID, err := h.upsertProgressCheckRun(ctx, repo, msg.InstallationID, headSHA, msg.WorkflowRunID, existingCheckRunID,
		"in_progress", "", "Scheduling", "Scheduling on AWS Device Farm...")
	if err != nil {
		return err
	}

	branchKey := model.BranchKey(repo.FullName, headRepo, headBranch)
	if err := h.supersedeActiveRuns(ctx, repo, branchKey, headSHA); err != nil {
		return err
	}

	artifacts, err := h.gh.ListWorkflowRunArtifacts(ctx, msg.InstallationID, repo.Owner, repo.Name, msg.WorkflowRunID, repo.ArtifactName)
	if err != nil {
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
		_, err := h.completeCheckRun(ctx, repo, msg.InstallationID, headSHA, msg.WorkflowRunID, checkRunID,
			"failure", "Artifact expired",
			fmt.Sprintf("The CI artifact `%s` for this commit is missing or expired. Re-run the CI workflow to rebuild the Android test package.", repo.ArtifactName))
		return err
	}

	tmpDir, err := os.MkdirTemp("", "device-farm-worker-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	outerZipPath := filepath.Join(tmpDir, "artifact.zip")
	if err := h.gh.DownloadArtifact(ctx, msg.InstallationID, repo.Owner, repo.Name, chosen.ID, outerZipPath, maxArtifactZipBytes); err != nil {
		if errors.Is(err, github.ErrArtifactExpired) {
			_, checkErr := h.completeCheckRun(ctx, repo, msg.InstallationID, headSHA, msg.WorkflowRunID, checkRunID,
				"failure", "Artifact expired",
				fmt.Sprintf("The CI artifact `%s` for this commit has expired. Re-run the CI workflow to rebuild the Android test package.", repo.ArtifactName))
			return checkErr
		}
		return err
	}

	// Extract only the inner package zip without unpacking or executing anything inside it.
	pkgPath := filepath.Join(tmpDir, repo.PackageFilename)
	if err := report.ExtractPackageFromArtifactZip(outerZipPath, repo.PackageFilename, pkgPath, maxArtifactZipBytes); err != nil {
		if errors.Is(err, report.ErrPackageNotFound) {
			_, checkErr := h.completeCheckRun(ctx, repo, msg.InstallationID, headSHA, msg.WorkflowRunID, checkRunID,
				"failure", "Invalid artifact",
				fmt.Sprintf("The CI artifact `%s` did not contain `%s`.", repo.ArtifactName, repo.PackageFilename))
			return checkErr
		}
		return err
	}
	// Free outer zip space in /tmp before uploading.
	_ = os.Remove(outerZipPath)

	// Always fetch the Device Farm test spec from the default branch (main), NEVER from PR branch.
	specBytes, err := h.gh.GetRawFile(ctx, msg.InstallationID, repo.Owner, repo.Name, repo.TestSpecPath, repo.DefaultBranch)
	if err != nil {
		return fmt.Errorf("fetch test spec %s@%s: %w", repo.TestSpecPath, repo.DefaultBranch, err)
	}
	specFilename := path.Base(repo.TestSpecPath)
	specPath := filepath.Join(tmpDir, specFilename)
	if err := os.WriteFile(specPath, specBytes, 0o600); err != nil {
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
	if specErr != nil {
		return specErr
	}
	if pkgErr != nil {
		return pkgErr
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
		return err
	}

	now := h.now().UTC()
	rec := model.RunRecord{
		RunARN:         runARN,
		State:          model.StateActive,
		CreatedAt:      now.Format(time.RFC3339Nano),
		BranchKey:      branchKey,
		RepoSHA:        model.RepoSHAKey(repo.FullName, headSHA),
		Repo:           repo.FullName,
		SHA:            headSHA,
		HeadBranch:     headBranch,
		HeadRepo:       headRepo,
		CheckRunID:     checkRunID,
		WorkflowRunID:  msg.WorkflowRunID,
		InstallationID: msg.InstallationID,
		ExpiresAt:      now.Add(store.RecordTTL).Unix(),
	}
	if err := h.store.PutRun(ctx, rec); err != nil {
		return err
	}

	// Clean up temporary pending tracking item once the real Device Farm run record is persisted.
	if err := h.store.DeleteRun(ctx, model.PendingRunKey(repo.FullName, msg.WorkflowRunID)); err != nil {
		h.logger.Warn("failed to delete pending run tracking record", "err", err)
	}

	consoleURL := devicefarm.ConsoleURL(runARN)
	summary := "Running Android tests on AWS Device Farm."
	if consoleURL != "" {
		summary += fmt.Sprintf("\n\n[View run in AWS Device Farm console](%s)", consoleURL)
	}
	if _, err := h.upsertProgressCheckRun(ctx, repo, msg.InstallationID, headSHA, msg.WorkflowRunID, checkRunID,
		"in_progress", consoleURL, "Running on AWS Device Farm", summary); err != nil {
		h.logger.Warn("failed to update check run with console URL after scheduling", "run_arn", runARN, "err", err)
	}

	h.logger.Info("scheduled device farm run", "repo", repo.FullName, "sha", headSHA, "run_arn", runARN, "check_run_id", checkRunID)
	return nil
}

func (h *Handler) supersedeActiveRuns(ctx context.Context, repo config.RepoConfig, branchKey, newSHA string) error {
	if branchKey == "" {
		return nil
	}
	active, err := h.store.QueryActiveByBranchKey(ctx, branchKey)
	if err != nil {
		return err
	}
	now := h.now().UTC()
	for _, earlier := range active {
		if !model.IsDeviceFarmARN(earlier.RunARN) {
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
			_, err := h.gh.UpdateCheckRun(ctx, earlier.InstallationID, repo.Owner, repo.Name, earlier.CheckRunID, github.UpdateCheckRunRequest{
				Status:      "completed",
				Conclusion:  "cancelled",
				CompletedAt: now.Format(time.RFC3339),
				DetailsURL:  devicefarm.ConsoleURL(earlier.RunARN),
				Output: &github.CheckRunOutput{
					Title:   "Superseded",
					Summary: fmt.Sprintf("Superseded by newer commit %s", shortSHA(newSHA)),
				},
			})
			if err != nil {
				h.logger.Warn("failed to mark superseded check run cancelled", "check_run_id", earlier.CheckRunID, "err", err)
			}
		}
	}
	return nil
}

func (h *Handler) hasScheduledRun(ctx context.Context, repoFullName, sha string, workflowRunID int64) (bool, error) {
	if workflowRunID == 0 || sha == "" {
		return false, nil
	}
	records, err := h.store.QueryByRepoSHA(ctx, repoFullName, sha)
	if err != nil {
		return false, err
	}
	for _, r := range records {
		if r.WorkflowRunID == workflowRunID && model.IsDeviceFarmARN(r.RunARN) &&
			(r.State == model.StateActive || r.State == model.StateCompleted) {
			return true, nil
		}
	}
	return false, nil
}

func (h *Handler) findOpenCheckRunID(ctx context.Context, repo config.RepoConfig, installationID int64, sha string, workflowRunID int64) (int64, error) {
	runs, err := h.gh.ListCheckRunsForRef(ctx, installationID, repo.Owner, repo.Name, sha, repo.CheckName)
	if err != nil {
		return 0, err
	}
	extID := strconv.FormatInt(workflowRunID, 10)
	var fallback int64
	for _, cr := range runs {
		if cr.Name != repo.CheckName || cr.Status == "completed" {
			continue
		}
		if cr.ExternalID == extID {
			return cr.ID, nil
		}
		if fallback == 0 {
			fallback = cr.ID
		}
	}
	return fallback, nil
}

func (h *Handler) upsertProgressCheckRun(ctx context.Context, repo config.RepoConfig, installationID int64, headSHA string, workflowRunID, existingID int64, status, detailsURL, title, summary string) (int64, error) {
	extID := ""
	if workflowRunID > 0 {
		extID = strconv.FormatInt(workflowRunID, 10)
	}
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

func (h *Handler) completeCheckRun(ctx context.Context, repo config.RepoConfig, installationID int64, headSHA string, workflowRunID, existingID int64, conclusion, title, summary string) (int64, error) {
	now := h.now().UTC().Format(time.RFC3339)
	extID := ""
	if workflowRunID > 0 {
		extID = strconv.FormatInt(workflowRunID, 10)
	}
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

func (h *Handler) saveTerminalPending(ctx context.Context, repo config.RepoConfig, msg model.JobMessage, checkRunID int64, state model.RunState, result string) error {
	now := h.now().UTC()
	return h.store.PutRun(ctx, model.RunRecord{
		RunARN:         model.PendingRunKey(repo.FullName, msg.WorkflowRunID),
		State:          state,
		CreatedAt:      now.Format(time.RFC3339Nano),
		CompletedAt:    now.Format(time.RFC3339Nano),
		BranchKey:      model.BranchKey(repo.FullName, msg.HeadRepo, msg.HeadBranch),
		RepoSHA:        model.RepoSHAKey(repo.FullName, msg.HeadSHA),
		Repo:           repo.FullName,
		SHA:            msg.HeadSHA,
		HeadBranch:     msg.HeadBranch,
		HeadRepo:       msg.HeadRepo,
		CheckRunID:     checkRunID,
		WorkflowRunID:  msg.WorkflowRunID,
		InstallationID: msg.InstallationID,
		Result:         result,
		ExpiresAt:      now.Add(store.RecordTTL).Unix(),
	})
}

func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
