import { createReadStream } from "node:fs";
import { mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { Readable } from "node:stream";
import {
  CreateUploadCommand,
  type DeviceFarmClient,
  GetUploadCommand,
  NotFoundException,
  ScheduleRunCommand,
  StopRunCommand,
  type UploadType,
} from "@aws-sdk/client-device-farm";
import type { SQSBatchResponse, SQSEvent } from "aws-lambda";
import type { Octokit } from "octokit";
import {
  branchAllowed,
  branchKey,
  type Config,
  fetchRepoConfig,
  formatExternalId,
  InvalidRepoConfigError,
  type JobMessage,
  lookupProjectArn,
  parseExternalId,
  repoShaKey,
} from "./config.js";
import {
  consoleUrl,
  DownloadSizeLimitError,
  downloadStreamToFile,
  extractPackageFromZip,
  InvalidArchiveError,
  PackageNotFoundError,
} from "./report.js";
import { RECORD_TTL_SEC, type RunStore } from "./store.js";

const MAX_ARTIFACT_ZIP_BYTES = 2 * 1024 * 1024 * 1024;

class PermanentFailure extends Error {
  constructor(
    readonly title: string,
    readonly summary: string,
  ) {
    super(summary);
  }
}

export async function handleWorkerEvent(
  event: SQSEvent,
  cfg: Config,
  getOctokit: (installationId: number) => Promise<Octokit>,
  store: RunStore,
  df: DeviceFarmClient,
): Promise<SQSBatchResponse> {
  const batchItemFailures: Array<{ itemIdentifier: string }> = [];
  for (const record of event.Records) {
    try {
      await processJob(JSON.parse(record.body) as JobMessage, cfg, getOctokit, store, df);
    } catch (err) {
      console.error("worker failed message", record.messageId, err);
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }
  return { batchItemFailures };
}

async function uploadToDeviceFarm(
  df: DeviceFarmClient,
  projectArn: string,
  name: string,
  type: UploadType,
  filePath: string,
): Promise<string> {
  const { upload } = await df.send(new CreateUploadCommand({ projectArn, name, type }));
  if (!upload?.arn || !upload?.url) throw new Error(`CreateUpload ${name} returned no ARN/URL`);

  const { size } = await stat(filePath);
  const put = await fetch(upload.url, {
    method: "PUT",
    headers: { "content-length": String(size) },
    body: Readable.toWeb(createReadStream(filePath)) as unknown as BodyInit,
    // @ts-expect-error Node fetch requires duplex: "half" for streaming bodies
    duplex: "half",
  });
  if (!put.ok) throw new Error(`Presigned upload PUT failed with HTTP ${put.status}`);

  const deadline = Date.now() + 600_000;
  while (true) {
    const out = await df.send(new GetUploadCommand({ arn: upload.arn }));
    if (out.upload?.status === "SUCCEEDED") return upload.arn;
    if (out.upload?.status === "FAILED") {
      const reason = out.upload.message || out.upload.metadata || "";
      throw new PermanentFailure(
        "Device Farm upload failed",
        `AWS Device Farm rejected \`${name}\`${reason ? `: ${reason}` : "."}`,
      );
    }
    if (Date.now() > deadline) throw new Error(`Timed out waiting for Device Farm upload ${name}`);
    await new Promise((r) => setTimeout(r, 2000));
  }
}

async function stopRun(df: DeviceFarmClient, arn: string) {
  try {
    await df.send(new StopRunCommand({ arn }));
  } catch (err) {
    if (
      !(err instanceof NotFoundException) &&
      (err as { name?: string })?.name !== "NotFoundException"
    ) {
      console.warn("failed to stop run", arn, err);
    }
  }
}

async function processJob(
  msg: JobMessage,
  cfg: Config,
  getOctokit: (installationId: number) => Promise<Octokit>,
  store: RunStore,
  df: DeviceFarmClient,
): Promise<void> {
  const projectArn = lookupProjectArn(cfg, msg.repo);
  if (!projectArn) return;
  const gh = await getOctokit(msg.installationId);
  let repo;
  try {
    repo = await fetchRepoConfig(gh, msg.repo, msg.defaultBranch || "main", projectArn);
  } catch (err) {
    if (err instanceof InvalidRepoConfigError) {
      console.error("invalid repo config", msg.repo, err.message);
      return;
    }
    throw err;
  }
  if (!repo) return;
  const base = { owner: repo.owner, repo: repo.name };

  const settle = async (
    conclusion: "success" | "failure",
    title: string,
    summary: string,
    opts: { headSha?: string; extId?: string; checkRunId?: number } = {},
  ) => {
    const completed_at = new Date().toISOString();
    const output = { title, summary };
    if (opts.checkRunId) {
      await gh.rest.checks.update({
        ...base,
        check_run_id: opts.checkRunId,
        status: "completed",
        conclusion,
        completed_at,
        output,
      });
    } else {
      await gh.rest.checks.create({
        ...base,
        name: repo.checkName,
        head_sha: opts.headSha ?? msg.headSha,
        external_id: opts.extId,
        status: "completed",
        conclusion,
        completed_at,
        output,
      });
    }
  };

  const scheduleRun = async (workflowRunId: number, requestedAttempt: number, isRerun: boolean) => {
    const { data: wfRun } = await gh.rest.actions.getWorkflowRun({
      ...base,
      run_id: workflowRunId,
    });
    const headSha = msg.headSha || wfRun.head_sha;
    const headBranch = msg.headBranch || wfRun.head_branch || "";
    const headRepo = msg.headRepo || wfRun.head_repository?.full_name || repo.fullName;
    const runAttempt = Math.max(1, requestedAttempt || wfRun.run_attempt || 1);
    if (headBranch && !branchAllowed(cfg, headBranch)) return;

    if (!isRerun) {
      const existing = await store.queryByRepoSha(repo.fullName, headSha);
      if (
        existing.some(
          (r) => r.workflow_run_id === workflowRunId && r.workflow_run_attempt === runAttempt,
        )
      ) {
        return;
      }
    }

    const extId = formatExternalId(workflowRunId, runAttempt);
    const { data: created } = await gh.rest.checks.create({
      ...base,
      name: repo.checkName,
      head_sha: headSha,
      external_id: extId,
      status: "in_progress",
      started_at: new Date().toISOString(),
      output: {
        title: "Scheduling",
        summary: "Downloading CI artifact and scheduling AWS Device Farm run.",
      },
    });
    const checkRunId = created.id;

    const bKey = branchKey(repo.fullName, headRepo, headBranch);
    if (headBranch) {
      // Stop older active runs on this branch; the poller settles their Check Runs as Stopped.
      for (const earlier of await store.queryActiveByBranch(bKey)) {
        await stopRun(df, earlier.run_arn);
      }
    }

    const dir = await mkdtemp(join(tmpdir(), "device-farm-worker-"));
    try {
      const artifacts = await gh.paginate(gh.rest.actions.listWorkflowRunArtifacts, {
        ...base,
        run_id: workflowRunId,
        name: repo.artifactName,
        per_page: 100,
      });
      const chosen = artifacts
        .filter((a) => a.name === repo.artifactName && !a.expired)
        .sort((a, b) => Date.parse(b.created_at ?? "") - Date.parse(a.created_at ?? ""))[0];
      if (!chosen) {
        throw new PermanentFailure(
          "Artifact expired",
          `The CI artifact \`${repo.artifactName}\` for this commit is missing or expired. Re-run the CI workflow to rebuild the Android test package.`,
        );
      }

      // Manual redirect prevents forwarding GitHub's Bearer token to Azure Blob Storage.
      const { token } = (await gh.auth({ type: "installation" })) as { token: string };
      const apiUrl = `https://api.github.com/repos/${encodeURIComponent(repo.owner)}/${encodeURIComponent(repo.name)}/actions/artifacts/${chosen.id}/zip`;
      const resp = await fetch(apiUrl, {
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/vnd.github+json",
          "X-GitHub-Api-Version": "2022-11-28",
        },
        redirect: "manual",
      });
      if (resp.status === 410) {
        throw new PermanentFailure(
          "Artifact expired",
          `The CI artifact \`${repo.artifactName}\` for this commit has expired. Re-run the CI workflow to rebuild the Android test package.`,
        );
      }
      const loc = resp.headers.get("location");
      await resp.body?.cancel();
      if (resp.status < 300 || resp.status >= 400 || !loc) {
        throw new Error(`Expected redirect from artifact download, got HTTP ${resp.status}`);
      }

      const outerZip = join(dir, "artifact.zip");
      try {
        await downloadStreamToFile(loc, outerZip, MAX_ARTIFACT_ZIP_BYTES);
      } catch (err) {
        if (err instanceof DownloadSizeLimitError) {
          throw new PermanentFailure(
            "Artifact too large",
            `The CI artifact \`${repo.artifactName}\` exceeds the ${Math.round(MAX_ARTIFACT_ZIP_BYTES / (1024 * 1024))} MiB worker limit.`,
          );
        }
        throw err;
      }

      const pkgPath = join(dir, repo.packageFilename);
      try {
        await extractPackageFromZip(
          outerZip,
          repo.packageFilename,
          pkgPath,
          MAX_ARTIFACT_ZIP_BYTES,
        );
      } catch (err) {
        if (err instanceof InvalidArchiveError || err instanceof PackageNotFoundError) {
          throw new PermanentFailure(
            "Invalid artifact",
            err instanceof InvalidArchiveError
              ? `The CI artifact \`${repo.artifactName}\` is not a valid ZIP archive or exceeds archive safety limits.`
              : `The CI artifact \`${repo.artifactName}\` did not contain \`${repo.packageFilename}\`.`,
          );
        }
        throw err;
      }
      await rm(outerZip, { force: true });

      // Always fetch the test spec from the repo's default branch (main), never from a PR branch.
      const { data: rawSpec } = await gh.rest.repos.getContent({
        ...base,
        path: repo.testSpecPath,
        ref: repo.defaultBranch,
        mediaType: { format: "raw" },
      });
      const specPath = join(dir, basename(repo.testSpecPath));
      await writeFile(
        specPath,
        typeof rawSpec === "string" ? rawSpec : Buffer.from(rawSpec as unknown as Uint8Array),
      );

      const testPackageArn = await uploadToDeviceFarm(
        df,
        repo.projectArn,
        repo.packageFilename,
        "APPIUM_WEB_NODE_TEST_PACKAGE",
        pkgPath,
      );
      const testSpecArn = await uploadToDeviceFarm(
        df,
        repo.projectArn,
        basename(repo.testSpecPath),
        "APPIUM_WEB_NODE_TEST_SPEC",
        specPath,
      );

      const runName =
        `${repo.name} ${headBranch || "detached"} ${headSha.slice(0, 7)} (run ${workflowRunId})`.slice(
          0,
          256,
        );
      const scheduled = await df.send(
        new ScheduleRunCommand({
          projectArn: repo.projectArn,
          name: runName,
          deviceSelectionConfiguration: {
            filters: [{ attribute: "ARN", operator: "IN", values: repo.devices }],
            maxDevices: repo.devices.length,
          },
          test: { type: "APPIUM_WEB_NODE", testPackageArn, testSpecArn },
          executionConfiguration: {
            jobTimeoutMinutes: repo.jobTimeoutMinutes,
            videoCapture: false,
          },
        }),
      );
      const runArn = scheduled.run?.arn;
      if (!runArn) throw new Error("ScheduleRun returned no run ARN");

      const now = new Date();
      try {
        await store.putRun({
          run_arn: runArn,
          state: "ACTIVE",
          created_at: now.toISOString(),
          branch_key: bKey,
          repo_sha: repoShaKey(repo.fullName, headSha),
          repo: repo.fullName,
          sha: headSha,
          head_repo: headRepo,
          head_branch: headBranch,
          check_run_id: checkRunId,
          installation_id: msg.installationId,
          workflow_run_id: workflowRunId,
          workflow_run_attempt: runAttempt,
          expires_at: Math.floor(now.getTime() / 1000) + RECORD_TTL_SEC,
        });
      } catch (err) {
        await stopRun(df, runArn);
        throw err;
      }

      await gh.rest.checks
        .update({
          ...base,
          check_run_id: checkRunId,
          status: "in_progress",
          details_url: consoleUrl(runArn),
          output: {
            title: "Running on Device Farm",
            summary: `Scheduled AWS Device Farm run on ${repo.devices.length} devices.`,
          },
        })
        .catch((err) => console.warn("failed to update check details_url", err));
    } catch (err) {
      if (err instanceof PermanentFailure) {
        await settle("failure", err.title, err.summary, { headSha, extId, checkRunId });
        return;
      }
      throw err;
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  };

  const handleConclusion = async (conclusion: string, runId: number, attempt: number) => {
    const extId = formatExternalId(runId, attempt);
    if (conclusion === "success") {
      await scheduleRun(runId, attempt, false);
    } else if (conclusion === "skipped") {
      await settle("success", "Not needed", "No Android device tests needed for this change.", {
        extId,
      });
    } else {
      await settle(
        "failure",
        "Build failed",
        `\`${repo.triggerJob}\` finished with conclusion \`${conclusion || "unknown"}\`.`,
        { extId },
      );
    }
  };

  if (msg.event === "workflow_job" && msg.action === "completed") {
    if (msg.headBranch && !branchAllowed(cfg, msg.headBranch)) return;
    await handleConclusion(
      msg.jobConclusion ?? "",
      msg.workflowRunId ?? 0,
      Math.max(1, msg.workflowRunAttempt ?? 1),
    );
  } else if (msg.event === "workflow_run" && msg.action === "completed") {
    if (msg.headBranch && !branchAllowed(cfg, msg.headBranch)) return;
    const runId = msg.workflowRunId ?? 0;
    const attempt = Math.max(1, msg.workflowRunAttempt ?? 1);
    const extId = formatExternalId(runId, attempt);

    const existing = await gh.paginate(gh.rest.checks.listForRef, {
      ...base,
      ref: msg.headSha,
      check_name: repo.checkName,
      filter: "all",
      per_page: 100,
    });
    if (existing.some((c) => c.external_id === extId)) return;

    const jobs = await gh.paginate(
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt_number}/jobs",
      { ...base, run_id: runId, attempt_number: attempt, per_page: 100 },
    );
    const trigger = jobs.find((j) => j.name === repo.triggerJob);
    if (!trigger) {
      await settle(
        "success",
        "Not needed",
        `Workflow \`${repo.workflowName}\` completed without running \`${repo.triggerJob}\`.`,
        { extId },
      );
    } else {
      await handleConclusion(trigger.conclusion ?? "", runId, attempt);
    }
  } else if (msg.event === "check_run" && msg.action === "rerequested") {
    let parsed = parseExternalId(msg.externalId);
    if (!parsed && msg.checkRunId) {
      const match = (await store.queryByRepoSha(repo.fullName, msg.headSha)).find(
        (r) => r.check_run_id === msg.checkRunId,
      );
      if (match?.workflow_run_id) {
        parsed = { runId: match.workflow_run_id, attempt: match.workflow_run_attempt || 1 };
      }
    }
    if (!parsed) {
      await settle(
        "failure",
        "Unable to re-run",
        "Could not identify the GitHub Actions workflow run for this commit. Re-run the CI workflow instead.",
      );
      return;
    }
    await scheduleRun(parsed.runId, parsed.attempt, true);
  }
}
