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
  findTargetByName,
  formatExternalId,
  InvalidRepoConfigError,
  type JobMessage,
  lookupProjectArn,
  parseExternalId,
  type RepoConfig,
  repoShaKey,
  type TargetConfig,
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

const appUploadType = (filename: string): UploadType =>
  filename.endsWith(".ipa") ? "IOS_APP" : "ANDROID_APP";

const isPullRequest = (event?: string) =>
  event === "pull_request" || event === "pull_request_target";

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
  let repo: RepoConfig | null;
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
    target: TargetConfig,
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
        name: target.checkName,
        head_sha: opts.headSha ?? msg.headSha,
        external_id: opts.extId,
        status: "completed",
        conclusion,
        completed_at,
        output,
      });
    }
  };

  const scheduleRun = async (
    target: TargetConfig,
    workflowRunId: number,
    requestedAttempt: number,
    isRerun: boolean,
  ) => {
    const { data: wfRun } = await gh.rest.actions.getWorkflowRun({
      ...base,
      run_id: workflowRunId,
    });
    const headSha = msg.headSha || wfRun.head_sha;
    const headBranch = msg.headBranch || wfRun.head_branch || "";
    const headRepo = msg.headRepo || wfRun.head_repository?.full_name || repo.fullName;
    const runAttempt = Math.max(1, requestedAttempt || wfRun.run_attempt || 1);
    if (headBranch && !branchAllowed(cfg, headBranch)) return;
    const extId = formatExternalId(workflowRunId, runAttempt);

    if (!repo.pullRequests && isPullRequest(wfRun.event)) {
      await settle(
        target,
        "success",
        "Not needed",
        `Pull requests don't run on the devices; \`${repo.defaultBranch}\`'s commits do.`,
        { headSha, extId },
      );
      return;
    }

    if (!isRerun) {
      const existing = await store.queryByRepoSha(repo.fullName, headSha);
      if (
        existing.some(
          (r) =>
            r.target === target.name &&
            r.workflow_run_id === workflowRunId &&
            r.workflow_run_attempt === runAttempt,
        )
      ) {
        return;
      }
    }

    const { data: created } = await gh.rest.checks.create({
      ...base,
      name: target.checkName,
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

    // A newer commit supersedes a branch's earlier runs, but each of the default branch's
    // commits gets its own run.
    const bKey = branchKey(repo.fullName, target.name, headRepo, headBranch);
    const isDefaultBranch =
      headRepo.toLowerCase() === repo.fullName.toLowerCase() && headBranch === repo.defaultBranch;
    if (headBranch && !isDefaultBranch) {
      // The poller settles the stopped runs' Check Runs as cancelled.
      for (const earlier of await store.queryActiveByBranch(bKey)) {
        await stopRun(df, earlier.run_arn);
      }
    }

    const dir = await mkdtemp(join(tmpdir(), "device-farm-worker-"));
    try {
      const artifacts = await gh.paginate(gh.rest.actions.listWorkflowRunArtifacts, {
        ...base,
        run_id: workflowRunId,
        name: target.artifactName,
        per_page: 100,
      });
      const chosen = artifacts
        .filter((a) => a.name === target.artifactName && !a.expired)
        .sort((a, b) => Date.parse(b.created_at ?? "") - Date.parse(a.created_at ?? ""))[0];
      if (!chosen) {
        throw new PermanentFailure(
          "Artifact expired",
          `The CI artifact \`${target.artifactName}\` for this commit is missing or expired. Re-run the CI workflow to rebuild it.`,
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
          `The CI artifact \`${target.artifactName}\` for this commit has expired. Re-run the CI workflow to rebuild it.`,
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
            `The CI artifact \`${target.artifactName}\` exceeds the ${Math.round(MAX_ARTIFACT_ZIP_BYTES / (1024 * 1024))} MiB worker limit.`,
          );
        }
        throw err;
      }

      const extract = async (filename: string) => {
        const path = join(dir, filename);
        try {
          await extractPackageFromZip(outerZip, filename, path, MAX_ARTIFACT_ZIP_BYTES);
        } catch (err) {
          if (err instanceof InvalidArchiveError || err instanceof PackageNotFoundError) {
            throw new PermanentFailure(
              "Invalid artifact",
              err instanceof InvalidArchiveError
                ? `The CI artifact \`${target.artifactName}\` is not a valid ZIP archive or exceeds archive safety limits.`
                : `The CI artifact \`${target.artifactName}\` did not contain \`${filename}\`.`,
            );
          }
          throw err;
        }
        return path;
      };
      const pkgPath = await extract(target.packageFilename);
      const appPath = target.appFilename ? await extract(target.appFilename) : undefined;
      await rm(outerZip, { force: true });

      // Always fetch the test spec from the repo's default branch (main), never from a PR branch.
      const { data: rawSpec } = await gh.rest.repos.getContent({
        ...base,
        path: target.testSpecPath,
        ref: repo.defaultBranch,
        mediaType: { format: "raw" },
      });
      const specPath = join(dir, basename(target.testSpecPath));
      await writeFile(
        specPath,
        typeof rawSpec === "string" ? rawSpec : Buffer.from(rawSpec as unknown as Uint8Array),
      );

      const [testPackageArn, testSpecArn, appArn] = await Promise.all([
        uploadToDeviceFarm(
          df,
          repo.projectArn,
          target.packageFilename,
          `${target.testType}_TEST_PACKAGE`,
          pkgPath,
        ),
        uploadToDeviceFarm(
          df,
          repo.projectArn,
          basename(target.testSpecPath),
          `${target.testType}_TEST_SPEC`,
          specPath,
        ),
        appPath && target.appFilename
          ? uploadToDeviceFarm(
              df,
              repo.projectArn,
              target.appFilename,
              appUploadType(target.appFilename),
              appPath,
            )
          : undefined,
      ]);

      const runName =
        `${repo.name} ${target.name} ${headBranch || "detached"} ${headSha.slice(0, 7)} (run ${workflowRunId})`.slice(
          0,
          256,
        );
      const scheduled = await df.send(
        new ScheduleRunCommand({
          projectArn: repo.projectArn,
          appArn,
          name: runName,
          deviceSelectionConfiguration: {
            filters: [{ attribute: "ARN", operator: "IN", values: target.devices }],
            maxDevices: target.devices.length,
          },
          test: { type: target.testType, testPackageArn, testSpecArn },
          configuration: { billingMethod: repo.billingMethod },
          executionConfiguration: {
            jobTimeoutMinutes: target.jobTimeoutMinutes,
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
          target: target.name,
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
            summary: `Scheduled AWS Device Farm run on ${target.devices.length} devices.`,
          },
        })
        .catch((err) => console.warn("failed to update check details_url", err));
    } catch (err) {
      if (err instanceof PermanentFailure) {
        await settle(target, "failure", err.title, err.summary, { headSha, extId, checkRunId });
        return;
      }
      throw err;
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  };

  const handleConclusion = async (
    target: TargetConfig,
    conclusion: string,
    runId: number,
    attempt: number,
  ) => {
    const extId = formatExternalId(runId, attempt);
    if (conclusion === "success") {
      await scheduleRun(target, runId, attempt, false);
    } else if (conclusion === "skipped") {
      await settle(
        target,
        "success",
        "Not needed",
        `CI skipped \`${target.triggerJob}\`, so this change needs no device tests.`,
        { extId },
      );
    } else {
      await settle(
        target,
        "failure",
        "Build failed",
        `\`${target.triggerJob}\` finished with conclusion \`${conclusion || "unknown"}\`.`,
        { extId },
      );
    }
  };

  if (msg.event === "workflow_job" && msg.action === "completed") {
    if (msg.headBranch && !branchAllowed(cfg, msg.headBranch)) return;
    const target = findTargetByName(repo, msg.target);
    if (!target) return;
    await handleConclusion(
      target,
      msg.jobConclusion ?? "",
      msg.workflowRunId ?? 0,
      Math.max(1, msg.workflowRunAttempt ?? 1),
    );
  } else if (msg.event === "workflow_run" && msg.action === "completed") {
    if (msg.headBranch && !branchAllowed(cfg, msg.headBranch)) return;
    const runId = msg.workflowRunId ?? 0;
    const attempt = Math.max(1, msg.workflowRunAttempt ?? 1);
    const extId = formatExternalId(runId, attempt);

    const jobs = await gh.paginate(
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt_number}/jobs",
      { ...base, run_id: runId, attempt_number: attempt, per_page: 100 },
    );
    for (const target of repo.targets) {
      const existing = await gh.paginate(gh.rest.checks.listForRef, {
        ...base,
        ref: msg.headSha,
        check_name: target.checkName,
        filter: "all",
        per_page: 100,
      });
      if (existing.some((c) => c.external_id === extId)) continue;

      const trigger = jobs.find((j) => j.name === target.triggerJob);
      if (!trigger) {
        await settle(
          target,
          "success",
          "Not needed",
          `Workflow \`${repo.workflowName}\` completed without running \`${target.triggerJob}\`.`,
          { extId },
        );
      } else {
        await handleConclusion(target, trigger.conclusion ?? "", runId, attempt);
      }
    }
  } else if (msg.event === "check_run" && msg.action === "rerequested") {
    const target = findTargetByName(repo, msg.target);
    if (!target) return;
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
        target,
        "failure",
        "Unable to re-run",
        "Could not identify the GitHub Actions workflow run for this commit. Re-run the CI workflow instead.",
      );
      return;
    }
    await scheduleRun(target, parsed.runId, parsed.attempt, true);
  }
}
