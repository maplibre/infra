import { randomUUID } from "node:crypto";
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
import type { Octokit } from "octokit";
import type { SQSBatchResponse, SQSEvent } from "aws-lambda";
import {
  branchAllowed,
  branchKey,
  claimKey,
  type Config,
  formatExternalId,
  isDeviceFarmArn,
  type JobMessage,
  lookupRepo,
  parseExternalId,
  type RepoConfig,
  repoShaKey,
  rerunClaimKey,
  type RunRecord,
} from "./config.js";
import {
  consoleUrl,
  downloadStreamToFile,
  extractPackageFromZip,
  InvalidArchiveError,
  PackageNotFoundError,
} from "./report.js";
import { RECORD_TTL_SEC, type RunStore } from "./store.js";

export const MAX_ARTIFACT_ZIP_BYTES = 256 << 20;

export class ArtifactExpiredError extends Error {
  constructor() {
    super("GitHub Actions artifact has expired");
  }
}

export class UploadRejectedError extends Error {
  constructor(
    public readonly uploadName: string,
    public readonly reason: string,
  ) {
    super(`Device Farm rejected upload ${uploadName}${reason ? `: ${reason}` : ""}`);
  }
}

export interface WorkerDeps {
  cfg: Config;
  getOctokit: (installationId: number) => Promise<Octokit>;
  store: RunStore;
  df: Pick<DeviceFarmClient, "send">;
  now?: () => Date;
  pollIntervalMs?: number;
  downloadArtifact?: (
    gh: Octokit,
    repo: RepoConfig,
    artifactId: number,
    dstPath: string,
  ) => Promise<void>;
  putPresigned?: (url: string, filePath: string) => Promise<void>;
}

async function defaultDownloadArtifact(
  gh: Octokit,
  repo: RepoConfig,
  artifactId: number,
  dstPath: string,
) {
  const { token } = (await gh.auth({ type: "installation" })) as {
    token: string;
  };
  const apiUrl = `https://api.github.com/repos/${encodeURIComponent(repo.owner)}/${encodeURIComponent(repo.name)}/actions/artifacts/${artifactId}/zip`;
  // Manual redirect prevents forwarding GitHub's Bearer token to Azure Blob Storage.
  const resp = await fetch(apiUrl, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    redirect: "manual",
  });
  if (resp.status === 410) throw new ArtifactExpiredError();
  const loc = resp.headers.get("location");
  await resp.body?.cancel();
  if (resp.status >= 300 && resp.status < 400 && loc) {
    await downloadStreamToFile(loc, dstPath, MAX_ARTIFACT_ZIP_BYTES);
    return;
  }
  throw new Error(`Expected redirect from artifact download, got HTTP ${resp.status}`);
}

async function defaultPutPresigned(url: string, filePath: string) {
  const { size } = await stat(filePath);
  const resp = await fetch(url, {
    method: "PUT",
    headers: { "content-length": String(size) },
    body: Readable.toWeb(createReadStream(filePath)) as unknown as BodyInit,
    // @ts-expect-error Node fetch requires duplex: "half" for streaming bodies
    duplex: "half",
  });
  if (!resp.ok) throw new Error(`Presigned upload PUT failed with HTTP ${resp.status}`);
}

async function uploadToDeviceFarm(
  deps: WorkerDeps,
  projectArn: string,
  name: string,
  type: UploadType,
  filePath: string,
): Promise<string> {
  const created = await deps.df.send(new CreateUploadCommand({ projectArn, name, type }));
  const arn = created.upload?.arn;
  const url = created.upload?.url;
  if (!arn || !url) throw new Error(`CreateUpload ${name} returned incomplete metadata`);
  await (deps.putPresigned ?? defaultPutPresigned)(url, filePath);

  const deadline = Date.now() + 240_000;
  while (true) {
    const out = await deps.df.send(new GetUploadCommand({ arn }));
    if (out.upload?.status === "SUCCEEDED") return arn;
    if (out.upload?.status === "FAILED") {
      throw new UploadRejectedError(name, out.upload.message || out.upload.metadata || "");
    }
    if (Date.now() > deadline) throw new Error(`Timed out waiting for Device Farm upload ${name}`);
    await new Promise((r) => setTimeout(r, deps.pollIntervalMs ?? 2000));
  }
}

async function stopDeviceFarmRun(df: Pick<DeviceFarmClient, "send">, arn: string) {
  try {
    await df.send(new StopRunCommand({ arn }));
  } catch (err) {
    const isNotFound =
      err instanceof NotFoundException ||
      (typeof err === "object" &&
        err !== null &&
        (err as { name?: string }).name === "NotFoundException");
    if (!isNotFound) throw err;
  }
}

function isNewerWorkflowRun(
  rec: RunRecord,
  incomingCreatedAt: string,
  incomingRunId: number,
): boolean {
  const existingMs = Date.parse(rec.workflow_created_at || rec.created_at || "");
  const incomingMs = Date.parse(incomingCreatedAt);
  if (!Number.isNaN(existingMs) && !Number.isNaN(incomingMs) && existingMs !== incomingMs) {
    return existingMs > incomingMs;
  }
  return Boolean(rec.workflow_run_id && incomingRunId && rec.workflow_run_id > incomingRunId);
}

async function findCheck(gh: Octokit, repo: RepoConfig, headSha: string, extId: string) {
  const runs = await gh.paginate(gh.rest.checks.listForRef, {
    owner: repo.owner,
    repo: repo.name,
    ref: headSha,
    check_name: repo.checkName,
    filter: "all",
    per_page: 100,
  });
  let openId = 0;
  for (const c of runs) {
    if (c.external_id !== extId) continue;
    if (c.status === "completed") return { completed: true, openId: 0 };
    if (c.id > openId) openId = c.id;
  }
  return { completed: false, openId };
}

async function settleCheck(
  gh: Octokit,
  repo: RepoConfig,
  headSha: string,
  extId: string,
  existingId: number,
  conclusion: "success" | "failure" | "cancelled",
  title: string,
  summary: string,
) {
  const completed_at = new Date().toISOString();
  if (existingId > 0) {
    await gh.rest.checks.update({
      owner: repo.owner,
      repo: repo.name,
      check_run_id: existingId,
      status: "completed",
      conclusion,
      completed_at,
      output: { title, summary },
    });
  } else {
    await gh.rest.checks.create({
      owner: repo.owner,
      repo: repo.name,
      name: repo.checkName,
      head_sha: headSha,
      external_id: extId,
      status: "completed",
      conclusion,
      completed_at,
      output: { title, summary },
    });
  }
}

export async function handleWorkerEvent(
  event: SQSEvent,
  deps: WorkerDeps,
): Promise<SQSBatchResponse> {
  const batchItemFailures: Array<{ itemIdentifier: string }> = [];
  for (const record of event.Records) {
    try {
      await processJob(JSON.parse(record.body) as JobMessage, deps);
    } catch (err) {
      console.error("worker failed message", record.messageId, err);
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }
  return { batchItemFailures };
}

async function processJob(msg: JobMessage, deps: WorkerDeps): Promise<void> {
  const repo = lookupRepo(deps.cfg, msg.repo);
  if (!repo) return;
  const gh = await deps.getOctokit(msg.installationId);

  if (msg.event === "workflow_job" && msg.action === "completed") {
    if (msg.headBranch && !branchAllowed(deps.cfg, msg.headBranch)) return;
    const attempt = Math.max(1, msg.workflowRunAttempt ?? 1);
    const extId = formatExternalId(msg.workflowRunId ?? 0, attempt);
    if (msg.jobConclusion === "success") {
      await scheduleDeviceFarmRun(gh, repo, msg, attempt, false, deps);
    } else if (msg.jobConclusion === "skipped") {
      await settleCheck(
        gh,
        repo,
        msg.headSha,
        extId,
        0,
        "success",
        "Not needed",
        "No Android device tests needed for this change.",
      );
    } else {
      await settleCheck(
        gh,
        repo,
        msg.headSha,
        extId,
        0,
        "failure",
        "Build failed",
        `\`${repo.triggerJob}\` finished with conclusion \`${msg.jobConclusion || "unknown"}\`.`,
      );
    }
    return;
  }

  if (msg.event === "workflow_run" && msg.action === "completed") {
    if (msg.headBranch && !branchAllowed(deps.cfg, msg.headBranch)) return;
    const runId = msg.workflowRunId ?? 0;
    const attempt = Math.max(1, msg.workflowRunAttempt ?? 1);
    const extId = formatExternalId(runId, attempt);
    const { completed, openId } = await findCheck(gh, repo, msg.headSha, extId);
    if (completed || openId > 0) return;

    const jobs = await gh.paginate(
      "GET /repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt_number}/jobs",
      {
        owner: repo.owner,
        repo: repo.name,
        run_id: runId,
        attempt_number: attempt,
        per_page: 100,
      },
    );
    const trigger = jobs.find((j) => j.name === repo.triggerJob);
    if (!trigger) {
      await settleCheck(
        gh,
        repo,
        msg.headSha,
        extId,
        0,
        "success",
        "Not needed",
        `Workflow \`${repo.workflowName}\` completed without running \`${repo.triggerJob}\`.`,
      );
    } else if (trigger.conclusion === "success") {
      await scheduleDeviceFarmRun(gh, repo, msg, attempt, false, deps);
    } else if (trigger.conclusion === "skipped") {
      await settleCheck(
        gh,
        repo,
        msg.headSha,
        extId,
        0,
        "success",
        "Not needed",
        "No Android device tests needed for this change.",
      );
    } else {
      await settleCheck(
        gh,
        repo,
        msg.headSha,
        extId,
        0,
        "failure",
        "Build failed",
        `\`${repo.triggerJob}\` finished with conclusion \`${trigger.conclusion || "unknown"}\`.`,
      );
    }
    return;
  }

  if (msg.event === "check_run" && msg.action === "rerequested") {
    let parsed = parseExternalId(msg.externalId);
    if (!parsed && msg.checkRunId) {
      const existing = await deps.store.queryByRepoSha(repo.fullName, msg.headSha);
      const match = existing.find(
        (r) => isDeviceFarmArn(r.run_arn) && r.check_run_id === msg.checkRunId,
      );
      if (match?.workflow_run_id)
        parsed = {
          runId: match.workflow_run_id,
          attempt: Math.max(1, match.workflow_run_attempt ?? 1),
        };
    }
    if (!parsed) {
      await settleCheck(
        gh,
        repo,
        msg.headSha,
        "",
        0,
        "failure",
        "Unable to re-run",
        "Could not identify the GitHub Actions workflow run for this commit. Re-run the CI workflow instead.",
      );
      return;
    }
    await scheduleDeviceFarmRun(
      gh,
      repo,
      { ...msg, workflowRunId: parsed.runId },
      parsed.attempt,
      true,
      deps,
    );
  }
}

async function scheduleDeviceFarmRun(
  gh: Octokit,
  repo: RepoConfig,
  msg: JobMessage,
  requestedAttempt: number,
  isRerun: boolean,
  deps: WorkerDeps,
): Promise<void> {
  const now = () => (deps.now ? deps.now() : new Date());
  const workflowRunId = msg.workflowRunId!;
  const { data: wfRun } = await gh.rest.actions.getWorkflowRun({
    owner: repo.owner,
    repo: repo.name,
    run_id: workflowRunId,
  });
  const headSha = msg.headSha || wfRun.head_sha;
  const headBranch = msg.headBranch || wfRun.head_branch || "";
  const headRepo = msg.headRepo || wfRun.head_repository?.full_name || repo.fullName;
  const workflowCreatedAt = msg.workflowCreatedAt || wfRun.created_at || now().toISOString();
  const runAttempt = Math.max(1, requestedAttempt || wfRun.run_attempt || 1);
  if (headBranch && !branchAllowed(deps.cfg, headBranch)) return;

  const cKey = !isRerun
    ? claimKey(repo.fullName, workflowRunId, runAttempt)
    : msg.deliveryId
      ? rerunClaimKey(repo.fullName, msg.deliveryId)
      : "";
  const owner = randomUUID();
  if (cKey && !(await deps.store.claimWorkflowRun(cKey, owner, now()))) return;

  const completeClaim = async () => {
    if (cKey) await deps.store.completeWorkflowClaim(cKey, owner, now()).catch(() => {});
  };
  const releaseClaim = async () => {
    if (cKey) await deps.store.releaseWorkflowClaim(cKey, owner).catch(() => {});
  };

  try {
    if (cKey) {
      const existing = await deps.store.queryByRepoSha(repo.fullName, headSha);
      const dup = existing.some(
        (r) =>
          isDeviceFarmArn(r.run_arn) &&
          (isRerun
            ? Boolean(msg.deliveryId && r.rerun_delivery_id === msg.deliveryId)
            : r.workflow_run_id === workflowRunId &&
              Math.max(1, r.workflow_run_attempt ?? 1) === runAttempt),
      );
      if (dup) return void (await completeClaim());
    }

    const extId = formatExternalId(workflowRunId, runAttempt, isRerun ? msg.deliveryId : undefined);
    const findOpenCheck = async () =>
      isRerun && !msg.deliveryId ? 0 : (await findCheck(gh, repo, headSha, extId)).openId;
    const finish = async (
      checkId: number,
      conclusion: "success" | "failure" | "cancelled",
      title: string,
      summary: string,
    ) => {
      await settleCheck(gh, repo, headSha, extId, checkId, conclusion, title, summary);
      await completeClaim();
    };

    // Reject older workflow attempts because ListWorkflowRunArtifacts is run-wide.
    if ((wfRun.run_attempt ?? 1) > runAttempt) {
      return await finish(
        await findOpenCheck(),
        isRerun ? "failure" : "cancelled",
        isRerun ? "Unable to re-run" : "Superseded",
        isRerun
          ? `Workflow run ${workflowRunId} already has a newer attempt (${wfRun.run_attempt}), so attempt ${runAttempt}'s artifact can no longer be uniquely identified. Re-run the latest attempt's check instead.`
          : `Superseded by workflow run attempt ${wfRun.run_attempt}.`,
      );
    }

    const bKey = branchKey(repo.fullName, headRepo, headBranch);
    const activeOnBranch = headBranch ? await deps.store.queryActiveByBranch(bKey) : [];
    const newer = !isRerun
      ? activeOnBranch.find(
          (r) =>
            isDeviceFarmArn(r.run_arn) && isNewerWorkflowRun(r, workflowCreatedAt, workflowRunId),
        )
      : undefined;
    if (newer) {
      return await finish(
        await findOpenCheck(),
        "cancelled",
        "Superseded",
        newer.sha === headSha
          ? "Superseded by newer run"
          : `Superseded by newer commit ${(newer.sha ?? "").slice(0, 7)}`,
      );
    }

    let checkRunId = await findOpenCheck();
    if (!checkRunId) {
      const { data: created } = await gh.rest.checks.create({
        owner: repo.owner,
        repo: repo.name,
        name: repo.checkName,
        head_sha: headSha,
        external_id: extId,
        status: "in_progress",
        started_at: now().toISOString(),
        output: {
          title: "Scheduling",
          summary: "Downloading CI artifact and scheduling AWS Device Farm run.",
        },
      });
      checkRunId = created.id;
    }

    // Stop older ACTIVE runs on Device Farm; the poller is the sole writer for ACTIVE runs.
    for (const earlier of activeOnBranch) {
      if (!isDeviceFarmArn(earlier.run_arn)) continue;
      if (!isRerun && isNewerWorkflowRun(earlier, workflowCreatedAt, workflowRunId)) continue;
      await stopDeviceFarmRun(deps.df, earlier.run_arn).catch((err) =>
        console.warn("failed to stop run", earlier.run_arn, err),
      );
    }

    const artifacts = await gh.paginate(gh.rest.actions.listWorkflowRunArtifacts, {
      owner: repo.owner,
      repo: repo.name,
      run_id: workflowRunId,
      name: repo.artifactName,
      per_page: 100,
    });
    const chosen = artifacts
      .filter((a) => a.name === repo.artifactName && !a.expired)
      .sort((a, b) => Date.parse(b.created_at ?? "") - Date.parse(a.created_at ?? ""))[0];
    if (!chosen) {
      return await finish(
        checkRunId,
        "failure",
        "Artifact expired",
        `The CI artifact \`${repo.artifactName}\` for this commit is missing or expired. Re-run the CI workflow to rebuild the Android test package.`,
      );
    }

    const dir = await mkdtemp(join(tmpdir(), "device-farm-worker-"));
    try {
      const outerZip = join(dir, "artifact.zip");
      try {
        await (deps.downloadArtifact ?? defaultDownloadArtifact)(gh, repo, chosen.id, outerZip);
      } catch (err) {
        if (err instanceof ArtifactExpiredError) {
          return await finish(
            checkRunId,
            "failure",
            "Artifact expired",
            `The CI artifact \`${repo.artifactName}\` for this commit has expired. Re-run the CI workflow to rebuild the Android test package.`,
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
        if (err instanceof PackageNotFoundError || err instanceof InvalidArchiveError) {
          return await finish(
            checkRunId,
            "failure",
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
        owner: repo.owner,
        repo: repo.name,
        path: repo.testSpecPath,
        ref: repo.defaultBranch,
        mediaType: { format: "raw" },
      });
      const specPath = join(dir, basename(repo.testSpecPath));
      await writeFile(
        specPath,
        typeof rawSpec === "string" ? rawSpec : Buffer.from(rawSpec as unknown as Uint8Array),
      );

      let testPackageArn: string, testSpecArn: string;
      try {
        testPackageArn = await uploadToDeviceFarm(
          deps,
          repo.projectArn,
          repo.packageFilename,
          "APPIUM_WEB_NODE_TEST_PACKAGE",
          pkgPath,
        );
        testSpecArn = await uploadToDeviceFarm(
          deps,
          repo.projectArn,
          basename(repo.testSpecPath),
          "APPIUM_WEB_NODE_TEST_SPEC",
          specPath,
        );
      } catch (err) {
        if (err instanceof UploadRejectedError) {
          return await finish(
            checkRunId,
            "failure",
            "Device Farm upload failed",
            `AWS Device Farm rejected \`${err.uploadName}\`${err.reason ? `: ${err.reason}` : "."}`,
          );
        }
        throw err;
      }

      const runName =
        `${repo.name} ${headBranch || "detached"} ${headSha.slice(0, 7)} (run ${workflowRunId})`.slice(
          0,
          256,
        );
      const scheduled = await deps.df.send(
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

      const ts = now();
      try {
        await deps.store.putRun({
          run_arn: runArn,
          state: "ACTIVE",
          created_at: ts.toISOString(),
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
          workflow_created_at: workflowCreatedAt,
          rerun_delivery_id: isRerun ? msg.deliveryId : undefined,
          expires_at: Math.floor(ts.getTime() / 1000) + RECORD_TTL_SEC,
        });
      } catch (err) {
        await stopDeviceFarmRun(deps.df, runArn).catch(() => {});
        throw err;
      }

      await completeClaim();
      await gh.rest.checks
        .update({
          owner: repo.owner,
          repo: repo.name,
          check_run_id: checkRunId,
          status: "in_progress",
          details_url: consoleUrl(runArn),
          output: {
            title: "Running on Device Farm",
            summary: `Scheduled AWS Device Farm run on ${repo.devices.length} devices.`,
          },
        })
        .catch((err) => console.warn("failed to update check run to running", checkRunId, err));
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  } catch (err) {
    await releaseClaim();
    throw err;
  }
}
