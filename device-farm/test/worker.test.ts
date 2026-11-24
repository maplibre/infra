import assert from "node:assert/strict";
import { createWriteStream } from "node:fs";
import { writeFile } from "node:fs/promises";
import { describe, it } from "node:test";
import {
  CreateUploadCommand,
  GetUploadCommand,
  ScheduleRunCommand,
  StopRunCommand,
} from "@aws-sdk/client-device-farm";
import type { SQSEvent } from "aws-lambda";
import type { Octokit } from "octokit";
import yazl from "yazl";
import {
  branchKey,
  claimKey,
  type JobMessage,
  loadConfig,
  repoShaKey,
  rerunClaimKey,
  type RunRecord,
} from "../src/config.js";
import { CLAIM_LEASE_SEC, RECORD_TTL_SEC, type RunStore } from "../src/store.js";
import { ArtifactExpiredError, handleWorkerEvent, type WorkerDeps } from "../src/worker.js";

function makeCfg() {
  return loadConfig({
    MODE: "worker",
    GITHUB_APP_SECRET_ARN: "arn:aws:secretsmanager:us-west-2:123:secret:gh",
    RUNS_TABLE_NAME: "runs-table",
  });
}

function sqsEvent(msg: JobMessage): SQSEvent {
  return {
    Records: [{ messageId: "m1", body: JSON.stringify(msg) }],
  } as unknown as SQSEvent;
}

class MemoryStore implements RunStore {
  records = new Map<string, RunRecord>();
  putRunErr: Error | null = null;

  async claimWorkflowRun(key: string, owner: string, now: Date): Promise<boolean> {
    const nowSec = Math.floor(now.getTime() / 1000);
    const cur = this.records.get(key);
    if (cur && (cur.state !== "CLAIMING" || (cur.lease_expires_at ?? 0) >= nowSec)) return false;
    this.records.set(key, {
      run_arn: key,
      state: "CLAIMING",
      claim_owner: owner,
      lease_expires_at: nowSec + CLAIM_LEASE_SEC,
      created_at: now.toISOString(),
      expires_at: nowSec + RECORD_TTL_SEC,
    });
    return true;
  }
  async completeWorkflowClaim(key: string, owner: string, now: Date): Promise<void> {
    const cur = this.records.get(key);
    if (!cur || cur.state !== "CLAIMING" || cur.claim_owner !== owner) return;
    cur.state = "CLAIMED";
    delete cur.lease_expires_at;
    cur.expires_at = Math.floor(now.getTime() / 1000) + RECORD_TTL_SEC;
  }
  async releaseWorkflowClaim(key: string, owner: string): Promise<void> {
    const cur = this.records.get(key);
    if (cur && cur.state === "CLAIMING" && cur.claim_owner === owner) this.records.delete(key);
  }
  async putRun(rec: RunRecord): Promise<void> {
    if (this.putRunErr) throw this.putRunErr;
    this.records.set(rec.run_arn, rec);
  }
  async queryActiveRuns(): Promise<RunRecord[]> {
    return [...this.records.values()].filter((r) => r.state === "ACTIVE");
  }
  async queryActiveByBranch(bk: string): Promise<RunRecord[]> {
    return [...this.records.values()].filter((r) => r.state === "ACTIVE" && r.branch_key === bk);
  }
  async queryByRepoSha(repo: string, sha: string): Promise<RunRecord[]> {
    const k = repoShaKey(repo, sha);
    return [...this.records.values()].filter((r) => r.repo_sha === k);
  }
  async markCompleted(arn: string, result: string, now: Date): Promise<void> {
    const cur = this.records.get(arn);
    if (cur && cur.state === "ACTIVE") {
      cur.state = "COMPLETED";
      cur.result = result;
      cur.completed_at = now.toISOString();
    }
  }
}

function makeFakeEnv() {
  let nextCheckId = 100;
  let uploadSeq = 0;
  let runSeq = 0;

  const createdChecks: any[] = [];
  const updatedChecks: any[] = [];
  const stoppedArns: string[] = [];
  const scheduledInputs: any[] = [];
  let listChecksParams: any[] = [];
  let getContentRef = "";

  const state = {
    listedChecks: [] as Array<{
      id: number;
      status: string;
      conclusion: string | null;
      external_id: string | null;
    }>,
    workflowRun: {
      id: 500,
      run_attempt: 1,
      head_branch: "feat/gpu",
      head_sha: "abc1234",
      head_repository: { full_name: "fork-user/mln-rs" },
      created_at: "2026-06-01T12:00:00Z",
    },
    jobs: [] as Array<{ name: string; conclusion: string | null }>,
    artifacts: [
      {
        id: 10,
        name: "android-device-tests",
        expired: false,
        created_at: "2026-06-01T12:01:00Z",
      },
    ],
    uploadStatus: "SUCCEEDED" as "SUCCEEDED" | "FAILED",
    downloadRawBytes: null as Buffer | null,
    downloadErr: null as Error | null,
  };

  const gh = {
    rest: {
      checks: {
        listForRef: Symbol("listForRef"),
        create: async (params: any) => {
          const id = ++nextCheckId;
          createdChecks.push(params);
          state.listedChecks.push({
            id,
            status: params.status,
            conclusion: params.conclusion ?? null,
            external_id: params.external_id ?? null,
          });
          return { data: { id } };
        },
        update: async (params: any) => {
          updatedChecks.push(params);
          const cur = state.listedChecks.find((c) => c.id === params.check_run_id);
          if (cur) {
            if (params.status) cur.status = params.status;
            if (params.conclusion) cur.conclusion = params.conclusion;
          }
          return { data: { id: params.check_run_id } };
        },
      },
      actions: {
        getWorkflowRun: async () => ({ data: state.workflowRun }),
        listWorkflowRunArtifacts: Symbol("listArtifacts"),
      },
      repos: {
        getContent: async (params: any) => {
          getContentRef = params.ref;
          return { data: "version: 0.1\n" };
        },
      },
    },
    paginate: async (endpoint: unknown, params: any) => {
      if (endpoint === gh.rest.checks.listForRef) {
        listChecksParams.push(params);
        return [...state.listedChecks];
      }
      if (endpoint === gh.rest.actions.listWorkflowRunArtifacts) return [...state.artifacts];
      if (typeof endpoint === "string" && endpoint.includes("/attempts/")) return [...state.jobs];
      throw new Error(`unexpected paginate endpoint: ${String(endpoint)}`);
    },
  } as unknown as Octokit;

  const df = {
    send: async (cmd: unknown) => {
      if (cmd instanceof CreateUploadCommand) {
        const arn = `arn:aws:devicefarm:us-west-2:123:upload:proj/up-${++uploadSeq}`;
        return { upload: { arn, url: `https://s3.example/up-${uploadSeq}` } };
      }
      if (cmd instanceof GetUploadCommand) {
        return {
          upload: {
            status: state.uploadStatus,
            message: state.uploadStatus === "FAILED" ? "bad apk" : "",
          },
        };
      }
      if (cmd instanceof ScheduleRunCommand) {
        scheduledInputs.push(cmd.input);
        return {
          run: {
            arn: `arn:aws:devicefarm:us-west-2:373521797162:run:proj/run-${++runSeq}`,
          },
        };
      }
      if (cmd instanceof StopRunCommand) {
        stoppedArns.push(cmd.input.arn!);
        return {};
      }
      throw new Error(`unexpected df cmd: ${String(cmd)}`);
    },
  };

  const store = new MemoryStore();
  const deps: WorkerDeps = {
    cfg: makeCfg(),
    getOctokit: async () => gh,
    store,
    df,
    now: () => new Date("2026-06-01T12:05:00Z"),
    pollIntervalMs: 1,
    putPresigned: async () => {},
    downloadArtifact: async (_gh, _repo, _id, dstPath) => {
      if (state.downloadErr) throw state.downloadErr;
      if (state.downloadRawBytes) {
        await writeFile(dstPath, state.downloadRawBytes);
        return;
      }
      const zf = new yazl.ZipFile();
      zf.addBuffer(Buffer.from("opaque-inner-zip"), "android-tests.zip");
      await new Promise<void>((resolve, reject) => {
        zf.outputStream.pipe(createWriteStream(dstPath)).on("close", resolve).on("error", reject);
        zf.end();
      });
    },
  };

  return {
    deps,
    store,
    state,
    createdChecks,
    updatedChecks,
    stoppedArns,
    scheduledInputs,
    getContentRef: () => getContentRef,
    listChecksParams,
  };
}

describe("worker handler", () => {
  it("schedules run, stops older active run without mutating its DB state, and fetches spec from main", async () => {
    const env = makeFakeEnv();
    const oldArn = "arn:aws:devicefarm:us-west-2:373521797162:run:proj/old-run";
    env.store.records.set(oldArn, {
      run_arn: oldArn,
      state: "ACTIVE",
      created_at: "2026-06-01T11:00:00Z",
      workflow_created_at: "2026-06-01T11:00:00Z",
      workflow_run_id: 400,
      branch_key: branchKey("maplibre/mln-rs", "fork-user/mln-rs", "feat/gpu"),
      check_run_id: 42,
      expires_at: 9999999999,
    });

    const resp = await handleWorkerEvent(
      sqsEvent({
        event: "workflow_job",
        action: "completed",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "abc1234",
        headBranch: "feat/gpu",
        workflowRunId: 500,
        workflowRunAttempt: 1,
        jobConclusion: "success",
      }),
      env.deps,
    );
    assert.deepEqual(resp.batchItemFailures, []);
    assert.equal(env.getContentRef(), "main");
    assert.deepEqual(env.stoppedArns, [oldArn]);
    assert.equal(env.store.records.get(oldArn)?.state, "ACTIVE"); // poller is sole writer
    assert.equal(env.scheduledInputs.length, 1);
    assert.equal(env.store.records.get(claimKey("maplibre/mln-rs", 500, 1))?.state, "CLAIMED");
    assert.equal(env.listChecksParams[0]?.filter, "all");

    // Duplicate delivery is deduplicated cleanly.
    const dup = await handleWorkerEvent(
      sqsEvent({
        event: "workflow_job",
        action: "completed",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "abc1234",
        headBranch: "feat/gpu",
        workflowRunId: 500,
        workflowRunAttempt: 1,
        jobConclusion: "success",
      }),
      env.deps,
    );
    assert.deepEqual(dup.batchItemFailures, []);
    assert.equal(env.scheduledInputs.length, 1);
  });

  it("rejects older workflow attempt for both normal and rerun deliveries", async () => {
    const env = makeFakeEnv();
    env.state.workflowRun.run_attempt = 3;

    await handleWorkerEvent(
      sqsEvent({
        event: "workflow_job",
        action: "completed",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "abc1234",
        headBranch: "feat/gpu",
        workflowRunId: 500,
        workflowRunAttempt: 1,
        jobConclusion: "success",
      }),
      env.deps,
    );
    assert.equal(env.scheduledInputs.length, 0);
    assert.equal(env.createdChecks[0]?.conclusion, "cancelled");
    assert.equal(env.createdChecks[0]?.output.title, "Superseded");

    await handleWorkerEvent(
      sqsEvent({
        event: "check_run",
        action: "rerequested",
        deliveryId: "rerun-1",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "abc1234",
        checkRunId: 50,
        externalId: "500:2",
      }),
      env.deps,
    );
    assert.equal(env.scheduledInputs.length, 0);
    assert.equal(env.createdChecks[1]?.conclusion, "failure");
    assert.equal(env.createdChecks[1]?.output.title, "Unable to re-run");
    assert.equal(
      env.store.records.get(rerunClaimKey("maplibre/mln-rs", "rerun-1"))?.state,
      "CLAIMED",
    );
  });

  it("settles invalid archive, expired artifact, and Device Farm upload rejection permanently without SQS retry", async () => {
    // 1. Corrupt non-ZIP archive -> Invalid artifact
    const env1 = makeFakeEnv();
    env1.state.downloadRawBytes = Buffer.from("not-a-zip");
    const r1 = await handleWorkerEvent(
      sqsEvent({
        event: "workflow_job",
        action: "completed",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "abc1234",
        headBranch: "feat/gpu",
        workflowRunId: 501,
        workflowRunAttempt: 1,
        jobConclusion: "success",
      }),
      env1.deps,
    );
    assert.deepEqual(r1.batchItemFailures, []);
    assert.equal(env1.updatedChecks.at(-1)?.output.title, "Invalid artifact");
    assert.equal(env1.store.records.get(claimKey("maplibre/mln-rs", 501, 1))?.state, "CLAIMED");

    // 2. Expired artifact -> Artifact expired
    const env2 = makeFakeEnv();
    env2.state.downloadErr = new ArtifactExpiredError();
    const r2 = await handleWorkerEvent(
      sqsEvent({
        event: "workflow_job",
        action: "completed",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "abc1234",
        headBranch: "feat/gpu",
        workflowRunId: 502,
        workflowRunAttempt: 1,
        jobConclusion: "success",
      }),
      env2.deps,
    );
    assert.deepEqual(r2.batchItemFailures, []);
    assert.equal(env2.updatedChecks.at(-1)?.output.title, "Artifact expired");

    // 3. Device Farm rejects upload -> Device Farm upload failed
    const env3 = makeFakeEnv();
    env3.state.uploadStatus = "FAILED";
    const r3 = await handleWorkerEvent(
      sqsEvent({
        event: "workflow_job",
        action: "completed",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "abc1234",
        headBranch: "feat/gpu",
        workflowRunId: 503,
        workflowRunAttempt: 1,
        jobConclusion: "success",
      }),
      env3.deps,
    );
    assert.deepEqual(r3.batchItemFailures, []);
    assert.equal(env3.updatedChecks.at(-1)?.output.title, "Device Farm upload failed");
  });

  it("handles skipped job and workflow_run safety net", async () => {
    const env = makeFakeEnv();
    await handleWorkerEvent(
      sqsEvent({
        event: "workflow_job",
        action: "completed",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "sha1",
        headBranch: "docs",
        workflowRunId: 600,
        workflowRunAttempt: 1,
        jobConclusion: "skipped",
      }),
      env.deps,
    );
    assert.equal(env.createdChecks[0]?.conclusion, "success");
    assert.equal(env.createdChecks[0]?.output.title, "Not needed");

    env.state.jobs = [{ name: "prepare-device-tests", conclusion: "failure" }];
    await handleWorkerEvent(
      sqsEvent({
        event: "workflow_run",
        action: "completed",
        repo: "maplibre/mln-rs",
        installationId: 99,
        headSha: "sha2",
        headBranch: "main",
        workflowRunId: 601,
        workflowRunAttempt: 2,
      }),
      env.deps,
    );
    assert.equal(env.createdChecks[1]?.conclusion, "failure");
    assert.equal(env.createdChecks[1]?.output.title, "Build failed");
  });
});
