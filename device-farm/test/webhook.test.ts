import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { sign } from "@octokit/webhooks-methods";
import type { APIGatewayProxyEventV2 } from "aws-lambda";
import {
  InvalidRepoConfigError,
  type JobMessage,
  loadConfig,
  type RepoConfig,
} from "../src/config.js";
import { handleWebhook, type RepoConfigLoader } from "../src/webhook.js";

const SECRET = "test-webhook-secret";
const PROJECT_ARN = "arn:aws:devicefarm:us-west-2:373521797162:project:proj-1";

const SAMPLE_REPO: RepoConfig = {
  fullName: "maplibre/mln-rs",
  owner: "maplibre",
  name: "mln-rs",
  defaultBranch: "main",
  projectArn: PROJECT_ARN,
  workflowName: "ci",
  pullRequests: false,
  billingMethod: "UNMETERED",
  targets: [
    {
      name: "android",
      triggerJob: "prepare-android-device-tests",
      artifactName: "android-device-tests",
      testType: "APPIUM_WEB_NODE",
      packageFilename: "tests.zip",
      testSpecPath: "tools/device-farm/android/test-spec.yml",
      checkName: "device-test / android",
      jobTimeoutMinutes: 60,
      devices: ["arn:aws:devicefarm:us-west-2::device:6D89F408709B4D3BBBBB17696A24B141"],
    },
    {
      name: "ios",
      triggerJob: "prepare-ios-device-tests",
      artifactName: "ios-device-tests",
      testType: "APPIUM_NODE",
      packageFilename: "tests.zip",
      appFilename: "app.ipa",
      testSpecPath: "tools/device-farm/ios/test-spec.yml",
      checkName: "device-test / ios",
      jobTimeoutMinutes: 60,
      devices: ["arn:aws:devicefarm:us-west-2::device:6200F380A4874FEB9C72EED72B863B67"],
    },
  ],
};

const loadSample: RepoConfigLoader = async () => SAMPLE_REPO;

function makeCfg(allowedBranches = "") {
  return loadConfig({
    MODE: "webhook",
    GITHUB_APP_SECRET_ARN: "arn:aws:secretsmanager:us-west-2:123:secret:gh",
    JOBS_QUEUE_URL: "https://sqs.us-west-2.amazonaws.com/123/jobs",
    REPO_PROJECTS: JSON.stringify({ "maplibre/mln-rs": PROJECT_ARN }),
    ALLOWED_BRANCHES: allowedBranches,
  });
}

async function makeEvent(
  eventType: string,
  payload: unknown,
  sigOverride?: string,
): Promise<APIGatewayProxyEventV2> {
  const body = JSON.stringify(payload);
  const sig = sigOverride ?? (await sign(SECRET, body));
  return {
    headers: {
      "x-github-event": eventType,
      "x-github-delivery": "delivery-1",
      "x-hub-signature-256": sig,
    },
    body,
    isBase64Encoded: false,
    requestContext: { http: { method: "POST" } },
  } as unknown as APIGatewayProxyEventV2;
}

describe("webhook handler", () => {
  it("rejects invalid signatures and handles ping", async () => {
    const queued: JobMessage[] = [];
    const cfg = makeCfg();
    const getSecret = async () => SECRET;
    const enqueue = async (m: JobMessage) => void queued.push(m);

    const bad = await handleWebhook(
      await makeEvent("ping", {}, "sha256=deadbeef"),
      cfg,
      getSecret,
      loadSample,
      enqueue,
    );
    assert.equal(bad.statusCode, 401);

    const pong = await handleWebhook(
      await makeEvent("ping", { zen: "hi" }),
      cfg,
      getSecret,
      loadSample,
      enqueue,
    );
    assert.equal(pong.statusCode, 200);
    assert.equal(queued.length, 0);
  });

  it("enqueues workflow_job completed and check_run rerequested, ignoring irrelevant actions before loading config", async () => {
    const queued: JobMessage[] = [];
    const cfg = makeCfg();
    const getSecret = async () => SECRET;
    const enqueue = async (m: JobMessage) => void queued.push(m);

    let configFetches = 0;
    const loader: RepoConfigLoader = async () => {
      configFetches++;
      return SAMPLE_REPO;
    };

    const inProg = await handleWebhook(
      await makeEvent("workflow_job", {
        action: "in_progress",
        repository: { full_name: "maplibre/mln-rs", default_branch: "main" },
        installation: { id: 99 },
        workflow_job: {
          name: "prepare-android-device-tests",
          workflow_name: "ci",
          run_id: 10,
          head_sha: "abc",
        },
      }),
      cfg,
      getSecret,
      loader,
      enqueue,
    );
    assert.equal(inProg.statusCode, 200);
    assert.equal(configFetches, 0);

    const jobDone = await handleWebhook(
      await makeEvent("workflow_job", {
        action: "completed",
        repository: { full_name: "maplibre/mln-rs", default_branch: "main" },
        installation: { id: 99 },
        workflow_job: {
          name: "prepare-android-device-tests",
          workflow_name: "ci",
          run_id: 10,
          run_attempt: 2,
          head_sha: "abc123",
          head_branch: "main",
          conclusion: "success",
        },
      }),
      cfg,
      getSecret,
      loader,
      enqueue,
    );
    assert.equal(jobDone.statusCode, 202);
    assert.equal(queued[0]?.workflowRunAttempt, 2);
    assert.equal(queued[0]?.defaultBranch, "main");
    assert.equal(queued[0]?.target, "android");

    const otherJob = await handleWebhook(
      await makeEvent("workflow_job", {
        action: "completed",
        repository: { full_name: "maplibre/mln-rs", default_branch: "main" },
        installation: { id: 99 },
        workflow_job: {
          name: "target (android)",
          workflow_name: "ci",
          run_id: 10,
          head_sha: "abc123",
          conclusion: "success",
        },
      }),
      cfg,
      getSecret,
      loader,
      enqueue,
    );
    assert.equal(otherJob.statusCode, 200);
    assert.equal(queued.length, 1);

    const rerun = await handleWebhook(
      await makeEvent("check_run", {
        action: "rerequested",
        repository: { full_name: "maplibre/mln-rs", default_branch: "main" },
        installation: { id: 99 },
        check_run: {
          id: 50,
          name: "device-test / ios",
          head_sha: "abc123",
          external_id: "10:2",
        },
      }),
      cfg,
      getSecret,
      loader,
      enqueue,
    );
    assert.equal(rerun.statusCode, 202);
    assert.equal(queued[1]?.externalId, "10:2");
    assert.equal(queued[1]?.target, "ios");
  });

  it("handles missing or invalid .config/device-farm.toml and ALLOWED_BRANCHES", async () => {
    const queued: JobMessage[] = [];
    const ev = await makeEvent("workflow_job", {
      action: "completed",
      repository: { full_name: "maplibre/mln-rs", default_branch: "main" },
      installation: { id: 99 },
      workflow_job: {
        name: "prepare-android-device-tests",
        workflow_name: "ci",
        run_id: 10,
        head_sha: "abc",
        head_branch: "other-branch",
        conclusion: "success",
      },
    });

    const missing = await handleWebhook(
      ev,
      makeCfg(),
      async () => SECRET,
      async () => null,
      async (m) => void queued.push(m),
    );
    assert.equal(missing.statusCode, 200);

    const invalid = await handleWebhook(
      ev,
      makeCfg(),
      async () => SECRET,
      async () => {
        throw new InvalidRepoConfigError("bad toml");
      },
      async (m) => void queued.push(m),
    );
    assert.equal(invalid.statusCode, 422);

    const filtered = await handleWebhook(
      ev,
      makeCfg("ci/device-farm-lambda"),
      async () => SECRET,
      loadSample,
      async (m) => void queued.push(m),
    );
    assert.equal(filtered.statusCode, 200);
    assert.equal(queued.length, 0);
  });
});
