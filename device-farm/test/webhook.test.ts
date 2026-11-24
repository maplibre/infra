import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { sign } from "@octokit/webhooks-methods";
import type { APIGatewayProxyEventV2 } from "aws-lambda";
import { type JobMessage, loadConfig } from "../src/config.js";
import { handleWebhook } from "../src/webhook.js";

const SECRET = "test-webhook-secret";

function makeCfg(allowedBranches = "") {
  return loadConfig({
    MODE: "webhook",
    GITHUB_APP_SECRET_ARN: "arn:aws:secretsmanager:us-west-2:123:secret:gh",
    JOBS_QUEUE_URL: "https://sqs.us-west-2.amazonaws.com/123/jobs",
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
      enqueue,
    );
    assert.equal(bad.statusCode, 401);

    const pong = await handleWebhook(
      await makeEvent("ping", { zen: "hi" }),
      cfg,
      getSecret,
      enqueue,
    );
    assert.equal(pong.statusCode, 200);
    assert.equal(queued.length, 0);
  });

  it("enqueues workflow_job completed, workflow_run completed, and check_run rerequested", async () => {
    const queued: JobMessage[] = [];
    const cfg = makeCfg();
    const getSecret = async () => SECRET;
    const enqueue = async (m: JobMessage) => void queued.push(m);

    const inProg = await handleWebhook(
      await makeEvent("workflow_job", {
        action: "in_progress",
        repository: { full_name: "maplibre/mln-rs" },
        installation: { id: 99 },
        workflow_job: {
          name: "prepare-device-tests",
          workflow_name: "ci",
          run_id: 10,
          head_sha: "abc",
        },
      }),
      cfg,
      getSecret,
      enqueue,
    );
    assert.equal(inProg.statusCode, 200);
    assert.equal(queued.length, 0);

    const jobDone = await handleWebhook(
      await makeEvent("workflow_job", {
        action: "completed",
        repository: { full_name: "maplibre/mln-rs" },
        installation: { id: 99 },
        workflow_job: {
          name: "prepare-device-tests",
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
      enqueue,
    );
    assert.equal(jobDone.statusCode, 202);
    assert.equal(queued[0]?.workflowRunAttempt, 2);

    const rerun = await handleWebhook(
      await makeEvent("check_run", {
        action: "rerequested",
        repository: { full_name: "maplibre/mln-rs" },
        installation: { id: 99 },
        check_run: {
          id: 50,
          name: "device-test / android",
          head_sha: "abc123",
          external_id: "10:2",
        },
      }),
      cfg,
      getSecret,
      enqueue,
    );
    assert.equal(rerun.statusCode, 202);
    assert.equal(queued[1]?.externalId, "10:2");
  });

  it("filters branches when ALLOWED_BRANCHES is set", async () => {
    const queued: JobMessage[] = [];
    const res = await handleWebhook(
      await makeEvent("workflow_job", {
        action: "completed",
        repository: { full_name: "maplibre/mln-rs" },
        installation: { id: 99 },
        workflow_job: {
          name: "prepare-device-tests",
          workflow_name: "ci",
          run_id: 10,
          head_sha: "abc",
          head_branch: "other-branch",
          conclusion: "success",
        },
      }),
      makeCfg("ci/device-farm-lambda"),
      async () => SECRET,
      async (m) => void queued.push(m),
    );
    assert.equal(res.statusCode, 200);
    assert.equal(queued.length, 0);
  });
});
