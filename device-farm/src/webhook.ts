import { SendMessageCommand, type SQSClient } from "@aws-sdk/client-sqs";
import { verify } from "@octokit/webhooks-methods";
import type { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2 } from "aws-lambda";
import { branchAllowed, type Config, type JobMessage, lookupRepo } from "./config.js";

export interface WebhookDeps {
  cfg: Config;
  getWebhookSecret: () => Promise<string>;
  enqueue: (msg: JobMessage) => Promise<void>;
}

const jsonResp = (
  statusCode: number,
  body: Record<string, string>,
): APIGatewayProxyStructuredResultV2 => ({
  statusCode,
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

function header(headers: Record<string, string | undefined> | undefined, name: string): string {
  if (!headers) return "";
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(headers)) {
    if (k.toLowerCase() === lower) return v ?? "";
  }
  return "";
}

export function createSqsEnqueuer(sqs: SQSClient, queueUrl: string) {
  return async (msg: JobMessage): Promise<void> => {
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(msg),
      }),
    );
  };
}

export async function handleWebhook(
  event: APIGatewayProxyEventV2,
  deps: WebhookDeps,
): Promise<APIGatewayProxyStructuredResultV2> {
  if (event.requestContext?.http?.method && event.requestContext.http.method !== "POST") {
    return jsonResp(405, { error: "method not allowed" });
  }

  const rawBody = event.isBase64Encoded
    ? Buffer.from(event.body ?? "", "base64").toString("utf8")
    : (event.body ?? "");

  const signature = header(event.headers, "x-hub-signature-256");
  const secret = await deps.getWebhookSecret();
  if (!signature || !(await verify(secret, rawBody, signature))) {
    return jsonResp(401, { error: "invalid signature" });
  }

  const eventType = header(event.headers, "x-github-event");
  const deliveryId = header(event.headers, "x-github-delivery");
  if (eventType === "ping") return jsonResp(200, { status: "pong" });

  let payload: Record<string, any>;
  try {
    payload = JSON.parse(rawBody);
  } catch {
    return jsonResp(400, { error: "invalid json" });
  }

  const fullName: string = payload.repository?.full_name ?? "";
  const repo = lookupRepo(deps.cfg, fullName);
  if (!repo)
    return jsonResp(200, {
      status: "ignored",
      reason: "unconfigured repository",
    });

  const installationId: number = payload.installation?.id ?? 0;
  if (!installationId) return jsonResp(400, { error: "missing installation.id" });

  let msg: JobMessage | null = null;

  if (eventType === "workflow_job" && payload.action === "completed") {
    const job = payload.workflow_job;
    if (
      job?.name === repo.triggerJob &&
      (!job.workflow_name || job.workflow_name === repo.workflowName) &&
      job.run_id &&
      job.head_sha
    ) {
      msg = {
        event: "workflow_job",
        action: "completed",
        deliveryId,
        repo: repo.fullName,
        installationId,
        headSha: job.head_sha,
        headBranch: job.head_branch || undefined,
        workflowRunId: job.run_id,
        workflowRunAttempt: job.run_attempt || 1,
        jobConclusion: job.conclusion || "",
      };
    }
  } else if (eventType === "workflow_run" && payload.action === "completed") {
    const run = payload.workflow_run;
    if (run?.name === repo.workflowName && run.id && run.head_sha) {
      msg = {
        event: "workflow_run",
        action: "completed",
        deliveryId,
        repo: repo.fullName,
        installationId,
        headSha: run.head_sha,
        headBranch: run.head_branch || undefined,
        headRepo: run.head_repository?.full_name || undefined,
        workflowRunId: run.id,
        workflowRunAttempt: run.run_attempt || 1,
        workflowCreatedAt: run.created_at || undefined,
      };
    }
  } else if (eventType === "check_run" && payload.action === "rerequested") {
    const cr = payload.check_run;
    if (cr?.name === repo.checkName && cr.id && cr.head_sha) {
      msg = {
        event: "check_run",
        action: "rerequested",
        deliveryId,
        repo: repo.fullName,
        installationId,
        headSha: cr.head_sha,
        headBranch: cr.check_suite?.head_branch || undefined,
        checkRunId: cr.id,
        externalId: cr.external_id || undefined,
      };
    }
  }

  if (!msg) return jsonResp(200, { status: "ignored" });
  if (msg.headBranch && !branchAllowed(deps.cfg, msg.headBranch)) {
    return jsonResp(200, { status: "ignored", reason: "branch filtered" });
  }

  await deps.enqueue(msg);
  return jsonResp(202, { status: "queued" });
}
