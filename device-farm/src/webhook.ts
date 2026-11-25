import { verify } from "@octokit/webhooks-methods";
import type { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2 } from "aws-lambda";
import {
  branchAllowed,
  type Config,
  findTargetByCheck,
  findTargetByJob,
  InvalidRepoConfigError,
  type JobMessage,
  lookupProjectArn,
  REPO_CONFIG_PATH,
  type RepoConfig,
} from "./config.js";

export type RepoConfigLoader = (
  fullName: string,
  defaultBranch: string,
  projectArn: string,
  installationId: number,
) => Promise<RepoConfig | null>;

const json = (
  statusCode: number,
  body: Record<string, string>,
): APIGatewayProxyStructuredResultV2 => ({
  statusCode,
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

function header(headers: Record<string, string | undefined> | undefined, name: string): string {
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(headers ?? {})) {
    if (k.toLowerCase() === lower) return v ?? "";
  }
  return "";
}

export async function handleWebhook(
  event: APIGatewayProxyEventV2,
  cfg: Config,
  getWebhookSecret: () => Promise<string>,
  loadRepoConfig: RepoConfigLoader,
  enqueue: (msg: JobMessage) => Promise<void>,
): Promise<APIGatewayProxyStructuredResultV2> {
  if (event.requestContext?.http?.method && event.requestContext.http.method !== "POST") {
    return json(405, { error: "method not allowed" });
  }

  const rawBody = event.isBase64Encoded
    ? Buffer.from(event.body ?? "", "base64").toString("utf8")
    : (event.body ?? "");

  const signature = header(event.headers, "x-hub-signature-256");
  if (!signature || !(await verify(await getWebhookSecret(), rawBody, signature))) {
    return json(401, { error: "invalid signature" });
  }

  const eventType = header(event.headers, "x-github-event");
  if (eventType === "ping") return json(200, { status: "pong" });

  let payload: Record<string, any>;
  try {
    payload = JSON.parse(rawBody);
  } catch {
    return json(400, { error: "invalid json" });
  }

  const fullName: string = payload.repository?.full_name ?? "";
  const projectArn = lookupProjectArn(cfg, fullName);
  if (!projectArn) return json(200, { status: "ignored", reason: "unconfigured repository" });

  const installationId: number = payload.installation?.id ?? 0;
  if (!installationId) return json(400, { error: "missing installation.id" });

  const action: string = payload.action ?? "";
  const relevant =
    (eventType === "workflow_job" && action === "completed") ||
    (eventType === "workflow_run" && action === "completed") ||
    (eventType === "check_run" && action === "rerequested");
  if (!relevant) return json(200, { status: "ignored" });

  const defaultBranch: string = payload.repository?.default_branch || "main";
  let repo: RepoConfig | null;
  try {
    repo = await loadRepoConfig(fullName, defaultBranch, projectArn, installationId);
  } catch (err) {
    if (err instanceof InvalidRepoConfigError) {
      console.error("invalid repo config", fullName, err.message);
      return json(422, { error: err.message });
    }
    throw err;
  }
  if (!repo) return json(200, { status: "ignored", reason: `missing ${REPO_CONFIG_PATH}` });

  let msg: JobMessage | null = null;
  if (eventType === "workflow_job") {
    const job = payload.workflow_job;
    const target = findTargetByJob(repo, job?.name);
    if (
      target &&
      (!job.workflow_name || job.workflow_name === repo.workflowName) &&
      job.run_id &&
      job.head_sha
    ) {
      msg = {
        event: "workflow_job",
        action: "completed",
        repo: repo.fullName,
        target: target.name,
        defaultBranch: repo.defaultBranch,
        installationId,
        headSha: job.head_sha,
        headBranch: job.head_branch || undefined,
        workflowRunId: job.run_id,
        workflowRunAttempt: job.run_attempt || 1,
        jobConclusion: job.conclusion || "",
      };
    }
  } else if (eventType === "workflow_run") {
    const run = payload.workflow_run;
    if (run?.name === repo.workflowName && run.id && run.head_sha) {
      msg = {
        event: "workflow_run",
        action: "completed",
        repo: repo.fullName,
        defaultBranch: repo.defaultBranch,
        installationId,
        headSha: run.head_sha,
        headBranch: run.head_branch || undefined,
        headRepo: run.head_repository?.full_name || undefined,
        workflowRunId: run.id,
        workflowRunAttempt: run.run_attempt || 1,
      };
    }
  } else if (eventType === "check_run") {
    const cr = payload.check_run;
    const target = findTargetByCheck(repo, cr?.name);
    if (target && cr.id && cr.head_sha) {
      msg = {
        event: "check_run",
        action: "rerequested",
        repo: repo.fullName,
        target: target.name,
        defaultBranch: repo.defaultBranch,
        installationId,
        headSha: cr.head_sha,
        headBranch: cr.check_suite?.head_branch || undefined,
        checkRunId: cr.id,
        externalId: cr.external_id || undefined,
      };
    }
  }

  if (!msg) return json(200, { status: "ignored" });
  if (msg.headBranch && !branchAllowed(cfg, msg.headBranch)) {
    return json(200, { status: "ignored", reason: "branch filtered" });
  }

  await enqueue(msg);
  return json(202, { status: "queued" });
}
