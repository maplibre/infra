import { DeviceFarmClient } from "@aws-sdk/client-device-farm";
import { S3Client } from "@aws-sdk/client-s3";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import type { APIGatewayProxyEventV2, SQSEvent } from "aws-lambda";
import {
  createCachedRepoConfigLoader,
  createGitHubAuth,
  type JobMessage,
  loadConfig,
} from "./config.js";
import { handlePollerEvent } from "./poller.js";
import { RunStore } from "./store.js";
import { handleWebhook } from "./webhook.js";
import { handleWorkerEvent } from "./worker.js";

const cfg = loadConfig();
const auth = createGitHubAuth(cfg.region, cfg.githubAppSecretArn);
const loadRepoConfig =
  cfg.mode === "webhook" ? createCachedRepoConfigLoader(auth.getOctokit) : null;
const sqs = cfg.mode === "webhook" ? new SQSClient({ region: cfg.region }) : null;
const store = cfg.mode !== "webhook" ? new RunStore(cfg.region, cfg.runsTableName) : null;
const df = cfg.mode !== "webhook" ? new DeviceFarmClient({ region: cfg.region }) : null;
const s3 = cfg.mode === "poller" ? new S3Client({ region: cfg.region }) : null;

const enqueue = async (msg: JobMessage) => {
  await sqs!.send(
    new SendMessageCommand({ QueueUrl: cfg.jobsQueueUrl, MessageBody: JSON.stringify(msg) }),
  );
};

export async function handler(event: unknown): Promise<unknown> {
  if (cfg.mode === "webhook") {
    return handleWebhook(
      event as APIGatewayProxyEventV2,
      cfg,
      auth.getWebhookSecret,
      loadRepoConfig!,
      enqueue,
    );
  }
  if (cfg.mode === "worker") {
    return handleWorkerEvent(event as SQSEvent, cfg, auth.getOctokit, store!, df!);
  }
  return handlePollerEvent(cfg, store!, df!, s3!, auth.getOctokit);
}
