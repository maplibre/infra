import { DeviceFarmClient } from "@aws-sdk/client-device-farm";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { S3Client } from "@aws-sdk/client-s3";
import { SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { SQSClient } from "@aws-sdk/client-sqs";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import type { APIGatewayProxyEventV2, SQSEvent } from "aws-lambda";
import { createCredentialsLoader, createOctokitApp, loadConfig } from "./config.js";
import { handlePollerEvent } from "./poller.js";
import { DynamoRunStore } from "./store.js";
import { createSqsEnqueuer, handleWebhook } from "./webhook.js";
import { handleWorkerEvent } from "./worker.js";

const cfg = loadConfig();
const getCreds = createCredentialsLoader(
  new SecretsManagerClient({ region: cfg.region }),
  cfg.githubAppSecretArn,
);

let cachedApp: {
  key: string;
  app: ReturnType<typeof createOctokitApp>;
} | null = null;
async function getOctokit(installationId: number) {
  const creds = await getCreds();
  const key = `${creds.app_id}:${creds.private_key_pem.length}`;
  if (!cachedApp || cachedApp.key !== key) cachedApp = { key, app: createOctokitApp(creds) };
  return cachedApp.app.getInstallationOctokit(installationId);
}

const makeStore = () =>
  new DynamoRunStore(
    DynamoDBDocumentClient.from(new DynamoDBClient({ region: cfg.region }), {
      marshallOptions: { removeUndefinedValues: true },
    }),
    cfg.runsTableName,
  );

const webhookEnqueue =
  cfg.mode === "webhook"
    ? createSqsEnqueuer(new SQSClient({ region: cfg.region }), cfg.jobsQueueUrl)
    : null;
const store = cfg.mode !== "webhook" ? makeStore() : null;
const df = cfg.mode !== "webhook" ? new DeviceFarmClient({ region: cfg.region }) : null;
const s3 = cfg.mode === "poller" ? new S3Client({ region: cfg.region }) : null;

export async function handler(event: unknown): Promise<unknown> {
  if (cfg.mode === "webhook") {
    return handleWebhook(event as APIGatewayProxyEventV2, {
      cfg,
      getWebhookSecret: async () => (await getCreds()).webhook_secret,
      enqueue: webhookEnqueue!,
    });
  }
  if (cfg.mode === "worker") {
    return handleWorkerEvent(event as SQSEvent, {
      cfg,
      getOctokit,
      store: store!,
      df: df!,
    });
  }
  return handlePollerEvent({
    cfg,
    store: store!,
    df: df!,
    s3: s3!,
    getOctokit,
  });
}
