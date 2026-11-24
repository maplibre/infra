import { GetSecretValueCommand, type SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { App } from "octokit";

export type Mode = "webhook" | "worker" | "poller";

export const DEFAULT_REGION = "us-west-2";
export const DEFAULT_MLN_RS_PROJECT_ARN =
  "arn:aws:devicefarm:us-west-2:373521797162:project:c5c3f5f6-e21b-41c4-a235-489d30075aeb";
export const DEVICE_GOOGLE_PIXEL_9 =
  "arn:aws:devicefarm:us-west-2::device:6D89F408709B4D3BBBBB17696A24B141";
export const DEVICE_SAMSUNG_GALAXY_S25 =
  "arn:aws:devicefarm:us-west-2::device:092D78E7B4694568812D5FF0711CD346";

export interface RepoConfig {
  fullName: string;
  owner: string;
  name: string;
  defaultBranch: string;
  projectArn: string;
  workflowName: string;
  triggerJob: string;
  artifactName: string;
  packageFilename: string;
  testSpecPath: string;
  checkName: string;
  jobTimeoutMinutes: number;
  devices: string[];
}

export interface Config {
  mode: Mode;
  region: string;
  githubAppSecretArn: string;
  jobsQueueUrl: string;
  runsTableName: string;
  resultsBucketName: string;
  resultsBaseUrl: string;
  allowedBranches: Set<string>;
  repos: Map<string, RepoConfig>;
}

export interface AppCredentials {
  app_id: string;
  private_key_pem: string;
  webhook_secret: string;
}

export interface JobMessage {
  event: "workflow_job" | "workflow_run" | "check_run";
  action: string;
  deliveryId?: string;
  repo: string;
  installationId: number;
  headSha: string;
  headBranch?: string;
  headRepo?: string;
  workflowRunId?: number;
  workflowRunAttempt?: number;
  workflowCreatedAt?: string;
  jobConclusion?: string;
  checkRunId?: number;
  externalId?: string;
}

export interface RunRecord {
  run_arn: string;
  state: "CLAIMING" | "CLAIMED" | "ACTIVE" | "COMPLETED";
  created_at: string;
  branch_key?: string;
  repo_sha?: string;
  repo?: string;
  sha?: string;
  head_repo?: string;
  head_branch?: string;
  check_run_id?: number;
  installation_id?: number;
  workflow_run_id?: number;
  workflow_run_attempt?: number;
  workflow_created_at?: string;
  rerun_delivery_id?: string;
  claim_owner?: string;
  lease_expires_at?: number;
  result?: string;
  completed_at?: string;
  expires_at: number;
}

export function defaultRepos(
  mlnRsProjectArn = DEFAULT_MLN_RS_PROJECT_ARN,
): Map<string, RepoConfig> {
  const repos: RepoConfig[] = [
    {
      fullName: "maplibre/mln-rs",
      owner: "maplibre",
      name: "mln-rs",
      defaultBranch: "main",
      projectArn: mlnRsProjectArn,
      workflowName: "ci",
      triggerJob: "prepare-device-tests",
      artifactName: "android-device-tests",
      packageFilename: "android-tests.zip",
      testSpecPath: "tools/device-farm/android.yml",
      checkName: "device-test / android",
      jobTimeoutMinutes: 60,
      devices: [DEVICE_GOOGLE_PIXEL_9, DEVICE_SAMSUNG_GALAXY_S25],
    },
  ];
  return new Map(repos.map((r) => [r.fullName.toLowerCase(), r]));
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const get = (k: string) => (env[k] ?? "").trim();
  const mode = get("MODE") as Mode;
  if (mode !== "webhook" && mode !== "worker" && mode !== "poller") {
    throw new Error(`Invalid or missing MODE "${mode}"`);
  }
  const githubAppSecretArn = get("GITHUB_APP_SECRET_ARN");
  if (!githubAppSecretArn) throw new Error("GITHUB_APP_SECRET_ARN is required");

  const cfg: Config = {
    mode,
    region: get("AWS_REGION") || DEFAULT_REGION,
    githubAppSecretArn,
    jobsQueueUrl: get("JOBS_QUEUE_URL"),
    runsTableName: get("RUNS_TABLE_NAME"),
    resultsBucketName: get("RESULTS_BUCKET_NAME"),
    resultsBaseUrl: get("RESULTS_BASE_URL").replace(/\/+$/, ""),
    allowedBranches: new Set(
      get("ALLOWED_BRANCHES")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean),
    ),
    repos: defaultRepos(get("MLN_RS_PROJECT_ARN") || DEFAULT_MLN_RS_PROJECT_ARN),
  };

  if (mode === "webhook" && !cfg.jobsQueueUrl) throw new Error("JOBS_QUEUE_URL is required");
  if ((mode === "worker" || mode === "poller") && !cfg.runsTableName) {
    throw new Error("RUNS_TABLE_NAME is required");
  }
  if (mode === "poller" && (!cfg.resultsBucketName || !cfg.resultsBaseUrl)) {
    throw new Error("RESULTS_BUCKET_NAME and RESULTS_BASE_URL are required");
  }
  return cfg;
}

export const lookupRepo = (cfg: Config, fullName: string) =>
  cfg.repos.get(fullName.trim().toLowerCase());
export const branchAllowed = (cfg: Config, branch?: string) =>
  cfg.allowedBranches.size === 0 || (Boolean(branch) && cfg.allowedBranches.has(branch!));
export const isDeviceFarmArn = (arn: string) => arn.startsWith("arn:aws:devicefarm:");
export const claimKey = (repo: string, runId: number, attempt: number) =>
  `claim#${repo.toLowerCase()}#${runId}#${Math.max(1, attempt)}`;
export const rerunClaimKey = (repo: string, deliveryId: string) =>
  `claim-rerun#${repo.toLowerCase()}#${deliveryId}`;
export const branchKey = (repo: string, headRepo: string, headBranch: string) =>
  `${repo.toLowerCase()}#${(headRepo || repo).toLowerCase()}#${headBranch}`;
export const repoShaKey = (repo: string, sha: string) => `${repo.toLowerCase()}#${sha}`;

export const formatExternalId = (runId: number, attempt: number, deliveryId?: string) =>
  deliveryId
    ? `${runId}:${Math.max(1, attempt)}:${deliveryId}`
    : `${runId}:${Math.max(1, attempt)}`;

export function parseExternalId(raw?: string): { runId: number; attempt: number } | null {
  if (!raw) return null;
  const parts = raw.trim().split(":");
  if (parts.length < 1 || parts.length > 3) return null;
  const runId = Number(parts[0]);
  if (!Number.isSafeInteger(runId) || runId <= 0) return null;
  if (parts.length === 1) return { runId, attempt: 1 };
  const attempt = Number(parts[1]);
  if (!Number.isSafeInteger(attempt) || attempt <= 0) return null;
  if (parts.length === 3 && !parts[2]) return null;
  return { runId, attempt };
}

export function createCredentialsLoader(
  sm: Pick<SecretsManagerClient, "send">,
  secretArn: string,
  ttlMs = 5 * 60 * 1000,
): () => Promise<AppCredentials> {
  let cached: { creds: AppCredentials; expiresAt: number } | null = null;
  return async () => {
    if (cached && Date.now() < cached.expiresAt) return cached.creds;
    const out = await sm.send(new GetSecretValueCommand({ SecretId: secretArn }));
    const parsed = JSON.parse(out.SecretString ?? "{}") as Partial<AppCredentials>;
    if (!parsed.app_id || !parsed.private_key_pem || !parsed.webhook_secret) {
      throw new Error("GitHub App secret must contain app_id, private_key_pem, and webhook_secret");
    }
    const creds = {
      app_id: String(parsed.app_id),
      private_key_pem: parsed.private_key_pem,
      webhook_secret: parsed.webhook_secret,
    };
    cached = { creds, expiresAt: Date.now() + ttlMs };
    return creds;
  };
}

export const createOctokitApp = (creds: AppCredentials) =>
  new App({
    appId: creds.app_id,
    privateKey: creds.private_key_pem,
    webhooks: { secret: creds.webhook_secret },
  });
