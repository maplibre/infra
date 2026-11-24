import { GetSecretValueCommand, SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { App, type Octokit } from "octokit";

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

export interface JobMessage {
  event: "workflow_job" | "workflow_run" | "check_run";
  action: string;
  repo: string;
  installationId: number;
  headSha: string;
  headBranch?: string;
  headRepo?: string;
  workflowRunId?: number;
  workflowRunAttempt?: number;
  jobConclusion?: string;
  checkRunId?: number;
  externalId?: string;
}

export interface RunRecord {
  run_arn: string;
  state: "ACTIVE" | "COMPLETED";
  created_at: string;
  branch_key: string;
  repo_sha: string;
  repo: string;
  sha: string;
  head_repo: string;
  head_branch: string;
  check_run_id: number;
  installation_id: number;
  workflow_run_id: number;
  workflow_run_attempt: number;
  result?: string;
  completed_at?: string;
  expires_at: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const get = (k: string) => (env[k] ?? "").trim();
  const mode = get("MODE") as Mode;
  if (mode !== "webhook" && mode !== "worker" && mode !== "poller") {
    throw new Error(`Invalid or missing MODE "${mode}"`);
  }
  const githubAppSecretArn = get("GITHUB_APP_SECRET_ARN");
  if (!githubAppSecretArn) throw new Error("GITHUB_APP_SECRET_ARN is required");

  const mlnRs: RepoConfig = {
    fullName: "maplibre/mln-rs",
    owner: "maplibre",
    name: "mln-rs",
    defaultBranch: "main",
    projectArn: get("MLN_RS_PROJECT_ARN") || DEFAULT_MLN_RS_PROJECT_ARN,
    workflowName: "ci",
    triggerJob: "prepare-device-tests",
    artifactName: "android-device-tests",
    packageFilename: "android-tests.zip",
    testSpecPath: "tools/device-farm/android.yml",
    checkName: "device-test / android",
    jobTimeoutMinutes: 60,
    devices: [DEVICE_GOOGLE_PIXEL_9, DEVICE_SAMSUNG_GALAXY_S25],
  };

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
    repos: new Map([[mlnRs.fullName, mlnRs]]),
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
export const branchKey = (repo: string, headRepo: string, headBranch: string) =>
  `${repo.toLowerCase()}#${(headRepo || repo).toLowerCase()}#${headBranch}`;
export const repoShaKey = (repo: string, sha: string) => `${repo.toLowerCase()}#${sha}`;
export const formatExternalId = (runId: number, attempt: number) =>
  `${runId}:${Math.max(1, attempt)}`;

export function parseExternalId(raw?: string): { runId: number; attempt: number } | null {
  if (!raw) return null;
  const parts = raw.trim().split(":");
  if (parts.length < 1 || parts.length > 2) return null;
  const runId = Number(parts[0]);
  const attempt = parts.length === 2 ? Number(parts[1]) : 1;
  return Number.isSafeInteger(runId) && runId > 0 && Number.isSafeInteger(attempt) && attempt > 0
    ? { runId, attempt }
    : null;
}

export function createGitHubAuth(region: string, secretArn: string) {
  const sm = new SecretsManagerClient({ region });
  let cached: { secret: string; app: App; expiresAt: number } | null = null;

  async function load() {
    if (cached && Date.now() < cached.expiresAt) return cached;
    const out = await sm.send(new GetSecretValueCommand({ SecretId: secretArn }));
    const { app_id, private_key_pem, webhook_secret } = JSON.parse(out.SecretString ?? "{}");
    if (!app_id || !private_key_pem || !webhook_secret) {
      throw new Error("GitHub App secret must contain app_id, private_key_pem, and webhook_secret");
    }
    const app = new App({
      appId: String(app_id),
      privateKey: private_key_pem,
      webhooks: { secret: webhook_secret },
    });
    cached = { secret: webhook_secret, app, expiresAt: Date.now() + 5 * 60 * 1000 };
    return cached;
  }

  return {
    getWebhookSecret: async (): Promise<string> => (await load()).secret,
    getOctokit: async (installationId: number): Promise<Octokit> =>
      (await load()).app.getInstallationOctokit(installationId),
  };
}
