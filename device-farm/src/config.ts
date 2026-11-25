import { GetSecretValueCommand, SecretsManagerClient } from "@aws-sdk/client-secrets-manager";
import { App, type Octokit } from "octokit";
import { parse as parseToml } from "smol-toml";

export type Mode = "webhook" | "worker" | "poller";

export const DEFAULT_REGION = "us-west-2";
export const REPO_CONFIG_PATH = ".config/device-farm.toml";
export const DEFAULT_JOB_TIMEOUT_MINUTES = 60;
export const MAX_JOB_TIMEOUT_MINUTES = 90;
export const MAX_DEVICES = 10;

export class InvalidRepoConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidRepoConfigError";
  }
}

export type TestType = "APPIUM_WEB_NODE" | "APPIUM_NODE";
export type BillingMethod = "METERED" | "UNMETERED";
export const TEST_TYPES: readonly TestType[] = ["APPIUM_WEB_NODE", "APPIUM_NODE"];
export const BILLING_METHODS: readonly BillingMethod[] = ["METERED", "UNMETERED"];

/** One kind of Device Farm run: a CI job's package, its test spec, and its devices. */
export interface TargetConfig {
  name: string;
  triggerJob: string;
  artifactName: string;
  testType: TestType;
  packageFilename: string;
  /** An app that the run installs, for test types that test an app. */
  appFilename?: string;
  testSpecPath: string;
  checkName: string;
  jobTimeoutMinutes: number;
  devices: string[];
}

export interface RepoConfig {
  fullName: string;
  owner: string;
  name: string;
  defaultBranch: string;
  projectArn: string;
  workflowName: string;
  /** Whether pull requests' CI runs run on the devices; when not, their checks pass as not needed. */
  pullRequests: boolean;
  billingMethod: BillingMethod;
  targets: TargetConfig[];
}

export const findTargetByJob = (repo: RepoConfig, job?: string) =>
  repo.targets.find((t) => t.triggerJob === job);
export const findTargetByCheck = (repo: RepoConfig, check?: string) =>
  repo.targets.find((t) => t.checkName === check);
export const findTargetByName = (repo: RepoConfig, name?: string) =>
  repo.targets.find((t) => t.name === name);

export interface Config {
  mode: Mode;
  region: string;
  githubAppSecretArn: string;
  jobsQueueUrl: string;
  runsTableName: string;
  resultsBucketName: string;
  resultsBaseUrl: string;
  allowedBranches: Set<string>;
  repoProjects: Map<string, string>;
}

export interface JobMessage {
  event: "workflow_job" | "workflow_run" | "check_run";
  action: string;
  repo: string;
  /** The target that a workflow_job or check_run event is for; a workflow_run is for every target. */
  target?: string;
  defaultBranch: string;
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
  target?: string;
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

function parseRepoProjects(raw: string): Map<string, string> {
  if (!raw) return new Map();
  const parsed = JSON.parse(raw) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("REPO_PROJECTS must be a JSON object mapping owner/repo to project ARN");
  }
  const map = new Map<string, string>();
  for (const [repo, arn] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof arn !== "string" || !arn.trim()) {
      throw new Error(`REPO_PROJECTS["${repo}"] must be a non-empty project ARN`);
    }
    map.set(repo.trim().toLowerCase(), arn.trim());
  }
  return map;
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
    repoProjects: parseRepoProjects(get("REPO_PROJECTS")),
  };

  if (mode === "webhook" && !cfg.jobsQueueUrl) throw new Error("JOBS_QUEUE_URL is required");
  if ((mode === "webhook" || mode === "worker") && cfg.repoProjects.size === 0) {
    throw new Error("REPO_PROJECTS must configure at least one repository");
  }
  if ((mode === "worker" || mode === "poller") && !cfg.runsTableName) {
    throw new Error("RUNS_TABLE_NAME is required");
  }
  if (mode === "poller" && (!cfg.resultsBucketName || !cfg.resultsBaseUrl)) {
    throw new Error("RESULTS_BUCKET_NAME and RESULTS_BASE_URL are required");
  }
  return cfg;
}

export const lookupProjectArn = (cfg: Config, fullName: string) =>
  cfg.repoProjects.get(fullName.trim().toLowerCase());
export const branchAllowed = (cfg: Config, branch?: string) =>
  cfg.allowedBranches.size === 0 || (Boolean(branch) && cfg.allowedBranches.has(branch!));
export const branchKey = (repo: string, target: string, headRepo: string, headBranch: string) =>
  `${repo.toLowerCase()}#${target}#${(headRepo || repo).toLowerCase()}#${headBranch}`;
export const repoShaKey = (repo: string, sha: string) => `${repo.toLowerCase()}#${sha}`;
export const formatExternalId = (runId: number, attempt: number) =>
  `${runId}:${Math.max(1, attempt)}`;

export function splitRepo(fullName: string): { owner: string; name: string } | null {
  const parts = fullName.trim().split("/");
  return parts.length === 2 && parts[0] && parts[1] ? { owner: parts[0], name: parts[1] } : null;
}

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

const TARGET_NAME = /^[a-z0-9][a-z0-9_-]*$/;

function plainFilename(value: string, where: string) {
  if (value.includes("/") || value.includes("\\") || value === "." || value === "..") {
    throw new InvalidRepoConfigError(`${where} must be a plain filename without path separators`);
  }
  return value;
}

function parseTarget(name: string, raw: unknown): TargetConfig {
  const where = `${REPO_CONFIG_PATH}: targets.${name}`;
  if (!TARGET_NAME.test(name)) {
    throw new InvalidRepoConfigError(
      `${REPO_CONFIG_PATH}: target name "${name}" must be lowercase letters, digits, "-" or "_"`,
    );
  }
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new InvalidRepoConfigError(`${where} must be a table`);
  }
  const doc = raw as Record<string, unknown>;
  const reqStr = (key: string) => {
    const v = doc[key];
    if (typeof v !== "string" || !v.trim()) {
      throw new InvalidRepoConfigError(`${where}: "${key}" must be a non-empty string`);
    }
    return v.trim();
  };

  const timeout =
    doc.job_timeout_minutes === undefined ? DEFAULT_JOB_TIMEOUT_MINUTES : doc.job_timeout_minutes;
  if (
    typeof timeout !== "number" ||
    !Number.isInteger(timeout) ||
    timeout < 1 ||
    timeout > MAX_JOB_TIMEOUT_MINUTES
  ) {
    throw new InvalidRepoConfigError(
      `${where}: "job_timeout_minutes" must be an integer between 1 and ${MAX_JOB_TIMEOUT_MINUTES}`,
    );
  }

  const rawDevices = doc.devices;
  if (!Array.isArray(rawDevices) || rawDevices.length === 0 || rawDevices.length > MAX_DEVICES) {
    throw new InvalidRepoConfigError(
      `${where}: "devices" must be an array of 1 to ${MAX_DEVICES} Device Farm ARNs`,
    );
  }
  const devices = rawDevices.map((d) => {
    if (typeof d !== "string" || !d.trim().startsWith("arn:aws:devicefarm:")) {
      throw new InvalidRepoConfigError(
        `${where}: every entry in "devices" must be an arn:aws:devicefarm: ARN`,
      );
    }
    return d.trim();
  });

  const testType = reqStr("test_type") as TestType;
  if (!TEST_TYPES.includes(testType)) {
    throw new InvalidRepoConfigError(
      `${where}: "test_type" must be one of ${TEST_TYPES.join(", ")}`,
    );
  }

  let appFilename: string | undefined;
  if (doc.app_filename !== undefined) {
    appFilename = plainFilename(reqStr("app_filename"), `${where}: "app_filename"`);
    if (!/\.(apk|ipa)$/.test(appFilename)) {
      throw new InvalidRepoConfigError(`${where}: "app_filename" must name an .apk or .ipa file`);
    }
  }
  if ((testType === "APPIUM_NODE") !== Boolean(appFilename)) {
    throw new InvalidRepoConfigError(
      `${where}: "app_filename" is required for APPIUM_NODE and not allowed for APPIUM_WEB_NODE`,
    );
  }

  return {
    name,
    triggerJob: reqStr("trigger_job"),
    artifactName: reqStr("artifact_name"),
    testType,
    packageFilename: plainFilename(reqStr("package_filename"), `${where}: "package_filename"`),
    appFilename,
    testSpecPath: reqStr("test_spec_path"),
    checkName: reqStr("check_name"),
    jobTimeoutMinutes: timeout,
    devices,
  };
}

export function parseRepoConfig(
  rawToml: string,
  meta: { fullName: string; defaultBranch: string; projectArn: string },
): RepoConfig {
  const parts = splitRepo(meta.fullName);
  if (!parts) throw new InvalidRepoConfigError(`Invalid repository name "${meta.fullName}"`);

  let doc: Record<string, unknown>;
  try {
    doc = parseToml(rawToml) as Record<string, unknown>;
  } catch (err) {
    throw new InvalidRepoConfigError(
      `Failed to parse ${REPO_CONFIG_PATH}: ${(err as Error).message}`,
    );
  }

  const workflowName = doc.workflow_name;
  if (typeof workflowName !== "string" || !workflowName.trim()) {
    throw new InvalidRepoConfigError(
      `${REPO_CONFIG_PATH}: "workflow_name" must be a non-empty string`,
    );
  }

  const pullRequests = doc.pull_requests ?? true;
  if (typeof pullRequests !== "boolean") {
    throw new InvalidRepoConfigError(`${REPO_CONFIG_PATH}: "pull_requests" must be a boolean`);
  }

  const billingMethod = (doc.billing_method ?? "METERED") as BillingMethod;
  if (!BILLING_METHODS.includes(billingMethod)) {
    throw new InvalidRepoConfigError(
      `${REPO_CONFIG_PATH}: "billing_method" must be one of ${BILLING_METHODS.join(", ")}`,
    );
  }

  const rawTargets = doc.targets;
  if (!rawTargets || typeof rawTargets !== "object" || Array.isArray(rawTargets)) {
    throw new InvalidRepoConfigError(
      `${REPO_CONFIG_PATH}: "targets" must be a table of at least one target`,
    );
  }
  const targets = Object.entries(rawTargets).map(([name, raw]) => parseTarget(name, raw));
  if (targets.length === 0) {
    throw new InvalidRepoConfigError(
      `${REPO_CONFIG_PATH}: "targets" must have at least one target`,
    );
  }
  for (const key of ["triggerJob", "checkName"] as const) {
    if (new Set(targets.map((t) => t[key])).size !== targets.length) {
      throw new InvalidRepoConfigError(
        `${REPO_CONFIG_PATH}: every target needs its own ${key === "triggerJob" ? "trigger_job" : "check_name"}`,
      );
    }
  }

  return {
    fullName: meta.fullName,
    owner: parts.owner,
    name: parts.name,
    defaultBranch: meta.defaultBranch || "main",
    projectArn: meta.projectArn,
    workflowName: workflowName.trim(),
    pullRequests,
    billingMethod,
    targets,
  };
}

export async function fetchRepoConfig(
  gh: Octokit,
  fullName: string,
  defaultBranch: string,
  projectArn: string,
): Promise<RepoConfig | null> {
  const parts = splitRepo(fullName);
  if (!parts) return null;
  try {
    const { data } = await gh.rest.repos.getContent({
      owner: parts.owner,
      repo: parts.name,
      path: REPO_CONFIG_PATH,
      ref: defaultBranch || "main",
      mediaType: { format: "raw" },
    });
    const raw =
      typeof data === "string" ? data : Buffer.from(data as unknown as Uint8Array).toString("utf8");
    return parseRepoConfig(raw, { fullName, defaultBranch: defaultBranch || "main", projectArn });
  } catch (err) {
    if ((err as { status?: number })?.status === 404) return null;
    throw err;
  }
}

export function createCachedRepoConfigLoader(
  getOctokit: (installationId: number) => Promise<Octokit>,
  ttlMs = 60 * 1000,
) {
  const cache = new Map<string, { config: RepoConfig; expiresAt: number }>();
  return async (
    fullName: string,
    defaultBranch: string,
    projectArn: string,
    installationId: number,
  ): Promise<RepoConfig | null> => {
    const key = `${fullName.toLowerCase()}@${defaultBranch || "main"}`;
    const hit = cache.get(key);
    if (hit && Date.now() < hit.expiresAt) return hit.config;
    const config = await fetchRepoConfig(
      await getOctokit(installationId),
      fullName,
      defaultBranch,
      projectArn,
    );
    if (config) cache.set(key, { config, expiresAt: Date.now() + ttlMs });
    return config;
  };
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
