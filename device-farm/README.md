# AWS Device Farm Orchestrator

A serverless TypeScript Lambda orchestrator in `us-west-2` that bridges GitHub Actions CI
builds to AWS Device Farm hardware runs and reports each one's results back as a
GitHub Check Run (such as `device-test / android`). A repository configures one or
more targets, each a CI job's test package and the devices it runs on. The OpenTofu stack lives in
[`../opentofu/device-farm/`](../opentofu/README.md).

## Architecture

One esbuild-bundled Node.js 22 ESM bundle (`nodejs22.x`, `arm64`, handler `index.handler`)
runs in three modes selected by the `MODE` environment variable:

1. **`webhook`** (`128 MB`, `15s` timeout, Lambda Function URL): validates
   `X-Hub-Signature-256` against the GitHub App webhook secret from Secrets Manager,
   checks the repository against `REPO_PROJECTS`, loads `.config/device-farm.toml`
   from the repository's default branch (cached for 60s) for relevant events
   (`workflow_job` `completed`, `workflow_run` `completed`, `check_run`
   `rerequested`), enqueues a compact JSON job onto SQS, and returns `202 Accepted`.
2. **`worker`** (`1769 MB`, `5120 MB /tmp`, `900s` timeout, SQS trigger with
   `batch_size = 1` and `ReportBatchItemFailures`): authenticates as the GitHub App
   installation, loads `.config/device-farm.toml` from the repository's default
   branch, passes a pull request's check as not needed when the repository runs
   only its default branch's commits, deduplicates against existing DynamoDB runs,
   stops any earlier `ACTIVE` runs of the target on the same
   `(repo, head_repo, head_branch)` other than the default branch, downloads the
   target's Actions artifact without leaking the GitHub `Authorization` header to
   Azure Blob Storage, extracts the test package (and app, if any) without
   unpacking or executing their contents, fetches the test spec from the default
   branch (never from a PR branch), uploads them to Device Farm, schedules the run
   with the repository's billing method, and records it in DynamoDB.
3. **`poller`** (`512 MB`, `1024 MB /tmp`, `120s` timeout, EventBridge `rate(1 minute)`):
   queries DynamoDB `state-index` for `ACTIVE` runs (exiting immediately when idle),
   checks `devicefarm:GetRun`, downloads `TESTSPEC_OUTPUT` and `CUSTOMER_ARTIFACT`
   for completed runs, safely parses `junit.xml` (bounded read, falling back to the
   last 40 lines of `TESTSPEC_OUTPUT.txt`), uploads artifacts to the private S3
   bucket with `Content-Disposition: attachment`, and completes the GitHub Check Run
   with a Markdown summary table and sandboxed CloudFront links.

### Security properties

- Untrusted fork PR code is never executed inside Lambda. The test package is
  transferred as an opaque file straight to Device Farm, and both
  `.config/device-farm.toml` and the test spec are always fetched from the
  repository's default branch (never from a PR branch).
- `infra` owns the repository allowlist and AWS Device Farm project ARN mapping
  (`repo_projects` in OpenTofu), plus guardrails on device count (max 10) and job
  timeout (max 90 minutes).
- Test outputs live in a private S3 bucket (`BlockPublicAccess` enabled, 30-day
  expiration) fronted by CloudFront Origin Access Control (OAC) with a response
  headers policy enforcing `Content-Security-Policy: sandbox` and
  `X-Content-Type-Options: nosniff`.
- Each Lambda has a dedicated least-privilege IAM role scoped to its own log group
  and only the AWS resources and API actions its mode needs.

## First-time setup

### 1. Create the GitHub App

Create an organization GitHub App (e.g. **MapLibre Device Farm**) under `maplibre`:

- **Webhook**: initially set a placeholder URL and generate a webhook secret with
  `openssl rand -hex 32`.
- **Repository permissions**:
  - `Actions`: Read-only
  - `Checks`: Read and write
  - `Contents`: Read-only
  - `Metadata`: Read-only
- **Subscribe to events**:
  - `Check run`
  - `Workflow job`
  - `Workflow run`
- Generate and download a **Private key** (`.pem`) and note the **App ID**.
- Install the App on `maplibre/mln-rs`.

### 2. Build and deploy the AWS stack

From the repository root (with AWS credentials targeting account `373521797162`):

```sh
just device-farm::install
just device-farm::init
just device-farm::build
just device-farm::configure   # optional: edit opentofu/device-farm/terraform.tfvars
just device-farm::plan
just device-farm::apply
```

Print the outputs:

```sh
just device-farm::outputs
```

### 3. Populate Secrets Manager

OpenTofu creates an empty `maplibre-device-farm/github-app` secret shell so no
credentials ever enter OpenTofu state. Populate it once out-of-band:

```sh
SECRET_ARN=$(tofu -chdir=opentofu/device-farm output -raw github_app_secret_arn)
jq -n \
  --arg app_id "123456" \
  --arg webhook_secret "YOUR_WEBHOOK_SECRET" \
  --rawfile private_key_pem /path/to/maplibre-device-farm.private-key.pem \
  '{app_id: $app_id, private_key_pem: $private_key_pem, webhook_secret: $webhook_secret}' |
  aws secretsmanager put-secret-value \
    --region us-west-2 \
    --secret-id "$SECRET_ARN" \
    --secret-string file:///dev/stdin
```

### 4. Point the GitHub App webhook at Lambda

Set the GitHub App's **Webhook URL** to the output of:

```sh
just device-farm::webhook-url
```

GitHub sends a `ping` delivery automatically; confirm it receives `200 OK`.

## Evaluating on a single PR branch before cutover

To validate the orchestrator on a single branch (such as `ci/device-farm-lambda`)
without affecting other PRs or `main`, set `allowed_branches` in
`opentofu/device-farm/terraform.tfvars` (gitignored):

```hcl
allowed_branches = "ci/device-farm-lambda"
```

Run `just device-farm::plan` and `just device-farm::apply`. While `allowed_branches`
is non-empty, events whose head branch is not in the comma-separated list are ignored
with `200 OK`.

Once verified, cut over all branches by clearing `allowed_branches`:

```hcl
allowed_branches = ""
```

Then run `just device-farm::plan` and `just device-farm::apply`, and add the
targets' check names to branch protection when ready.

## Repository configuration (`.config/device-farm.toml`)

Each onboarded repository provides its own `.config/device-farm.toml` on its
default branch (always read from the default branch, never from a PR branch):

```toml
workflow_name = "ci"
# Optional (default true). When false, only pushes run on the devices, and pull
# requests' checks pass as not needed.
pull_requests = false
# Optional (default "METERED"). "UNMETERED" runs on the account's flat-rate devices.
billing_method = "UNMETERED"

# Each target is a job of the workflow whose artifact holds a test package.
[targets.android]
trigger_job = "prepare-android-device-tests"
artifact_name = "android-device-tests"
check_name = "device-test / android"
# APPIUM_WEB_NODE needs no app; APPIUM_NODE installs app_filename (.apk or .ipa).
test_type = "APPIUM_WEB_NODE"
package_filename = "tests.zip"
test_spec_path = "tools/device-farm/android/test-spec.yml"
job_timeout_minutes = 60 # optional, at most 90
devices = [
  "arn:aws:devicefarm:us-west-2::device:6D89F408709B4D3BBBBB17696A24B141", # Google Pixel 9
]

[targets.ios]
trigger_job = "prepare-ios-device-tests"
artifact_name = "ios-device-tests"
check_name = "device-test / ios"
test_type = "APPIUM_NODE"
package_filename = "tests.zip"
app_filename = "app.ipa"
test_spec_path = "tools/device-farm/ios/test-spec.yml"
devices = [
  "arn:aws:devicefarm:us-west-2::device:6200F380A4874FEB9C72EED72B863B67", # Apple iPhone 17 Pro
]
```

The package and app are files at the root of the target's artifact.

## Adding another repository (e.g. `maplibre/maplibre-native`)

1. Add the repository's `"owner/repo" = "<device-farm-project-arn>"` entry to
   `repo_projects` in [`../opentofu/device-farm/variables.tf`](../opentofu/device-farm/variables.tf).
2. Add `.config/device-farm.toml` (and the referenced test spec) to the target
   repository's default branch.
3. Install the GitHub App on the new repository.
4. Run `just device-farm::plan` and `just device-farm::apply`.
