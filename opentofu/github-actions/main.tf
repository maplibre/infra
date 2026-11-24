terraform {
  required_version = ">= 1.11, < 2.0"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 6.0" }
    github = { source = "integrations/github", version = "~> 6.0" }
  }
  backend "s3" {
    bucket       = "maplibre-ci-runners-tofu-state-373521797162"
    key          = "github-actions/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}
provider "aws" {
  region              = "us-east-1"
  allowed_account_ids = ["373521797162"]
  default_tags { tags = { Project = "maplibre-github-actions", ManagedBy = "OpenTofu" } }
}
# Device Farm's only region.
provider "aws" {
  alias               = "device_farm"
  region              = "us-west-2"
  allowed_account_ids = ["373521797162"]
  default_tags { tags = { Project = "maplibre-github-actions", ManagedBy = "OpenTofu" } }
}
# Authenticates with GITHUB_TOKEN, which needs admin access to the repositories
# in local.workflow_run_repositories.
provider "github" {
  owner = "maplibre"
}
data "github_organization" "maplibre" {
  name         = "maplibre"
  summary_only = true
}
data "github_repository" "workflow_run" {
  for_each  = local.workflow_run_repositories
  full_name = "maplibre/${each.key}"
}
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}
locals {
  issuer = "token.actions.githubusercontent.com"
  # The repositories whose workflow-run environment gets the narrower role, and
  # whether GitHub gives them immutable OIDC subjects (a repository's setting),
  # which name the owner and the repository by ID too: repo:OWNER@ID/REPO@ID.
  workflow_run_repositories = { "maplibre-native" = false, "mln-rs" = true }
  workflow_run_subjects = { for repository, environment in github_repository_environment.workflow_run :
    repository => join(":", [
      local.workflow_run_repositories[repository]
      ? "repo:maplibre@${data.github_organization.maplibre.id}/${repository}@${data.github_repository.workflow_run[repository].repo_id}"
      : "repo:maplibre/${repository}",
      "environment", environment.environment,
  ]) }
  # maplibre-native's project, which this stack doesn't manage, and mln-rs's.
  device_farm_projects = ["20687d72-0e46-403e-8f03-0941850665bc", split(":", aws_devicefarm_project.mln_rs.arn)[6]]
}
import {
  to = aws_iam_role.github_actions
  id = "GitHubActionsOpenID"
}
import {
  to = aws_iam_role_policy_attachments_exclusive.github_actions
  id = "GitHubActionsOpenID"
}
resource "aws_iam_role" "github_actions" {
  name                 = "GitHubActionsOpenID"
  max_session_duration = 14400
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect    = "Allow", Action = "sts:AssumeRoleWithWebIdentity"
    Principal = { Federated = data.aws_iam_openid_connect_provider.github.arn }
    Condition = {
      StringEquals = { "${local.issuer}:aud" = "sts.amazonaws.com" }
      StringLike   = { "${local.issuer}:sub" = ["repo:maplibre/maplibre-native:*", "repo:maplibre/maplibre-native-qt:*"] }
      # Jobs in the workflow-run environment must only get the narrower role.
      StringNotEquals = { "${local.issuer}:sub" = local.workflow_run_subjects["maplibre-native"] }
    }
  }] })
}
# The customer-managed policies are shared with other principals, so this stack
# attaches them without managing their documents.
resource "aws_iam_role_policy_attachments_exclusive" "github_actions" {
  role_name = aws_iam_role.github_actions.name
  policy_arns = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/AWSDeviceFarmFullAccess",
    "arn:aws:iam::373521797162:policy/github-actions-s3-buckets",
  ]
}
# The workflow_run jobs of local.workflow_run_repositories handle artifacts
# that fork pull requests control, so they can only publish maplibre-native's
# results and run device tests.
resource "aws_iam_role" "workflow_run" {
  name                 = "GitHubActionsWorkflowRun"
  max_session_duration = 14400 # device tests request four-hour sessions
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect    = "Allow", Action = "sts:AssumeRoleWithWebIdentity"
    Principal = { Federated = data.aws_iam_openid_connect_provider.github.arn }
    Condition = { StringEquals = {
      "${local.issuer}:aud" = "sts.amazonaws.com"
      "${local.issuer}:sub" = values(local.workflow_run_subjects)
    } }
  }] })
}
resource "aws_iam_role_policy" "workflow_run" {
  name = "publish-results-and-run-device-tests"
  role = aws_iam_role.workflow_run.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    {
      Sid = "PublishResults", Effect = "Allow", Action = "s3:PutObject"
      Resource = [for prefix in ["bloaty-results", "bloaty-results-ios", "benchmark-results", "android-benchmark-render"] :
      "arn:aws:s3:::maplibre-native/${prefix}/*"]
    },
    {
      Sid = "RunDeviceTests", Effect = "Allow"
      Action = [for action in ["CreateUpload", "GetUpload", "ScheduleRun", "GetRun", "ListJobs", "ListSuites", "ListArtifacts"] :
      "devicefarm:${action}"]
      Resource = flatten([for project in local.device_farm_projects : [
        for type in ["project", "upload", "devicepool", "run", "job", "suite", "test"] :
      "arn:aws:devicefarm:us-west-2:373521797162:${type}:${project}*"]])
    },
    {
      # mln-rs's workflow stops the run it scheduled when the workflow is cancelled.
      Sid      = "StopDeviceTests", Effect = "Allow", Action = "devicefarm:StopRun"
      Resource = "arn:aws:devicefarm:us-west-2:373521797162:run:${split(":", aws_devicefarm_project.mln_rs.arn)[6]}*"
    },
  ] })
}
# mln-rs's tests run on phones in a project of their own.
import {
  provider = aws.device_farm
  to       = aws_devicefarm_project.mln_rs
  id       = "arn:aws:devicefarm:us-west-2:373521797162:project:c5c3f5f6-e21b-41c4-a235-489d30075aeb"
}
resource "aws_devicefarm_project" "mln_rs" {
  provider                    = aws.device_farm
  name                        = "mln-rs"
  default_job_timeout_minutes = 60
}
moved {
  from = github_repository_environment.workflow_run
  to   = github_repository_environment.workflow_run["maplibre-native"]
}
moved {
  from = github_repository_environment_deployment_policy.workflow_run
  to   = github_repository_environment_deployment_policy.workflow_run["maplibre-native"]
}
moved {
  from = github_actions_environment_variable.workflow_run_role
  to   = github_actions_environment_variable.workflow_run_role["maplibre-native"]
}
resource "github_repository_environment" "workflow_run" {
  for_each    = local.workflow_run_repositories
  repository  = each.key
  environment = "workflow-run"
  deployment_branch_policy {
    protected_branches     = false
    custom_branch_policies = true
  }
}
resource "github_repository_environment_deployment_policy" "workflow_run" {
  for_each       = github_repository_environment.workflow_run
  repository     = each.value.repository
  environment    = each.value.environment
  branch_pattern = "main"
}
# Takes precedence over the repository variable for jobs in the environment.
resource "github_actions_environment_variable" "workflow_run_role" {
  for_each      = github_repository_environment.workflow_run
  repository    = each.value.repository
  environment   = each.value.environment
  variable_name = "OIDC_AWS_ROLE_TO_ASSUME"
  value         = aws_iam_role.workflow_run.arn
}
resource "github_actions_environment_variable" "mln_rs_device_farm_project" {
  repository    = github_repository_environment.workflow_run["mln-rs"].repository
  environment   = github_repository_environment.workflow_run["mln-rs"].environment
  variable_name = "AWS_DEVICE_FARM_PROJECT_ARN"
  value         = aws_devicefarm_project.mln_rs.arn
}
