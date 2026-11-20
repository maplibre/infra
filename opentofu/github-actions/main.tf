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
# Authenticates with GITHUB_TOKEN, which needs admin access to maplibre/maplibre-native.
provider "github" {
  owner = "maplibre"
}
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}
locals {
  issuer               = "token.actions.githubusercontent.com"
  workflow_run_subject = "repo:maplibre/maplibre-native:environment:${github_repository_environment.workflow_run.environment}"
  device_farm_project  = "20687d72-0e46-403e-8f03-0941850665bc"
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
      StringNotEquals = { "${local.issuer}:sub" = local.workflow_run_subject }
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
    "arn:aws:iam::373521797162:policy/STS",
    "arn:aws:iam::373521797162:policy/github-actions-s3-buckets",
  ]
}
# maplibre-native's workflow_run jobs handle artifacts that fork pull requests
# control, so they can only publish results and run device tests.
resource "aws_iam_role" "workflow_run" {
  name                 = "GitHubActionsWorkflowRun"
  max_session_duration = 14400 # device tests request four-hour sessions
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect    = "Allow", Action = "sts:AssumeRoleWithWebIdentity"
    Principal = { Federated = data.aws_iam_openid_connect_provider.github.arn }
    Condition = { StringEquals = {
      "${local.issuer}:aud" = "sts.amazonaws.com"
      "${local.issuer}:sub" = local.workflow_run_subject
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
      Resource = [for type in ["project", "upload", "devicepool", "run", "job", "suite", "test"] :
      "arn:aws:devicefarm:us-west-2:373521797162:${type}:${local.device_farm_project}*"]
    },
  ] })
}
resource "github_repository_environment" "workflow_run" {
  repository  = "maplibre-native"
  environment = "workflow-run"
  deployment_branch_policy {
    protected_branches     = false
    custom_branch_policies = true
  }
}
resource "github_repository_environment_deployment_policy" "workflow_run" {
  repository     = github_repository_environment.workflow_run.repository
  environment    = github_repository_environment.workflow_run.environment
  branch_pattern = "main"
}
# Takes precedence over the repository variable for jobs in the environment.
resource "github_actions_environment_variable" "workflow_run_role" {
  repository    = github_repository_environment.workflow_run.repository
  environment   = github_repository_environment.workflow_run.environment
  variable_name = "OIDC_AWS_ROLE_TO_ASSUME"
  value         = aws_iam_role.workflow_run.arn
}
