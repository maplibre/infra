terraform {
  required_version = ">= 1.11, < 2.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
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
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}
locals {
  issuer = "token.actions.githubusercontent.com"
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
