terraform {
  required_version = ">= 1.11, < 2.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "s3" {
    bucket       = "maplibre-ci-runners-tofu-state-373521797162"
    key          = "device-farm/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region              = "us-west-2"
  allowed_account_ids = ["373521797162"]
  default_tags { tags = { Project = "maplibre-device-farm", ManagedBy = "OpenTofu" } }
}

locals {
  name_prefix        = "maplibre-device-farm"
  account_id         = "373521797162"
  region             = "us-west-2"
  lambda_zip_path    = "${path.module}/${var.lambda_package_path}"
  lambda_source_hash = fileexists(local.lambda_zip_path) ? filebase64sha256(local.lambda_zip_path) : null

  lambda_assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# Operator populates this secret out-of-band with JSON:
# { "app_id": "...", "private_key_pem": "...", "webhook_secret": "..." }
# Never manage secret values in OpenTofu state.
resource "aws_secretsmanager_secret" "github_app" {
  name        = "${local.name_prefix}/github-app"
  description = "GitHub App credentials (app_id, private_key_pem, webhook_secret) for the Device Farm orchestrator."
}

# Tracks pending, active, completed, and superseded Device Farm runs.
resource "aws_dynamodb_table" "runs" {
  name         = "${local.name_prefix}-runs"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_arn"

  attribute {
    name = "run_arn"
    type = "S"
  }
  attribute {
    name = "state"
    type = "S"
  }
  attribute {
    name = "created_at"
    type = "S"
  }
  attribute {
    name = "branch_key"
    type = "S"
  }
  attribute {
    name = "repo_sha"
    type = "S"
  }

  global_secondary_index {
    name            = "state-index"
    projection_type = "ALL"
    key_schema {
      attribute_name = "state"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "created_at"
      key_type       = "RANGE"
    }
  }

  global_secondary_index {
    name            = "branch_key-index"
    projection_type = "ALL"
    key_schema {
      attribute_name = "branch_key"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "created_at"
      key_type       = "RANGE"
    }
  }

  global_secondary_index {
    name            = "sha-index"
    projection_type = "ALL"
    key_schema {
      attribute_name = "repo_sha"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "created_at"
      key_type       = "RANGE"
    }
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Decouples webhook acknowledgement (<1s) from Device Farm uploads and scheduling.
resource "aws_sqs_queue" "jobs_dlq" {
  name                      = "${local.name_prefix}-jobs-dlq"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "jobs" {
  name                       = "${local.name_prefix}-jobs"
  visibility_timeout_seconds = 360
  message_retention_seconds  = 345600
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.jobs_dlq.arn
    maxReceiveCount     = 3
  })
}

# Private S3 bucket holding per-phone logs, JUnit XML, and customer artifacts for 30 days.
resource "aws_s3_bucket" "results" {
  bucket = "${local.name_prefix}-results-${local.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    id     = "expire-results"
    status = "Enabled"
    filter {}
    expiration { days = 30 }
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

# CloudFront Origin Access Control serves read-only results from the private S3 bucket
# with sandboxed security headers so untrusted fork output can never execute scripts.
resource "aws_cloudfront_origin_access_control" "results" {
  name                              = "${local.name_prefix}-results-oac"
  description                       = "OAC for private Device Farm results bucket"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_response_headers_policy" "results" {
  name    = "${local.name_prefix}-results-headers"
  comment = "Sandbox untrusted CI test output served from S3"
  security_headers_config {
    content_security_policy {
      content_security_policy = "sandbox"
      override                = true
    }
    content_type_options {
      override = true
    }
  }
}

data "aws_cloudfront_cache_policy" "caching_optimized" {
  name = "Managed-CachingOptimized"
}

resource "aws_cloudfront_distribution" "results" {
  enabled         = true
  is_ipv6_enabled = true
  comment         = "MapLibre AWS Device Farm test results"

  origin {
    domain_name              = aws_s3_bucket.results.bucket_regional_domain_name
    origin_id                = "results-s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.results.id
  }

  default_cache_behavior {
    target_origin_id           = "results-s3"
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD"]
    cached_methods             = ["GET", "HEAD"]
    compress                   = true
    cache_policy_id            = data.aws_cloudfront_cache_policy.caching_optimized.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.results.id
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

resource "aws_s3_bucket_policy" "results" {
  bucket = aws_s3_bucket.results.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "RequireTLS"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = [aws_s3_bucket.results.arn, "${aws_s3_bucket.results.arn}/*"]
        Condition = { Bool = { "aws:SecureTransport" = "false" } }
      },
      {
        Sid       = "AllowCloudFrontRead"
        Effect    = "Allow"
        Principal = { Service = "cloudfront.amazonaws.com" }
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.results.arn}/*"
        Condition = { StringEquals = { "AWS:SourceArn" = aws_cloudfront_distribution.results.arn } }
      },
    ]
  })
}

# CloudWatch log groups with 14-day retention for each Lambda.
resource "aws_cloudwatch_log_group" "webhook" {
  name              = "/aws/lambda/${local.name_prefix}-webhook"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/${local.name_prefix}-worker"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "poller" {
  name              = "/aws/lambda/${local.name_prefix}-poller"
  retention_in_days = 14
}

# --- Webhook Lambda ---

resource "aws_iam_role" "webhook" {
  name               = "${local.name_prefix}-webhook"
  assume_role_policy = local.lambda_assume_role_policy
}

resource "aws_iam_role_policy" "webhook" {
  name = "${local.name_prefix}-webhook"
  role = aws_iam_role.webhook.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.webhook.arn}:*"
      },
      {
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = aws_secretsmanager_secret.github_app.arn
      },
      {
        Effect   = "Allow"
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.jobs.arn
      },
    ]
  })
}

resource "aws_lambda_function" "webhook" {
  function_name    = "${local.name_prefix}-webhook"
  role             = aws_iam_role.webhook.arn
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  handler          = "bootstrap"
  filename         = local.lambda_zip_path
  source_code_hash = local.lambda_source_hash
  memory_size      = 128
  timeout          = 10

  environment {
    variables = {
      MODE                  = "webhook"
      GITHUB_APP_SECRET_ARN = aws_secretsmanager_secret.github_app.arn
      JOBS_QUEUE_URL        = aws_sqs_queue.jobs.url
      ALLOWED_BRANCHES      = var.allowed_branches
      MLN_RS_PROJECT_ARN    = var.mln_rs_project_arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.webhook, aws_iam_role_policy.webhook]
}

resource "aws_lambda_function_url" "webhook" {
  function_name      = aws_lambda_function.webhook.function_name
  authorization_type = "NONE"
}

# --- Worker Lambda ---

resource "aws_iam_role" "worker" {
  name               = "${local.name_prefix}-worker"
  assume_role_policy = local.lambda_assume_role_policy
}

resource "aws_iam_role_policy" "worker" {
  name = "${local.name_prefix}-worker"
  role = aws_iam_role.worker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.worker.arn}:*"
      },
      {
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = aws_secretsmanager_secret.github_app.arn
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility",
        ]
        Resource = aws_sqs_queue.jobs.arn
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
        ]
        Resource = [aws_dynamodb_table.runs.arn, "${aws_dynamodb_table.runs.arn}/index/*"]
      },
      {
        Effect = "Allow"
        Action = [
          "devicefarm:CreateUpload",
          "devicefarm:GetUpload",
          "devicefarm:ScheduleRun",
          "devicefarm:StopRun",
          "devicefarm:GetRun",
          "devicefarm:ListRuns",
          "devicefarm:ListDevices",
          "devicefarm:ListDevicePools",
        ]
        Resource = [
          "arn:aws:devicefarm:${local.region}:${local.account_id}:*",
          "arn:aws:devicefarm:${local.region}::device:*",
        ]
      },
    ]
  })
}

resource "aws_lambda_function" "worker" {
  function_name    = "${local.name_prefix}-worker"
  role             = aws_iam_role.worker.arn
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  handler          = "bootstrap"
  filename         = local.lambda_zip_path
  source_code_hash = local.lambda_source_hash
  memory_size      = 1024
  timeout          = 300

  ephemeral_storage {
    size = 2048
  }

  environment {
    variables = {
      MODE                  = "worker"
      GITHUB_APP_SECRET_ARN = aws_secretsmanager_secret.github_app.arn
      RUNS_TABLE_NAME       = aws_dynamodb_table.runs.name
      ALLOWED_BRANCHES      = var.allowed_branches
      MLN_RS_PROJECT_ARN    = var.mln_rs_project_arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.worker, aws_iam_role_policy.worker]
}

resource "aws_lambda_event_source_mapping" "worker_sqs" {
  event_source_arn        = aws_sqs_queue.jobs.arn
  function_name           = aws_lambda_function.worker.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
}

# --- Poller Lambda ---

resource "aws_iam_role" "poller" {
  name               = "${local.name_prefix}-poller"
  assume_role_policy = local.lambda_assume_role_policy
}

resource "aws_iam_role_policy" "poller" {
  name = "${local.name_prefix}-poller"
  role = aws_iam_role.poller.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.poller.arn}:*"
      },
      {
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = aws_secretsmanager_secret.github_app.arn
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query",
        ]
        Resource = [aws_dynamodb_table.runs.arn, "${aws_dynamodb_table.runs.arn}/index/*"]
      },
      {
        Effect = "Allow"
        Action = [
          "devicefarm:GetRun",
          "devicefarm:ListJobs",
          "devicefarm:ListArtifacts",
        ]
        Resource = "arn:aws:devicefarm:${local.region}:${local.account_id}:*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.results.arn}/*"
      },
    ]
  })
}

resource "aws_lambda_function" "poller" {
  function_name    = "${local.name_prefix}-poller"
  role             = aws_iam_role.poller.arn
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  handler          = "bootstrap"
  filename         = local.lambda_zip_path
  source_code_hash = local.lambda_source_hash
  memory_size      = 512
  timeout          = 120

  ephemeral_storage {
    size = 1024
  }

  environment {
    variables = {
      MODE                  = "poller"
      GITHUB_APP_SECRET_ARN = aws_secretsmanager_secret.github_app.arn
      RUNS_TABLE_NAME       = aws_dynamodb_table.runs.name
      RESULTS_BUCKET_NAME   = aws_s3_bucket.results.bucket
      RESULTS_BASE_URL      = "https://${aws_cloudfront_distribution.results.domain_name}"
      MLN_RS_PROJECT_ARN    = var.mln_rs_project_arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.poller, aws_iam_role_policy.poller]
}

resource "aws_cloudwatch_event_rule" "poller" {
  name                = "${local.name_prefix}-poller"
  description         = "Poll active AWS Device Farm runs every minute"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "poller" {
  rule = aws_cloudwatch_event_rule.poller.name
  arn  = aws_lambda_function.poller.arn
}

resource "aws_lambda_permission" "poller_events" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.poller.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.poller.arn
}

output "webhook_url" {
  description = "Lambda Function URL to configure as the GitHub App webhook endpoint."
  value       = aws_lambda_function_url.webhook.function_url
}

output "results_base_url" {
  description = "CloudFront base URL serving sandboxed Device Farm test artifacts."
  value       = "https://${aws_cloudfront_distribution.results.domain_name}"
}

output "github_app_secret_arn" {
  description = "Secrets Manager ARN where the operator populates GitHub App credentials."
  value       = aws_secretsmanager_secret.github_app.arn
}

output "results_bucket_name" {
  description = "Private S3 bucket storing Device Farm test outputs and artifacts."
  value       = aws_s3_bucket.results.bucket
}
