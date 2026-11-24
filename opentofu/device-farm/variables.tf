variable "allowed_branches" {
  description = "Optional comma-separated list of head branch names to process (empty processes all branches)."
  type        = string
  default     = ""
}

variable "mln_rs_project_arn" {
  description = "AWS Device Farm project ARN for maplibre/mln-rs."
  type        = string
  default     = "arn:aws:devicefarm:us-west-2:373521797162:project:c5c3f5f6-e21b-41c4-a235-489d30075aeb"
}

variable "lambda_package_path" {
  description = "Path (relative to opentofu/device-farm) to the built Lambda lambda.zip package."
  type        = string
  default     = "../../device-farm/build/lambda.zip"
}
