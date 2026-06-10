variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-west-2"
}

variable "function_name" {
  description = "Name for the Lambda function and ECR repository"
  type        = string
  default     = "ldn-tiler"
}

# TODO: Reenable for non-source.coop.
# variable "s3_bucket" {
#   description = "S3 bucket containing mosaic JSONs and COGs"
#   type        = string
#   default     = "https://data.source.coop"
# }
