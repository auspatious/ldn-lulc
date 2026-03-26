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

variable "s3_bucket" {
  description = "S3 bucket containing mosaic JSONs and COGs"
  type        = string
  default     = "data.ldn.auspatious.com"
}

variable "memory_size" {
  description = "Lambda memory in MB — GDAL/rasterio needs headroom"
  type        = number
  default     = 3008
}

variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 30
}
