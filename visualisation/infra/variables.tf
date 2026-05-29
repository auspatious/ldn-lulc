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

variable "s3_bucket_pacific" {
  description = "S3 bucket containing Pacific mosaic JSONs and COGs"
  type        = string
  default     = "dep-public-staging"
}

variable "s3_bucket_non_pacific" {
  description = "S3 bucket containing Non-Pacific mosaic JSONs and COGs"
  type        = string
  default     = "data.ldn.auspatious.com"
}

variable "owner_pacific" {
  description = "Short owner prefix for Pacific datasets (e.g. dep)"
  type        = string
  default     = "dep"
}

variable "owner_non_pacific" {
  description = "Short owner prefix for Non-Pacific datasets (e.g. ci)"
  type        = string
  default     = "ci"
}

variable "geomad_version" {
  description = "Version string for GeoMAD data (e.g. 0-2-1)"
  type        = string
  default     = "0-2-1"
}

variable "prediction_version" {
  description = "Version string for LULC prediction data (e.g. 0-0-4)"
  type        = string
  default     = "0-0-4"
}

variable "memory_size" {
  description = "Lambda memory in MB — GDAL/rasterio needs headroom"
  type        = number
  default     = 3008
}

variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 60
}
