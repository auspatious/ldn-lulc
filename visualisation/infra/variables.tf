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
  default     = "data.ldn.auspatious.com" # TODO: this is where the prediction is.
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

variable "geomad_version" {
  description = "GeoMAD dataset version string (e.g. 0-1-0)"
  type        = string
}

# TODO: Remove once comparison is done.
variable "geomad_version_new" {
  description = "New GeoMAD dataset version string for comparison (e.g. 0-2-0)"
  type        = string
}

variable "prediction_version" {
  description = "LULC prediction dataset version string (e.g. 0-0-3)"
  type        = string
}

variable "geomad_s3_bucket" {
  description = "S3 bucket containing GeoMAD data"
  type        = string
  default     = "dep-public-staging" # TODO: this is where the geomad is.
}
