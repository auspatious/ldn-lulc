terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Store state in S3.
  backend "s3" {
    # Using private bucket.
    bucket = "auspatious-ldn-terraform-state"
    key    = "ldn-tiler/terraform.tfstate"
    region = "us-west-2"
  }
}

provider "aws" {
  region = var.aws_region
}

# ── ECR ────────────────────────────────────────────────────────────────────────

resource "aws_ecr_repository" "app" {
  name                 = var.function_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Keep only the last 5 images to avoid storage costs
resource "aws_ecr_lifecycle_policy" "app" {
  repository = aws_ecr_repository.app.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

# ── IAM ────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda" {
  name = "${var.function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 read access for mosaic JSONs and COGs
resource "aws_iam_role_policy" "s3_read" {
  name = "${var.function_name}-s3-read"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket"
      ]
      Resource = [
        "arn:aws:s3:::${var.s3_bucket}",
        "arn:aws:s3:::${var.s3_bucket}/*"
      ]
    }]
  })
}

# ── Lambda ─────────────────────────────────────────────────────────────────────

# Look up the latest image digest so Terraform redeploys when a new image is pushed
data "aws_ecr_image" "latest" {
  repository_name = aws_ecr_repository.app.name
  image_tag       = "latest"
}

resource "aws_lambda_function" "app" {
  function_name = var.function_name
  role          = aws_iam_role.lambda.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.app.repository_url}@${data.aws_ecr_image.latest.image_digest}"
  architectures = ["arm64"]
  memory_size   = var.memory_size
  timeout       = var.timeout

  environment {
    variables = {
      GDAL_HTTP_MULTIPLEX                = "YES"
      GDAL_HTTP_MERGE_CONSECUTIVE_RANGES = "YES"
      GDAL_DISABLE_READDIR_ON_OPEN       = "EMPTY_DIR"
      VSI_CACHE                          = "TRUE"
      VSI_CACHE_SIZE                     = "536870912"
      GDAL_CACHEMAX                      = "512"
      PYTHONWARNINGS                     = "ignore"
      GEOMAD_VERSION                     = var.geomad_version
      PREDICTION_VERSION                 = var.prediction_version
    }
  }

  depends_on = [aws_iam_role_policy_attachment.basic_execution]
}

# ── API Gateway (HTTP API) ──────────────────────────────────────────────────────
# Using API Gateway because public function URL was being blocked by broad account policy I think.

resource "aws_apigatewayv2_api" "app" {
  name          = var.function_name
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET"]
  }
}

resource "aws_apigatewayv2_integration" "lambda" {
  api_id                 = aws_apigatewayv2_api.app.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.app.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "catch_all" {
  api_id    = aws_apigatewayv2_api.app.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.app.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.app.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.app.execution_arn}/*/*"
}
