#!/bin/bash
# Exit if any command fails, if any variable is unset.
set -euo pipefail

# aws configure

AWS_REGION=${AWS_REGION:-us-west-2}
FUNCTION_NAME="ldn-tiler"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URL="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${FUNCTION_NAME}"

echo "==> Building mosaics..."
# TODO: Do all years when available.
ldn create-mosaics -dataset all -y 2020

echo "==> Creating ECR repository..."
cd visualisation/infra
terraform init
# terraform plan
terraform apply -target=aws_ecr_repository.app -target=aws_ecr_lifecycle_policy.app # -auto-approve
cd ../..

echo "==> Building Docker image..."
docker build --provenance=false -f visualisation/Dockerfile -t ${FUNCTION_NAME} .

echo "==> Pushing image to ECR..."
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
docker tag ${FUNCTION_NAME}:latest ${ECR_URL}:latest
docker push ${ECR_URL}:latest

echo "==> Applying remaining infrastructure..."
cd visualisation/infra
terraform apply # -auto-approve

terraform output api_url
echo "==> Done."
