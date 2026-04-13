#!/bin/bash
# Exit if any command fails, if any variable is unset.
set -euo pipefail

# You need to have set up env vars or run `aws configure` for this to work.

AWS_REGION=${AWS_REGION:-us-west-2}
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

eval $(python3 -c "from ldn.utils import GEOMAD_VERSION, PREDICTION_VERSION; print(f'export GEOMAD_VERSION={GEOMAD_VERSION}; export PREDICTION_VERSION={PREDICTION_VERSION}')")

# TODO: Run index here too? To ensure mosaics are created with the latest data.

echo "==> Building mosaics..."
# Make mosaics for all years for geomad.
# TODO: Uncomment this to create geomad mosaics. I have already done this for 0-0-4a so no need to wait to recreate.
# poetry run ldn make-mosaics --dataset "geomad" --years "2000-2025" --version-geomad $GEOMAD_VERSION --version-prediction $PREDICTION_VERSION
# Make mosaics for one year for prediction.
# poetry run ldn make-mosaics --dataset "prediction" --years "2023-2025" --version-geomad $GEOMAD_VERSION --version-prediction $PREDICTION_VERSION
echo "==> Creating ECR repository..."
terraform -chdir=visualisation/infra init
terraform -chdir=visualisation/infra apply -target=aws_ecr_repository.app -target=aws_ecr_lifecycle_policy.app # -auto-approve

FUNCTION_NAME=$(terraform -chdir=visualisation/infra output -raw function_name)
echo "==> Function name: ${FUNCTION_NAME}"

poetry check --lock || { echo "poetry.lock is out of date. Run 'poetry lock' first."; exit 1; }

echo "==> Building Docker image..."
docker build --platform=linux/arm64 --provenance=false -f visualisation/Dockerfile -t ${FUNCTION_NAME} .

echo "==> Pushing image to ECR..."
ECR_URL="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${FUNCTION_NAME}"
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
docker tag ${FUNCTION_NAME}:latest ${ECR_URL}:latest
docker push ${ECR_URL}:latest

# Verify the pushed image digest matches the local image
LOCAL_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' ${ECR_URL}:latest | cut -d@ -f2)
REMOTE_DIGEST=$(aws ecr describe-images --repository-name ${FUNCTION_NAME} --image-ids imageTag=latest --query 'imageDetails[0].imageDigest' --output text)
if [ "$LOCAL_DIGEST" != "$REMOTE_DIGEST" ]; then
  echo "ERROR: Push verification failed. Local digest: $LOCAL_DIGEST, Remote digest: $REMOTE_DIGEST"
  exit 1
fi

echo "==> Applying remaining infrastructure..."
terraform -chdir=visualisation/infra apply # -auto-approve

terraform -chdir=visualisation/infra output api_url
echo "==> Done."
