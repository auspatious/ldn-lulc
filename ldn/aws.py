import os

import boto3
from obstore.auth.boto3 import Boto3CredentialProvider
from odc.stac import configure_s3_access

# Profile env var is used locally using aws sso.
profile = os.environ.get("AWS_PROFILE")
# In argo there is no profile so fall back to default credentials.
aws_session = boto3.Session(profile_name=profile) if profile else boto3.Session()
s3_client = aws_session.client("s3", region_name=aws_session.region_name)

credential_provider = Boto3CredentialProvider(aws_session)


def configure_s3_access_profile():
    """Needs to be a function to be run before Dask workers are started. Running at module doesn't work."""
    configure_s3_access(requester_pays=True, profile=os.environ.get("AWS_PROFILE"))
