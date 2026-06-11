# This file handles writing geomad to Source.Coop.
# Writing there needs its own AWS credentials, that are different to our standard credentials.
# This is all optional because writing to other buckets doesn't need this.

import logging
import os

import boto3
import obstore
from botocore.client import BaseClient
from dep_tools.writers import write_to_s3

from ldn.utils import AWS_REGION

logger = logging.getLogger(__name__)


_WRITE_KEY = "AWS_WRITE_ACCESS_KEY_ID"
_WRITE_SECRET = "AWS_WRITE_SECRET_ACCESS_KEY"
_WRITE_TOKEN = "AWS_WRITE_SESSION_TOKEN"


def get_write_session() -> boto3.Session:
    """Return a boto3 Session for write operations.

    Prefers explicit AWS_WRITE_* env vars (injected via Kubernetes secret
    or set locally). Falls back to the default credential chain so that
    local runs without write-specific creds still work.
    """
    key = os.environ.get(_WRITE_KEY)
    secret = os.environ.get(_WRITE_SECRET)

    if not (key and secret):
        logger.info("AWS_WRITE_* env vars not set; falling back to default credential chain for writes.")
        return boto3.Session()

    logger.info("Using explicit write credentials from environment.")
    return boto3.Session(
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=os.environ.get(_WRITE_TOKEN),
        region_name=AWS_REGION,  # Same for Source.Coop and all other buckets.
    )


def make_write_function(
    session: boto3.Session,
):
    client: BaseClient = session.client("s3")

    def write_with_credentials(data, path, bucket, **kwargs):
        return write_to_s3(data, path, bucket, client=client, **kwargs)

    return write_with_credentials


def get_write_client(session: boto3.Session) -> BaseClient:
    return session.client("s3")


def _session_credentials(session: boto3.Session) -> dict:
    """Extract resolved credentials from a boto3 session as a plain dict."""
    creds = session.get_credentials().get_frozen_credentials()
    return {
        "access_key_id": creds.access_key,
        "secret_access_key": creds.secret_key,
        "token": creds.token,  # None for long-lived creds
        "region": session.region_name,
    }


def make_obstore_s3(bucket: str, session: boto3.Session) -> "obstore.store.S3Store":
    """Create an obstore S3Store using credentials from the given boto3 session."""

    creds = _session_credentials(session)
    kwargs = {
        "bucket": bucket,
        "region": creds["region"] or AWS_REGION,
        "access_key_id": creds["access_key_id"],
        "secret_access_key": creds["secret_access_key"],
    }
    if creds["token"]:
        kwargs["token"] = creds["token"]

    return obstore.store.S3Store(**kwargs)
