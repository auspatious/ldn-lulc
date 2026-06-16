# This file handles writing geomad to Source.Coop.
# Writing there needs its own AWS credentials, that are different to our standard credentials.
# This is all optional because writing to other buckets doesn't need this.

import logging
import os
from functools import partial

import boto3
import obstore
from botocore.client import BaseClient
from dep_tools.writers import write_to_s3

from ldn.utils import AWS_REGION, SOURCE_COOP_PREFIX_GEOMAD, SOURCE_COOP_PREFIX_PREDICTION, SOURCE_COOP_PUBLIC_URL

is_public = bool(SOURCE_COOP_PUBLIC_URL) and bool(SOURCE_COOP_PREFIX_GEOMAD) and bool(SOURCE_COOP_PREFIX_PREDICTION)


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

    if not (is_public):
        logger.info("SOURCE_COOP_PUBLIC_URL or prefixes not set; skipping write credential setup.")
        return boto3.Session()

    if not (key and secret):
        logger.info("AWS_WRITE_* env vars not set; falling back to default credential chain for writes.")
        return boto3.Session()

    logger.info("Using explicit write credentials from environment.")
    return boto3.Session(
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=os.environ.get(_WRITE_TOKEN),
        region_name=AWS_REGION,
    )


def make_write_function(session: boto3.Session):
    """Create a write function bound to the credentials of the given session.

    Args:
        session: A boto3 session with the credentials to use for writing.

    Returns:
        A partial of :func:`write_to_s3` with the session's S3 client bound.
    """
    client: BaseClient = session.client("s3")
    return partial(write_to_s3, client=client)


# Used for indexing
def make_obstore_s3(bucket: str, session: boto3.Session) -> "obstore.store.S3Store":
    """Create an obstore S3Store using credentials from the given boto3 session."""

    def _session_credentials(session: boto3.Session) -> dict:
        """Extract resolved credentials from a boto3 session as a plain dict."""
        creds = session.get_credentials().get_frozen_credentials()
        return {
            "access_key_id": creds.access_key,
            "secret_access_key": creds.secret_key,
            "token": creds.token,  # None for long-lived creds
            "region": session.region_name,
        }

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
