import json
import logging
from typing import Literal

import obstore
import obstore.store
import typer
from rustac import write_sync

from ldn.train_predict import run_predict_task

train_predict_app = typer.Typer()
logger = logging.getLogger(__name__)

@train_predict_app.command("train-model")
def _train_model() -> None:
    year = 2020
    # Steps:
    # 1. Gather all training data CSVs. Later do this per region/time range.
    # 2. Again filter by outliers per class.
    # 3. Train a random forest model. 
    # 4. Write model to S3.

    # TODO: Adapt from notebooks/training_data/1_Train_Predict.ipynb.
    raise NotImplementedError("This command is not implemented yet.")

@train_predict_app.command("predict")
def _predict(
    tile_id: str = typer.Option(..., help="Tile ID to predict LULC for."),
    year: str = typer.Option(..., help="Year to predict LULC for."),
    version: str = typer.Option(..., help="Version of the model to use e.g. '0-0-1'."),
    region: Literal["pacific", "non-pacific"] = typer.Option(..., help="Region to predict LULC for. Can be 'pacific' or 'non-pacific'."),
    output_bucket: str = typer.Option("data.ldn.auspatious.com", help="S3 bucket to write predictions to."),
    model_path: str = typer.Option("ldn/lulc_random_forest_model.joblib", help="Model to use for prediction."),
    xy_chunk_size: int = typer.Option(1024, help="Chunk size in pixels for x and y dimensions when predicting. Larger chunk sizes may be faster but use more memory."),
    asset_url_prefix: str | None = typer.Option(None, help="Prefix for asset URLs."),
    decimated: bool = typer.Option(False, help="Whether to use decimated data for prediction. Decimated data is faster to predict but less accurate."),
    overwrite: bool = typer.Option(False, help="Whether to overwrite existing prediction."),
    ) -> None:
    if int(year) < 2000 or int(year) > 2024:
        raise ValueError("Year must be between 2000 and 2024.")

    run_predict_task(
        tile_id, 
        datetime=year,
        version=version,
        region=region,
        output_bucket=output_bucket,
        model_path=model_path,
        xy_chunk_size=xy_chunk_size,
        asset_url_prefix=asset_url_prefix,
        decimated=decimated,
        overwrite=overwrite,
        # probability_threshold
    )


def _find_stac_items_s3(
    bucket: str,
    prefix: str,
    aws_region: str,
    suffix: str = ".stac-item.json",
    chunk_size: int = 200,
) -> list[str]:
    """List S3 keys ending in suffix under bucket/prefix.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix to search under.
        aws_region: AWS region of the bucket.
        suffix: File suffix to match.
        chunk_size: Number of objects per listing page.

    Returns:
        List of S3 keys (without the s3://bucket/ prefix) that match.
    """
    store = obstore.store.S3Store(bucket=bucket, region=aws_region)
    matches: list[str] = []
    stream = obstore.list(store, prefix=prefix.lstrip("/"), chunk_size=chunk_size)

    for chunk in stream:
        for obj in chunk:
            path = obj.get("path", "")
            if path.endswith(suffix):
                matches.append(path)

    return matches


def _load_stac_docs(
    bucket: str,
    keys: list[str],
    aws_region: str,
) -> list[dict]:
    """Load STAC item JSON documents from S3 into memory.

    Args:
        bucket: S3 bucket name.
        keys: S3 object keys to load.
        aws_region: AWS region of the bucket.

    Returns:
        List of parsed STAC item dictionaries.
    """
    store = obstore.store.S3Store(bucket=bucket, region=aws_region)
    docs: list[dict] = []

    for key in keys:
        raw = obstore.get(store, key)
        payload = raw.bytes()
        if hasattr(payload, "to_bytes"):
            payload = payload.to_bytes()
        elif not isinstance(payload, (bytes, bytearray)):
            payload = bytes(payload)
        docs.append(json.loads(payload.decode("utf-8")))

    return docs


@train_predict_app.command("index-predictions")
def _index_predictions(
    version: str = typer.Option("0-0-1", help="Prediction version string e.g. '0-0-1'."),
    bucket: str = typer.Option("data.ldn.auspatious.com", help="S3 bucket containing predictions."),
    aws_region: str = typer.Option("us-west-2", help="AWS region of the bucket."),
) -> None:
    """Build a STAC-Geoparquet index from all prediction STAC items on S3."""
    prefix = f"ausp_ls_lulc_prediction/{version}"
    parquet_key = f"{prefix}/ausp_ls_lulc_prediction.parquet"

    logger.info(f"Listing STAC items under s3://{bucket}/{prefix}")
    keys = _find_stac_items_s3(bucket, prefix, aws_region)
    logger.info(f"Found {len(keys)} STAC items")

    if len(keys) == 0:
        logger.warning("No STAC items found, nothing to index.")
        raise typer.Exit(code=1)

    logger.info("Loading STAC item documents into memory")
    docs = _load_stac_docs(bucket, keys, aws_region)
    logger.info(f"Loaded {len(docs)} STAC documents")

    logger.info(f"Writing STAC-Geoparquet to s3://{bucket}/{parquet_key}")
    store = obstore.store.S3Store(bucket=bucket, region=aws_region)
    write_sync(parquet_key, docs, store=store)

    logger.info(f"Wrote index with {len(docs)} items to s3://{bucket}/{parquet_key}")
