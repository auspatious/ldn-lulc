import logging
from typing import Annotated, Literal

import typer

from ldn.lulc import run_classify_task
from ldn.utils import (
    AWS_REGION,
    BUCKET,
    GEOMAD_VERSION,
    LULC_VERSION,
    MODEL_VERSION,
    NON_PACIFIC_OWNER,
    PACIFIC_OWNER,
    LdnError,
    owner_for_region,
)

classify_app = typer.Typer()
logger = logging.getLogger(__name__)


@classify_app.command()
def run(
    tile_id: str = typer.Option(..., help="Tile ID to classify LULC for."),
    year: str = typer.Option(..., help="Year to classify LULC for."),
    version: str = typer.Option(
        LULC_VERSION,
        help=f"Version of training data to output e.g. '{LULC_VERSION}'.",
    ),
    version_geomad: str = typer.Option(
        GEOMAD_VERSION,
        help=f"Version of the GeoMAD data to use e.g. '{GEOMAD_VERSION}'.",
    ),
    region: Literal["pacific", "non-pacific"] = typer.Option(
        ..., help="Region tile belongs to. Can be 'pacific' or 'non-pacific'."
    ),
    bucket: str = typer.Option(BUCKET, help="S3 bucket for data."),
    owner_pacific: str = typer.Option(PACIFIC_OWNER, help="S3 owner prefix for Pacific data."),
    owner_non_pacific: str = typer.Option(NON_PACIFIC_OWNER, help="S3 owner prefix for non-Pacific data."),
    product_owner: str | None = typer.Option(None, help="Override the region-derived owner prefix."),
    model_path: str = typer.Option(
        # TODO: defaults to pacific. Later have per region/time period models.
        f"https://s3.{AWS_REGION}.amazonaws.com/data.ldn.auspatious.com/models/{MODEL_VERSION}/pacific/2020/lulc_random_forest_model_pacific_2020.joblib",
        help="Model to use for LULC classification.",
    ),
    decimated: bool = typer.Option(
        False,
        help="Lower resolution data for faster processing/testing.",
    ),
    overwrite: bool = typer.Option(False, help="Whether to overwrite existing LULC classification."),
    probability_threshold: float = typer.Option(
        30.0,
        help="Probability threshold (0-100) for classifying a pixel as the target class. "
        "Higher values mean only pixels with higher predicted probability will be classified as the target class.",
    ),
    nodata_value: int = typer.Option(
        255,
        help="Value to use for NoData pixels in the output. Must be an integer between 0 and 255.",
    ),
    memory_limit: Annotated[str, typer.Option(help="Per-worker Dask memory limit.")] = "10GB",
    n_workers: Annotated[int, typer.Option(help="Number of Dask workers.")] = 2,
    threads_per_worker: Annotated[int, typer.Option(help="Threads per Dask worker.")] = 4,
    xy_chunk_size: Annotated[
        int,
        typer.Option(
            help="Chunk size in pixels for x and y dimensions. Larger chunk sizes may be faster but use more memory."
        ),
    ] = 1024,
) -> None:
    if int(year) < 2000 or int(year) > 2025:
        raise LdnError("Year must be between 2000 and 2025.")

    owner = owner_for_region(region, owner_pacific, owner_non_pacific, product_owner)

    run_classify_task(
        tile_id,
        year=year,
        version=version,
        version_geomad=version_geomad,
        region=region,
        bucket=bucket,
        owner=owner,
        model_path=model_path,
        xy_chunk_size=xy_chunk_size,
        decimated=decimated,
        overwrite=overwrite,
        probability_threshold=probability_threshold,
        nodata_value=nodata_value,
        memory_limit=memory_limit,
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
    )
