import logging
from typing import Annotated, Literal

import typer

from ldn.lulc import run_classify_task
from ldn.utils import (
    GEOMAD_VERSION,
    LULC_VERSION,
    SENSOR,
    LdnError,
    get_env_var,
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
    geomad_version: str = typer.Option(
        GEOMAD_VERSION,
        help=f"Version of the GeoMAD data to use e.g. '{GEOMAD_VERSION}'.",
    ),
    region: Literal["pacific", "non-pacific"] = typer.Option(
        ..., help="Region tile belongs to. Can be 'pacific' or 'non-pacific'."
    ),
    bucket: Annotated[str | None, typer.Option(help="S3 bucket for data.")] = None,
    product_owner: str | None = typer.Option(None, help="Override the region-derived owner prefix."),
    model_path: str = typer.Option(
        # TODO: defaults to pacific. Later have per region/time period models.
        "https://dep-public-staging.s3.us-west-2.amazonaws.com/dep_ls_lulc/models/0-0-9/pacific/2020/lulc_random_forest_model_pacific_2020.joblib",
        help="Model to use for LULC classification.",
    ),
    decimated: bool = typer.Option(
        False,
        help="Lower resolution data for faster processing/testing.",
    ),
    integration_test: bool = typer.Option(
        False,
        help="Integration test mode: use decimated data for faster processing.",
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
    sensor: str = typer.Option(SENSOR, help="Sensor to use for LULC classification. Defaults to 'ls'."),
    collection_url_root: Annotated[
        str | None,
        typer.Option(
            help="Override the default collection URL root"
            " e.g for a STAC API like 'https://stac.digitalearthpacific.org/collections'"
        ),
    ] = None,
) -> None:
    if int(year) < 2000 or int(year) > 2025:
        raise LdnError("Year must be between 2000 and 2025.")

    bucket = bucket or get_env_var("BUCKET")  # Default

    run_classify_task(
        tile_id,
        year=year,
        version=version,
        geomad_version=geomad_version,
        region=region,
        bucket=bucket,
        product_owner=product_owner,
        model_path=model_path,
        xy_chunk_size=xy_chunk_size,
        decimated=decimated,
        integration_test=integration_test,
        overwrite=overwrite,
        probability_threshold=probability_threshold,
        nodata_value=nodata_value,
        memory_limit=memory_limit,
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        sensor=sensor,
        collection_url_root=collection_url_root,
    )
