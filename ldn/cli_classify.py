import logging
from typing import Literal

import typer

from ldn.classify import run_classify_task

classify_app = typer.Typer()
logger = logging.getLogger(__name__)


@classify_app.command("train-model")
def _train_model() -> None:
    # year = 2020
    # Steps:
    # 1. Gather all training data CSVs. Later do this per region/time range.
    # 2. Again filter by outliers per class.
    # 3. Train a random forest model.
    # 4. Write model to S3.

    # TODO: Adapt from notebooks/training_data/1_Train_Predict.ipynb.
    raise NotImplementedError("This command is not implemented yet.")


@classify_app.command("classify")
def _classify(
    tile_id: str = typer.Option(..., help="Tile ID to predict LULC for."),
    year: str = typer.Option(..., help="Year to predict LULC for."),
    version: str = typer.Option(..., help="Version of the model to use e.g. '0-0-1'."),
    version_geomad: str = typer.Option(
        ..., help="Version of the GeoMAD data to use e.g. '0-0-1'."
    ),
    region: Literal["pacific", "non-pacific"] = typer.Option(
        ..., help="Region to predict LULC for. Can be 'pacific' or 'non-pacific'."
    ),
    output_bucket: str = typer.Option(
        "data.ldn.auspatious.com", help="S3 bucket to write predictions to."
    ),
    model_path: str = typer.Option(
        "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/lulc_random_forest_model.joblib",
        help="Model to use for prediction.",
    ),
    xy_chunk_size: int = typer.Option(
        1024,
        help="Chunk size in pixels for x and y dimensions when predicting. Larger chunk sizes may be faster but use more memory.",
    ),
    asset_url_prefix: str | None = typer.Option(None, help="Prefix for asset URLs."),
    decimated: bool = typer.Option(
        False,
        help="Whether to use decimated data for prediction. Decimated data is faster to predict but less accurate.",
    ),
    overwrite: bool = typer.Option(
        False, help="Whether to overwrite existing prediction."
    ),
    probability_threshold: float = typer.Option(
        30.0,
        help="Probability threshold (0-100) for classifying a pixel as the target class. Higher values mean only pixels with higher predicted probability will be classified as the target class.",
    ),
    nodata_value: int = typer.Option(
        255,
        help="Value to use for NoData pixels in the output. Must be an integer between 0 and 255.",
    ),
) -> None:
    if int(year) < 2000 or int(year) > 2025:
        raise ValueError("Year must be between 2000 and 2025.")

    run_classify_task(
        tile_id,
        datetime=year,
        version=version,
        version_geomad=version_geomad,
        region=region,
        output_bucket=output_bucket,
        model_path=model_path,
        xy_chunk_size=xy_chunk_size,
        asset_url_prefix=asset_url_prefix,
        decimated=decimated,
        overwrite=overwrite,
        probability_threshold=probability_threshold,
        nodata_value=nodata_value,
    )
