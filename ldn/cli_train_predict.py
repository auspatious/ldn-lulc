import logging
from typing import Literal

import typer

train_predict_app = typer.Typer()
logger = logging.getLogger(__name__)


@train_predict_app.command("create-training-data")
def _create_training_data(
    # TODO: Should this take a tile_id, or country, or something else as the spatial extent?
    tile_id: str = typer.Option(..., help="Tile ID to predict LULC for."),
    country_name: str = typer.Option(..., help="Country name to predict LULC for.")
) -> None:
    year = 2020
    # Steps:
    # 1. Load geomad and dem data for the tile/country and year.
    # 2. Load LULC products and make agreeing dataset.
    # 3. Make random sample points in tile/country (where there is agreeing data).
    # 4. Add geomad_dem to points.
    # 5. Filter outliers per LULC class.
    # 6. Write to CSV.

    raise NotImplementedError("This command is not implemented yet.")

@train_predict_app.command("train-model")
def _train_model() -> None:
    year = 2020
    # Steps:
    # 1. Gather all training data CSVs. Later do this per region/time range.
    # 2. Again filter by outliers per class.
    # 3. Train a random forest model. Export this model.
    raise NotImplementedError("This command is not implemented yet.")

@train_predict_app.command("predict")
def _predict(
    tile_id: str = typer.Option(..., help="Tile ID to predict LULC for."),
    year: str = typer.Option(..., help="Year to predict LULC for."),
    version: str = typer.Option(..., help="Version of the model to use e.g. '0-0-1'."),
    region: Literal["pacific", "non-pacific"] = typer.Option(..., help="Region to predict LULC for. Can be 'pacific' or 'non-pacific'."),
    ) -> None:
    if int(year) < 2000 or int(year) > 2024:
        raise ValueError("Year must be between 2000 and 2024.")
    # Steps:
    # 1. Load geomad and dem data for the tile and year.
    # 2. Load model for region and version.
    # 3. Predict LULC for tile and year.
    # 4. Write predicted LULC as COG to S3.
    # 5. Update STAC-Geoparquet in S3 with new metadata.
    
    raise NotImplementedError("This command is not implemented yet.")
