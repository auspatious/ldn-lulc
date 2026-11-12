# Forked from https://github.com/frontiersi/FAO_LC_workshop_Rwanda/blob/main/random_sampling.py
import logging

import geopandas as gpd
import numpy as np
import odc.geo.xr  # noqa: F401  Registers the .odc accessor.
import pandas as pd
from xarray import DataArray

from ldn.utils import CLASS_ATTR, WGS84, LdnError

logger = logging.getLogger(__name__)


def random_sampling(
    da: DataArray,
    n: int,
    min_sample_n: int | None,
    sampling: str = "stratified_random",
    seed: int | None = None,
):
    """
    Creates randomly sampled points for post-classification
    accuracy assessment.

    Params:
    -------
    da: xarray.DataArray
        A classified 2-dimensional xarray.DataArray
    n: int
        Total number of points to sample.
    min_sample_n: int, optional
        Minimum number of samples to generate per class. Only used for stratified_random sampling method
    sampling: str
        'stratified_random' = Create points that are randomly
        distributed within each class, where each class has a
        number of points proportional to its relative area.
        'equal_stratified_random' = Create points that are randomly
        distributed within each class, where each class has the
        same number of points.
    seed: int, optional
        Random seed so the sampled locations are reproducible.

    Output
    ------
    GeoPandas.Dataframe

    """

    if sampling not in ["stratified_random", "equal_stratified_random"]:
        raise LdnError("Sampling strategy must be 'stratified_random' or 'equal_stratified_random'")

    # Open the dataset as a pandas dataframe
    da = da.squeeze()
    df = da.to_dataframe(name=CLASS_ATTR)

    # Drop nodata pixels.
    df = df[df[CLASS_ATTR] != 255]

    # List to store points
    samples = []

    # Determine class ratios in image
    class_ratio = pd.DataFrame(
        {
            "proportion": df[CLASS_ATTR].value_counts(normalize=True),
            "n_available": df[CLASS_ATTR].value_counts(normalize=False),
            "class": df[CLASS_ATTR].value_counts(normalize=True).keys(),
        }
    )
    logger.info(class_ratio)
    if sampling == "stratified_random":
        if not min_sample_n:
            raise LdnError("min_sample_n must be defined for stratified_random sampling method")
        for _class in class_ratio["class"]:
            # Use relative proportions of classes to sample df
            no_of_points = n * class_ratio[class_ratio["class"] == _class]["proportion"].values[0]
            n_available = class_ratio[class_ratio["class"] == _class]["n_available"].values[0]
            if n_available >= max([min_sample_n, no_of_points]):
                no_of_points = max([min_sample_n, no_of_points])
                # Random sample each class
                logger.info(f"Class {_class}: sampling at {round(no_of_points)} locations")
            else:
                no_of_points = n_available
                logger.info(f"Class {_class}: not enough pixels, sampling all {int(no_of_points)} locations")
            sample_loc = df[df[CLASS_ATTR] == _class].sample(n=int(round(no_of_points)), random_state=seed)
            samples.append(sample_loc)

    if sampling == "equal_stratified_random":
        classes = np.unique(df[CLASS_ATTR])

        for _class in classes:
            # Use relative proportions of classes to sample df
            no_of_points = n / len(classes)
            n_available = class_ratio[class_ratio["class"] == _class]["n_available"].values[0]
            if n_available >= no_of_points:
                sample_loc = df[df[CLASS_ATTR] == _class].sample(n=int(round(no_of_points)), random_state=seed)
                logger.info(f"Class {_class}: sampling at {round(no_of_points)} locations")
            else:
                no_of_points = n_available
                logger.info(f"Class {_class}: not enough pixels, sampling all {int(no_of_points)} locations")
            sample_loc = df[df[CLASS_ATTR] == _class].sample(n=int(round(no_of_points)), random_state=seed)
            samples.append(sample_loc)

    # Join back into single dataframe
    all_samples = pd.concat([samples[i] for i in range(0, len(samples))])

    # Get pd.multiindex coords as list
    y = [i[0] for i in list(all_samples.index)]
    x = [i[1] for i in list(all_samples.index)]

    # CRS of the DataArray, None if it has none.
    crs = da.odc.crs

    # Create geopandas dataframe and ensure output is in WGS84.
    gdf = gpd.GeoDataFrame(all_samples, crs=crs, geometry=gpd.points_from_xy(x, y)).reset_index()

    gdf = gdf.drop(["latitude", "longitude"], axis=1)

    if crs is not None and gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(WGS84)

    return gdf
