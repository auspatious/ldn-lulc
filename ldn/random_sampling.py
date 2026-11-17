# Forked from https://github.com/frontiersi/FAO_LC_workshop_Rwanda/blob/main/random_sampling.py
import logging

import geopandas as gpd
import numpy as np
import odc.geo.xr  # noqa: F401  Registers the .odc accessor.
import pandas as pd
from xarray import DataArray

from ldn.utils import CLASS_ATTR, LULC_NODATA, WGS84

logger = logging.getLogger(__name__)


def random_sampling(
    da: DataArray,
    n: int,
    class_n_range: dict[int, tuple[int, int]],
    seed: int | None,
):
    """Stratified random sample of a classified array.

    Each class gets points in proportion to its area out of n, clipped to its (min, max) in class_n_range, and to the
    number of pixels it has.

    Args:
        da: Classified 2D DataArray with latitude and longitude dims, LULC_NODATA for nodata.
        n: Total number of points to share between classes by area.
        class_n_range: (min, max) points for each class value.
        seed: Random seed so the sampled locations are reproducible. None for non-reproducible sampling.

    Returns:
        GeoDataFrame of sample points with class labels, in WGS84 if da has a CRS.
    """
    da = da.squeeze()
    df = da.to_dataframe(name=CLASS_ATTR)
    df = df[df[CLASS_ATTR] != LULC_NODATA]

    n_available = df[CLASS_ATTR].value_counts()
    logger.info(f"Pixels per class:\n{n_available}")

    samples = []
    for _class, available in n_available.items():
        low, high = class_n_range[_class]
        n_points = min(int(np.clip(round(n * available / len(df)), low, high)), available)
        logger.info(f"Class {_class}: sampling {n_points} of {available} pixels")
        samples.append(df[df[CLASS_ATTR] == _class].sample(n=n_points, random_state=seed))

    # Join back into single dataframe
    all_samples = pd.concat(samples)

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
