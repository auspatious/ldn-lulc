# spatial functions

import glob
from typing import Tuple, List, Optional
import geopandas as gpd

country_code = {
    "Antigua and Barbuda": "ATG",
    "Bahamas": "BHS",
    "Barbados": "BRB",
    "Belize": "BLZ",
    "Cabo Verde": "CPV",
    "Comoros": "COM",
    "Cook Islands": "COK",
    "Cuba": "CUB",
    "Dominica": "DMA",
    "Dominican Republic": "DOM",
    "Fiji": "FJI",
    "Grenada": "GRD",
    "Guinea-Bissau": "GNB",
    "Guyana": "GUY",
    "Haiti": "HTI",
    "Jamaica": "JAM",
    "Kiribati": "KIR",
    "Maldives": "MDV",
    "Marshall Islands": "MHL",
    "Mauritius": "MUS",
    "Micronesia (Federated States of)": "FSM",
    "Nauru": "NRU",
    "Niue": "NIU",
    "Palau": "PLW",
    "Papua New Guinea": "PNG",
    "Samoa": "WSM",
    "Sao Tome and Principe": "STP",
    "Seychelles": "SYC",
    "Singapore": "SGP",
    "Solomon Islands": "SLB",
    "St Kitts and Nevis": "KNA",
    "St Lucia": "LCA",
    "St Vincent and the Grenadines": "VCT",
    "Suriname": "SUR",
    "Trinidad and Tobago": "TTO",
    "Timor-Leste": "TLS",
    "Tonga": "TON",
    "Tuvalu": "TUV",
    "Vanuatu": "VUT",
}


def list_countries() -> List[str]:
    """List available countries in the boundaries dataset.

    Returns:
        List[str]: List of country names.
    """
    return list(country_code.keys())


def get_country_boundary(
    country: str, boundaries_path: str = "../data/gadm_boundaries", level: int = 0
) -> gpd.GeoDataFrame:
    """Get country boundary as a GeoDataFrame.

    Args:
        country (str): Country name.
        boundaries_path (str): Path to boundaries file(s). Defaults to "country_boundaries".
    Returns:
        gpd.GeoDataFrame: Country boundary GeoDataFrame.
    """
    boundaries_file = f"{boundaries_path}/gadm41_{country_code[country]}.gpkg"
    gdf = gpd.read_file(boundaries_file, layer=f"ADM_ADM_{level}")
    return gdf


def sids_bounding_boxes(
    boundaries_path: str = "../data/gadm_boundaries",
    country: Optional[str] = None,
    buffer_m: float = 0,
) -> List[Tuple[float, float, float, float]]:
    """Get bounding boxes from boundaries files.

    Args:
        boundaries_path (str): Path to boundaries file(s). Defaults to "country_boundaries".
        country (str, optional): Country name to filter boundaries.
    Returns:
        List[Tuple[float, float, float, float]]: List of bounding boxes (minx, miny, maxx, maxy).
    """

    def _buffer_bounds(
        gdf: gpd.GeoDataFrame, meters: float
    ) -> Tuple[float, float, float, float]:
        if buffer_m == 0:
            return tuple(gdf.total_bounds.tolist())
        orig_crs = gdf.crs
        # choose a metric CRS: try local UTM, fall back to Web Mercator
        try:
            metric_crs = gdf.estimate_utm_crs()
        except Exception:
            metric_crs = "EPSG:3857"
        gdf_m = gdf.to_crs(metric_crs)
        # buffer geometries in metric CRS
        gdf_m["geometry"] = gdf_m.geometry.buffer(meters)
        return tuple(gdf_m.to_crs(orig_crs).total_bounds.tolist())

    if country is not None:
        gdf = get_country_boundary(country, boundaries_path=boundaries_path, level=0)
        return [_buffer_bounds(gdf, buffer_m)]
    else:
        for country in list_countries():
            try:
                gdf = get_country_boundary(
                    country, boundaries_path=boundaries_path, level=0
                )
            except Exception as e:
                print(f"Warning: could not load boundary for {country}: {e}")
            bounds.append(_buffer_bounds(gdf, buffer_m))
        return bounds
