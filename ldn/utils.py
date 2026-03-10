from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES

import xarray as xr
from odc.stac import load
from pystac import Item
from pystac_client import Client
from rustac import search_sync
import numpy as np
from planetary_computer import sign_url
from geopandas import GeoDataFrame
from scipy.ndimage import sobel



SIDS_COUNTRIES_AND_CODES = {
    # Caribbean
    "Anguilla": "AIA",
    "Antigua and Barbuda": "ATG",
    "Aruba": "ABW",
    "Bahamas": "BHS",
    "Barbados": "BRB",
    "Belize": "BLZ",
    "Bermuda": "BMU",
    "British Virgin Islands": "VGB",
    "Cayman Islands": "CYM",
    "Cuba": "CUB",
    "Curaçao": "CUW",
    "Dominica": "DMA",
    "Dominican Republic": "DOM",
    "Grenada": "GRD",
    "Guadeloupe": "GLP",
    "Guyana": "GUY",
    "Haiti": "HTI",
    "Jamaica": "JAM",
    "Martinique": "MTQ",
    "Montserrat": "MSR",
    "Puerto Rico": "PRI",
    "Saint Kitts and Nevis": "KNA",
    "Saint Lucia": "LCA",
    "Saint Vincent and the Grenadines": "VCT",
    "Sint Maarten": "SXM",
    "Suriname": "SUR",
    "Trinidad and Tobago": "TTO",
    "Turks and Caicos Islands": "TCA",
    "Virgin Islands, U.S.": "VIR",
    # Pacific
    "American Samoa": "ASM",
    "Cook Islands": "COK",
    "Fiji": "FJI",
    "French Polynesia": "PYF",
    "Guam": "GUM",
    "Kiribati": "KIR",
    "Marshall Islands": "MHL",
    "Micronesia": "FSM",
    "Nauru": "NRU",
    "New Caledonia": "NCL",
    "Niue": "NIU",
    "Northern Mariana Islands": "MNP",
    "Palau": "PLW",
    "Papua New Guinea": "PNG",
    "Samoa": "WSM",
    "Solomon Islands": "SLB",
    "Timor-Leste": "TLS",
    "Tonga": "TON",
    "Tuvalu": "TUV",
    "Vanuatu": "VUT",
    # Africa, Indian Ocean, Mediterranean, South China Sea (AIMS)
    "Cabo Verde": "CPV",
    "Comoros": "COM",
    "Guinea-Bissau": "GNB",
    "Maldives": "MDV",
    "Mauritius": "MUS",
    "São Tomé and Príncipe": "STP",
    "Seychelles": "SYC",
    "Singapore": "SGP",
}

# Merge dicts, DEP override SIDS if duplicate name
ALL_COUNTRIES = {**SIDS_COUNTRIES_AND_CODES, **DEP_COUNTRIES_AND_CODES}

# Get SIDS countries that are not in DEP for CI Grid use.
NON_DEP_COUNTRIES = {
    k: v
    for k, v in SIDS_COUNTRIES_AND_CODES.items()
    if k not in DEP_COUNTRIES_AND_CODES
}


# TODO: Put this in a more specific/appropriate location.
# This function gets the Geomedian (scaled to floats), GeoMAD, elevation, and indices.
# region_polygon_gdf is a GeoDataFrame of a single region multipolygon in WGS84.
def get_geomad_dem_indices(region_polygon_gdf: GeoDataFrame, stac_geoparquet: str, year: str, catalog: Client) -> xr.Dataset:
    assert len(region_polygon_gdf.geometry) == 1, "region_polygon_gdf must contain at one multipolygon"

    print(region_polygon_gdf.geometry[0].bounds)

    geomad_items = search_sync(stac_geoparquet, bbox=list(region_polygon_gdf.total_bounds), datetime=year)

    geomad_items = [Item.from_dict(doc) for doc in geomad_items]
    print(f"Found {len(geomad_items)} GeoMAD items for this region and year")

    bands = list(geomad_items[0].assets.keys())
    print(f"Available bands: {bands}")

    geomad_ds = load(
        geomad_items,
        # Region is in 4326 which is good for clipping, despite GeoMAD being in 3857 (for pacific region).
        geopolygon=region_polygon_gdf.geometry[0], # Clip to the region polygon bbox to avoid loading whole items (which can cross the antimeridian).
        chunks={}, # Force lazy loading.
    )
    geomad_ds = geomad_ds.drop_vars("count")
    bands.remove('count')

    print(f"GeoMAD dataset loaded CRS (should be native): {geomad_ds.odc.crs.epsg}")
    geomad_ds = geomad_ds.squeeze().load()
    print(f"GeoMAD dataset shape: {geomad_ds.dims}")

    # Scale + indices
    band_names_geomad = [b for b in bands if b.endswith('mad')]
    band_names_geomedian = [b for b in bands if b not in band_names_geomad]

    def scale_offset(band):
        nodata = band == 0
        band = band * 0.0000275 + -0.2
        band = band.clip(0, 1)
        return band.where(~nodata, other=np.nan)

    for band in band_names_geomedian:
        geomad_ds[band] = scale_offset(geomad_ds[band])

    def make_indices(geomad: xr.Dataset) -> xr.Dataset:
        nir = geomad.nir08
        red = geomad.red
        green = geomad.green
        blue = geomad.blue
        swir1 = geomad.swir16
        swir2 = geomad.swir22
        geomad["ndvi"]  = (nir - red)   / (nir + red)
        geomad["ndwi"]  = (green - nir)  / (green + nir)
        geomad["mndwi"] = (green - swir1) / (green + swir1)
        geomad["ndti"]  = (red - green)  / (red + green)
        geomad["bsi"]   = ((swir1 + red) - (nir + blue)) / ((swir1 + red) + (nir + blue))
        geomad["mbi"]   = ((swir1 - swir2 - nir) / (swir1 + swir2 + nir)) + 0.5
        geomad["baei"]  = (red + 0.3) / (green + swir1)
        ndbi = (swir1 - nir) / (swir1 + nir)
        geomad["bui"]   = ndbi - geomad["ndvi"]
        return geomad

    geomad_ds = make_indices(geomad_ds)

    # Now for DEM data do per bbox search and load to avoid loading the whole world for Fiji.
    dem_items = catalog.search(
        collections=["cop-dem-glo-30"],
        bbox=list(region_polygon_gdf.geometry[0].bounds),
        # datetime="2021"
    )
    dem_items = list(dem_items.items())
    print(f"Found {len(dem_items)} unique DEM items for this AOI")

    dem = load(
        dem_items,
        like=geomad_ds, # Needed for alingment.
        resampling="bilinear", # Alternatively nearest. # TODO: Validate resampling method for upsampling DEM.
        patch_url=sign_url,
    ).squeeze().compute().rename({"data": "elevation"}) # Squeeze removes the time dimension, which is not needed for DEM.

    dem_da = dem['elevation']
    dem_vals = dem_da.values.astype("float32")
    res_m = abs(float(dem.x[1] - dem.x[0]))

    dz_dx = sobel(dem_vals, axis=1) / (8 * res_m)
    dz_dy = sobel(dem_vals, axis=0) / (8 * res_m)

    slope  = xr.DataArray(np.degrees(np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))), coords=dem_da.coords, dims=dem_da.dims, name="slope")
    aspect = xr.DataArray((90 - np.degrees(np.arctan2(-dz_dy, dz_dx))) % 360,  coords=dem_da.coords, dims=dem_da.dims, name="aspect")

    dem_ds = xr.Dataset({"elevation": dem_da, "slope": slope, "aspect": aspect})

    # Merge GeoMAD (10m native) and DEM (30m, resampled to 10m GeoMAD grid) on x, y, time.
    dem_ds = dem_ds.assign_coords(time=geomad_ds.time) # Add GeoMAD time coordinate to DEM dataset so they can be merged.

    return xr.merge([geomad_ds, dem_ds])
