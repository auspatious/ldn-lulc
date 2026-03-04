import json
from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES

import xarray as xr
from odc.stac import load
from pystac import Item
from pystac_client import Client
from rustac import search_sync
import numpy as np
from planetary_computer import sign_url
from geopandas import GeoSeries
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
# aoi can be a (buffered) country Geoseries, or tile.
def get_geomad_dem_indices(aoi_or_tile: GeoSeries, year: str, analysis_crs: str, catalog: Client) -> xr.Dataset:

    aoi_geojson = json.loads(aoi_or_tile.to_json(to_wgs84=True))["features"][0]["geometry"]

    # Query our GeoMAD STAC-Geoparquet by AOI bbox and year.
    # parquet = "s3://data.ldn.auspatious.com/ausp_ls_geomad/0-0-2/ausp_ls_geomad.parquet"
    stac_geoparquet = "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ci_ls_geomad/0-0-2/ci_ls_geomad.parquet" # Non-Pacific
    # stac_geoparquet = "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_geomad/0-0-2/ausp_ls_geomad.parquet" # Pacific
    stac_docs = search_sync(stac_geoparquet, intersects=aoi_geojson, datetime=year) # , store=S3Store(bucket="data.ldn.auspatious.com", region="us-west-2"))
    print(f"Found {len(stac_docs)} STAC documents for this AOI and year")
    geomad_items = [Item.from_dict(feature) for feature in stac_docs]
    print(f"Parsed {len(geomad_items)} STAC items")

    bands = list(geomad_items[0].assets.keys())
    print(f"Available bands: {bands}")


    geomad_ds = load(
        geomad_items,
        groupby="solar_day",
        crs=analysis_crs, # Non-pacific data is already in EPSG:6933, but we want to make sure it matches our analysis CRS. Pacific data is in EPSG:3832, so it needs to be reprojected.
        intersects=aoi_geojson,
        datetime=year,
        chunks={}, # Force lazy loading.
    )
    print(f"Native GeoMAD dataset CRS: {geomad_ds.odc.crs.epsg}") # 6933 for non-Pacific, 3832 for Pacific. Should be in the same CRS as our analysis.
    geomad_ds = geomad_ds.squeeze().load() # .load() to read the data into memory, .squeeze() to remove any singleton dimensions.
    print(f"GeoMAD dataset shape: {geomad_ds.dims}")


    # Scale observed values
    # Done before calculating indices.
    # Apply scaling to the relevant bands -- Landsat data values are supplied as integers, but it is best practice to scale them to have values between 0 and 1 when doing calculations of indices. Landsat has a scale factor of 0.0000275 and an offset of -0.2. The cell below defines a function for this, and also sets any negative values to nan.
    try:
        bands.remove('count')
    except ValueError:
        pass # If rerunning, count is already removed
    band_names_geomad = [b for b in bands if b.endswith('mad')] # ['smad', 'bcmad', 'emad']
    band_names_geomedian = [b for b in bands if b not in band_names_geomad] # ['red', 'blue', 'green', 'swir16', 'swir22', 'nir08']


    def scale_offset(band):
        nodata = band == 0
        # Apply a scaling factor and offset to the band
        band = band * 0.0000275 + -0.2
        band = band.clip(0, 1)

        return band.where(~nodata, other=np.nan)

    # Only scale the integer geomedian bands, not the float MAD bands
    for band in band_names_geomedian:
        geomad_ds[band] = scale_offset(geomad_ds[band])


    ### Calculate indices like NDVI (from the Geomedian/GeoMAD bands).
    # Adapted from https://github.com/digitalearthpacific/dep-sdb/blob/main/src/utils.py
    # Use our own function because deafrica calculate_indices is large and clunky.
    def make_indices(geomad: xr.Dataset) -> xr.Dataset:
        nir = geomad.nir08
        red = geomad.red
        green = geomad.green
        blue = geomad.blue
        swir1 = geomad.swir16
        swir2 = geomad.swir22

        # Vegetation
        geomad["ndvi"] = (nir - red) / (nir + red)

        # Water
        geomad["ndwi"] = (green - nir) / (green + nir)
        geomad["mndwi"] = (green - swir1) / (green + swir1)

        # Turbidity
        geomad["ndti"] = (red - green) / (red + green)

        # Soil
        # Bare Soil Index (common form)
        geomad["bsi"] = ((swir1 + red) - (nir + blue)) / ((swir1 + red) + (nir + blue))
        # Modified Bare Soil Index
        geomad["mbi"] = ((swir1 - swir2 - nir) / (swir1 + swir2 + nir)) + 0.5

        # Built-Up
        # Built-up Area Extraction Index (BAEI)
        geomad["baei"] = (red + 0.3) / (green + swir1)
        # Built-Up Index (BUI = NDBI - NDVI)
        ndbi = (swir1 - nir) / (swir1 + nir)
        geomad["bui"] = ndbi - geomad["ndvi"]


        # Shallow water bathymetry. Need to calc it before scaling aparently
        # # Stumpf, calculate off non-scaled data to remove nan/infinities
        # geomad["stumpf"] = np.log(np.abs(geomad.green - geomad.blue)) / np.log(
        #     geomad.green + geomad.blue
        # )

        # Blue over red index
        # geomad["br"] = blue / red

        # # Lyzenga... seems problematic
        # geomad["lyzenga"] = np.log(green / blue)

        return geomad

    geomad_ds = make_indices(geomad_ds)
    print(geomad_ds.head())


    #### Elevation, slope, aspect.
    # Load DEM
    dem_items = list(
        catalog.search(
            collections=["cop-dem-glo-30"],
            intersects=aoi_geojson,
        ).items()
    )

    print(f"Found {len(dem_items)} DEM items")
    print(dem_items[0].to_dict() if dem_items else "NO ITEMS FOUND")

    # Upscales from 30m to 10m.
    dem = load(
        dem_items,
        like=geomad_ds,
        resampling="nearest",
        patch_url=sign_url,
    ).squeeze().compute().rename({"data": "elevation"})

    print(dem)

    dem_da = dem['elevation']
    dem_vals = dem_da.values.astype("float32")
    res_m = abs(float(dem.x[1] - dem.x[0]))

    dz_dx = sobel(dem_vals, axis=1) / (8 * res_m)
    dz_dy = sobel(dem_vals, axis=0) / (8 * res_m)

    slope  = xr.DataArray(np.degrees(np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))), coords=dem_da.coords, dims=dem_da.dims, name="slope")
    aspect = xr.DataArray((90 - np.degrees(np.arctan2(-dz_dy, dz_dx))) % 360,  coords=dem_da.coords, dims=dem_da.dims, name="aspect")

    print("Aspect min:", aspect.min().values, "max:", aspect.max().values)
    print("Slope min:", slope.min().values, "max:", slope.max().values)

    # Merge elevation, slope, aspect, and geomad
    dem_ds = xr.Dataset({"elevation": dem_da, "slope": slope, "aspect": aspect})
    dem_ds = dem_ds.assign_coords(time=geomad_ds.time)
    geomad_dem = xr.merge([geomad_ds, dem_ds])

    # We probably should keep the distinction between nan and zero... but I dont know if it matters for us.
    # geomad_dem = geomad_dem.fillna(0) # Fill NaN with 0 for exploration. We will keep NaN in the actual training data.

    return geomad_dem

