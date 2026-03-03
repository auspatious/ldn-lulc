from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES


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
# aoi can be a (buffered) country multipolygon, or tile polygon from the GridSpec.
def get_geomad_dem_indices(aoi_or_tile: Multipolygon | Polygon, datetime: str) -> xarray.Dataset:

    # Query our GeoMAD STAC-Geoparquet by AOI bbox and datetime.
    # parquet = "s3://data.ldn.auspatious.com/ausp_ls_geomad/0-0-2/ausp_ls_geomad.parquet"
    stac_geoparquet = "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ci_ls_geomad/0-0-2/ci_ls_geomad.parquet" # Non-Pacific
    # stac_geoparquet = "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_geomad/0-0-2/ausp_ls_geomad.parquet" # Pacific
    stac_docs = search_sync(stac_geoparquet, intersects=country_geojson, datetime=datetime_year) # , store=S3Store(bucket="data.ldn.auspatious.com", region="us-west-2"))
    print(f"Found {len(stac_docs)} STAC documents for this AOI and datetime year")
    geomad_items = [Item.from_dict(feature) for feature in stac_docs]
    print(f"Parsed {len(geomad_items)} STAC items")

    bands = list(geomad_items[0].assets.keys())
    print(f"Available bands: {bands}")



    # TODO: Load geomad without any CRS/res options. We want to load it as native.
    geomad_ds = load(
        geomad_items,
        crs=analysis_crs,
        resolution=10, # Do 100 for faster development, but use 10m for final runs.
        groupby="solar_day",
        intersects=country_geojson,
        datetime=datetime_year,
        chunks={}, # Force lazy loading.
    )
    geomad_ds = geomad_ds.squeeze().load() # .load() to read the data into memory, .squeeze() to remove any singleton dimensions.
    print(f"GeoMAD dataset shape: {geomad_ds.dims}")
    geomad_ds


    # Scale observed values
    # Done before calculating indices.
    # Apply scaling to the relevant bands -- Landsat data values are supplied as integers, but it is best practice to scale them to have values between 0 and 1 when doing calculations of indices. Landsat has a scale factor of 0.0000275 and an offset of -0.2. The cell below defines a function for this, and also sets any negative values to nan.
    try:
        bands.remove('count')
    except ValueError:
        pass # If rerunning, count is already removed
    band_names_geomad = [b for b in bands if b.endswith('mad')] # ['smad', 'bcmad', 'emad']
    band_names_geomedian = [b for b in bands if b not in band_names_geomad] # ['red', 'blue', 'green', 'swir16', 'swir22', 'nir08']
    print(band_names_geomad)
    print(band_names_geomedian)

    # Adapted from https://github.com/auspatious/cloud-native-geospatial-eo-workshop/blob/main/02_Cloud_Native_Land_Productivity_For_SDG15_LS.ipynb
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
    geomad_ds.head()


    #### Elevation 
    # TODO: Get slope and aspect too.
    # Load DEM
    dem_items = list(
        catalog.search(
            collections=["cop-dem-glo-30"],
            intersects=country_geojson,
        ).items()
    )

    print(f"Found {len(dem_items)} DEM items")
    print(dem_items[0].to_dict() if dem_items else "NO ITEMS FOUND")

    # Upscales from 30m to 10m.
    dem = load(
        dem_items,
        # geobox=geomad_ds.odc.geobox, # Use the same geobox as geomad for alignment. This will reproject and resample the DEM to match geomad.
        like=geomad_ds,
        resampling="nearest",
        patch_url=sign_url,
    ).squeeze().compute().rename({"data": "elevation"})

    print(dem)

    # Merge
    dem = dem.assign_coords(time=geomad_ds.time)
    geomad_dem = xr.merge([geomad_ds, dem])

    # We probably should keep the distinction between nan and zero... but I dont know if it matters for us.
    # geomad_dem = geomad_dem.fillna(0) # Fill NaN with 0 for exploration. We will keep NaN in the actual training data.

    return geomad_dem

