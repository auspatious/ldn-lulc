# Geometric Median & Median Absolute Deviation Mosaics for Small Island Developing States (SIDS)

Annual cloud-free Landsat mosaics for Small Island Developing States (SIDS) and selected Pacific territories, generated using robust pixel compositing methods.

The dataset provides annual 30 m Landsat surface reflectance mosaics and associated Median Absolute Deviation (MAD) layers for every year from **2000–2025**, enabling long-term environmental monitoring, land cover mapping, coastal change detection, and machine learning applications.

Created by [Auspatious](https://auspatious.com/).

**Data repository:** https://source.coop/auspatious/geomad-sids

**Code:** https://github.com/auspatious/ldn-lulc

**License:** Open Data Commons Attribution License (ODC-By)

## Dataset Summary

| Property            | Value                   |
| ------------------- | ----------------------- |
| Spatial resolution  | 30 m                    |
| Temporal coverage   | 2000–2025               |
| Temporal resolution | Annual                  |
| Sensors             | Landsat 5, 7, 8, 9      |
| Products            | Geometric Median, MAD   |
| Format              | Cloud-Optimized GeoTIFF |
| Metadata            | Spatio-Temporal Asset Catalog (STAC)   |
| Count Countries/territories     | 60      |
| Total grid tiles    | 817                  |

## Area of Interests - Small Island Developing States + Digital Earth Pacific countries

The dataset covers Small Island Developing States (SIDS) and selected Pacific countries and territories.

Coverage includes sovereign states, overseas territories, and dependencies commonly included in SIDS-focused environmental monitoring programs.

Processing extents extend beyond administrative boundaries where required to ensure complete land coverage and consistent tiling.

List of SIDs: TODO
List of Pacific countries/territoris: TODO


### Regions/Grid

We use [ODC Gridspec](https://odc-geo.readthedocs.io/en/latest/_api/odc.geo.gridspec.GridSpec.html) to tile the so it can be processed in spatial chunks.

We use 2 gridspecs because of the antimeridian. Otherwise the single grid in 6933 would break at this margin (in geographic CRSs?).

| Region            | Number of Countries/Territories  | Number of grid tiles intersecting | Link to list of countries/territories | Link to geojson of grid tiles | Link to ouput |
| --------- | ------- | ------- | ------- | -- | CRS |
| Pacific  |  22  | 517 | XXX | XXX | https://source.coop/auspatious/geomad-sids/dep_ls_geomad | EPSG:3832 |
| Non-Pacific  | 38  | 300  | XXX | XXX | https://source.coop/auspatious/geomad-sids/ci_ls_geomad | EPSG:6933 |


## Products

Each annual tile includes:

### Geometric Median Mosaic

A cloud-free annual surface reflectance composite generated using the geometric median of all valid observations within the compositing window.

The geometric median preserves spectral relationships between bands better than independent per-band median compositing and generally produces more physically realistic reflectance values.

### Median Absolute Deviation (MAD)

A multiband raster containing the Median Absolute Deviation for each spectral band.

MAD provides a robust measure of temporal variability and can be used to:

* Quantify uncertainty
* Identify unstable surfaces
* Detect environmental change
* Support machine learning workflows

Each output has a single cloud optimised geotiff e.g. https://source.coop/auspatious/geomad-sids/ci_ls_geomad/0-2-1/118/125/2000/ci_ls_geomad_118_125_2000_bcmad.tif. These are structured using their grid index and the year (x, y, year).

## Bands

The geometric median product contains the standard Landsat surface reflectance bands:

| Band  | Description           |
| ----- | --------------------- |
| blue  | Blue                  |
| green | Green                 |
| red   | Red                   |
| nir08 | Near Infrared 08     |
| swir16 | Short-wave Infrared 16 |
| swir22 | Short-wave Infrared 22 |


The MAD product contains corresponding variability layers for each spectral band.

| Band  | Description           |
| ----- | --------------------- |
| emad   | Euclidean distance (EMAD)  |
| smad | Cosine (spectral) distance                 |
| bcmad | Bray Curtis dissimilarity                 |

More technical information here https://knowledge.dea.ga.gov.au/data/product/dea-geometric-median-and-median-absolute-deviation-landsat/?tab=description#median-absolute-deviation


There is also a count band which has the total number of observations used to created these products, per pixel.

## What can these products be used for?

Use these for any change detection analysis.

These products will be used by Auspatious to classfify the SIDs (and Pacific) countries/territories Land Use/Land Cover. These annual outputs will then be used to calculate Land Degredation Neutrality for United Nations' Sustainable Development Goals reporting (specifically [UN SDG Indicator 15.3.1: Proportion of land that is degraded over total land area](https://sdgs.unep.org/article/sdg-indicator-1531)).


## Method

Due to limits in the source data we have some differences in how the calculation runs.
First we search for tier 1 data for the year we are processing, if not enough timesteps are available, we then include tier 2 data, if still not enough is found, we then inlcude data from one year on either side of the year we are creating. In a few cases there is not enough data even with this relaxation of the search parameters.

Cloud masking is very important to the creation of cloud-free mosaics. We aggresively mask cloud, preferring to be missing small areas of output products (where no cloud-free data is available), rather than producing cloudy outputs. In addition to masking cloud, dilated cloud, cirrus from the qa_pixel band, we also mask cloud shadow and pixels flagged as snow. Snow is very unlikely in almost all of our AOI, but it is common in the data. This is important for the aggressive cloud masking that we need. As well as using qa_pixel data to filter pixels, we also use custom whiteness and blueness indices to filter any cloud that was not flagged.

We apply morphological filters to the cloud mask. This cleans up the cloud mask and catches small pixel groups that are surrounded by cloud.

We also mask saturated pixels using the qa_radsat band however this is much less widespread than cloud.


## Data Access

The dataset is distributed through Source Cooperative:

https://source.coop/auspatious/geomad-sids


### STAC GeoParquet Index

https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet

The GeoParquet index contains STAC metadata for all annual tiles and can be queried directly using DuckDB, GeoPandas, or Apache Arrow-compatible tools without downloading the full catalog.

This includes both grids/regions.

Within seconds you can search, load, and visualise this data using the following Python code.

### Search the STAC GeoParquet Index



```python
from rustac import search_sync
from pystac import Item, ItemCollection

url = "https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet"

bbox = [166.0, -22.5, 167.0, -21.5]  # New Caledonia example

raw = search_sync(
    url,
    bbox=bbox,
    datetime="2023",
)

items = [Item.from_dict(doc) for doc in raw]
collection = ItemCollection(items)

print(f"Found {len(collection.items)} items for New Caledonia in 2023")
```

### Load Data with odc-stac

```python
from odc.stac import load

ds = load(
    collection,
    chunks={}, # Lazy load
)

print(ds)
```

### Visualise the GeoMedian (RGB)

```python
import matplotlib.pyplot as plt

rgb = (
    ds[["red", "green", "blue"]] # You can alternaitvely visualise any of these bands: blue, green, red, nir08, swir16, swir22, count, smad, emad, bcmad.
    .isel(time=0) # select first timestep
    .to_array()
    .transpose("y", "x", "variable")
    .squeeze()
)
# Percentile stretch
p2, p98 = rgb.quantile([0.02, 0.98])
rgb_norm = (rgb - p2) / (p98 - p2)
rgb_norm = rgb_norm.clip(0, 1)

plt.figure(figsize=(10, 10))
plt.imshow(rgb_norm)
plt.axis("off")
plt.show()
```


## Quality

This product is missing pixels in some areas. Some areas such as Fiji, for some years are commonly obscured by cloud so even with the broadened inclusion of input data it can still be impossible to create good outputs.
