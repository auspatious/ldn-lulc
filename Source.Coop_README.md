# Geometric Median & Median Absolute Deviation Mosaics for Small Island Developing States (SIDS)

Annual cloud-free Landsat mosaics for Small Island Developing States (SIDS) and selected Pacific territories, generated using robust pixel compositing methods.

The dataset provides annual 30 m Landsat surface reflectance mosaics and associated Median Absolute Deviation (MAD) layers for every year from **2000–2025**, enabling long-term environmental monitoring, land cover mapping, coastal change detection, and machine learning applications.

Created by [Auspatious](https://auspatious.com/).

**Data repository:** https://source.coop/auspatious/geomad-sids

**Processing code:** https://github.com/auspatious/ldn-lulc

## Dataset Summary

| Property            | Value                   |
| ------------------- | ----------------------- |
| Spatial resolution  | 30 m                    |
| Temporal coverage   | 2000–2025               |
| Temporal resolution | Annual                  |
| Sensors             | Landsat 5, 7, 8, 9      |
| Products            | Geometric Median, MAD   |
| Format              | Cloud-Optimized GeoTIFF |
| Metadata            | STAC                    |
| Count Countries/territories     | 60                      |
| Total grid tiles    | 817                     |

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

SMAD
EMAD
BCMAD
Count

## Bands

The geometric median product contains the standard Landsat surface reflectance bands:

| Band  | Description           |
| ----- | --------------------- |
| blue  | Blue                  |
| green | Green                 |
| red   | Red                   |
| nir   | Near Infrared         |
| swir1 | Short-wave Infrared 1 |
| swir2 | Short-wave Infrared 2 |

The MAD product contains corresponding variability layers for each spectral band.

## Countries and Territories Covered

The dataset covers Small Island Developing States (SIDS) and selected Pacific countries and territories.

Coverage includes sovereign states, overseas territories, and dependencies commonly included in SIDS-focused environmental monitoring programs.

Processing extents extend beyond administrative boundaries where required to ensure complete land coverage and consistent tiling.



### Grids/regions
We used 2 regions because of the antimeridian.

1. Pacifc: EPSG:3832. 22 countries/territories. 517 grid tiles intersecting.
2. Non-Pacific. EPSG:6933. 38 countries/territories. 300 grid tiles intersecting.


## Method
TODO:
LS7 buffer years
Cloud masking. Snow.


## Data Access

The dataset is distributed through Source Cooperative:

https://source.coop/auspatious/geomad-sids


### STAC GeoParquet Index

https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet

The GeoParquet index contains STAC metadata for all annual tiles and can be queried directly using DuckDB, GeoPandas, or Apache Arrow-compatible tools without downloading the full catalog.

## Example Usage

### Search the STAC GeoParquet Index

```python
from rustac import search_sync
from pystac import Item, ItemCollection

url = "https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet"

bbox = [166.0, -22.5, 167.0, -21.5]  # New Caledonia example

raw = search_sync(
    url,
    bbox=bbox,
    datetime="2023-01-01/2023-12-31",
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

### Visualise RGB

```python
import matplotlib.pyplot as plt

rgb = (
    ds[["red", "green", "blue"]]
    .to_array()
    .transpose("y", "x", "variable")
    .squeeze()
)

plt.figure(figsize=(10, 10))
plt.imshow(rgb / 3000)
plt.axis("off")
plt.show()
```
