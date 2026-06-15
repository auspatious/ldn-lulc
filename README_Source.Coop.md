# Geometric Median & Median Absolute Deviation Mosaics for Small Island Developing States (SIDS) and Pacific Community (SPC)'s countries/territories

Annual cloud-free 30 m Landsat mosaics for Small Island Developing States (SIDS) and Pacific Community (SPC)'s countries/territories for 2000-2025, generated using robust pixel compositing methods for long-term environmental monitoring, land cover mapping, coastal change detection, and machine learning applications.

**Created by:** [Auspatious](https://auspatious.com/)

**Repository:** https://source.coop/auspatious/geomad-sids

**Code:** https://github.com/auspatious/ldn-lulc

**License:** [Creative Commons BY](https://creativecommons.org/licenses/by/4.0/)

**Visualisation app:** https://mmufb4pjqf.execute-api.us-west-2.amazonaws.com/

---

## Dataset Summary

| Property | Value |
| - | - |
| Spatial resolution  | 30 m |
| Temporal coverage   | 2000–2025 |
| Temporal resolution | Annual |
| Sensors             | Landsat 5, 7, 8, 9 |
| Products            | Geometric Median, Median Absolute Deviation (MAD) |
| Format              | Cloud-Optimized GeoTIFF |
| Metadata            | Spatio-Temporal Asset Catalog (STAC) |
| Countries/territories | 60 |
| Regions             | 2 |
| Grid tiles processed    | 817 |

---

## Area of Interest

Coverage includes:

- [Small Island Developing States](https://www.un.org/ohrlls/content/about-small-island-developing-states)
- [Pacific Community (SPC)'s countries/territories](https://en.wikipedia.org/wiki/Pacific_Community#Membership)

Many countries/territories belong to both of these groups. In that case, they are placed in the Pacific region.

#### Pacific (22)

- American Samoa `ASM`
- Cook Islands `COK`
- Fiji `FJI`
- French Polynesia `PYF`
- Guam `GUM`
- Kiribati `KIR`
- Marshall Islands `MHL`
- Micronesia `FSM`
- Nauru `NRU`
- New Caledonia `NCL`
- Niue `NIU`
- Northern Mariana Islands `MNP`
- Palau `PLW`
- Papua New Guinea `PNG`
- Pitcairn Islands `PCN`
- Samoa `WSM`
- Solomon Islands `SLB`
- Tokelau `TKL`
- Tonga `TON`
- Tuvalu `TUV`
- Vanuatu `VUT`
- Wallis and Futuna `WLF`

#### Non-Pacific (38)

- Anguilla `AIA`
- Antigua and Barbuda `ATG`
- Aruba `ABW`
- Bahamas `BHS`
- Barbados `BRB`
- Belize `BLZ`
- Bermuda `BMU`
- British Virgin Islands `VGB`
- Cabo Verde `CPV`
- Cayman Islands `CYM`
- Comoros `COM`
- Cuba `CUB`
- Curaçao `CUW`
- Dominica `DMA`
- Dominican Republic `DOM`
- Grenada `GRD`
- Guadeloupe `GLP`
- Guinea-Bissau `GNB`
- Guyana `GUY`
- Haiti `HTI`
- Jamaica `JAM`
- Maldives `MDV`
- Martinique `MTQ`
- Mauritius `MUS`
- Montserrat `MSR`
- Puerto Rico `PRI`
- Saint Kitts and Nevis `KNA`
- Saint Lucia `LCA`
- Saint Vincent and the Grenadines `VCT`
- Seychelles `SYC`
- Singapore `SGP`
- Sint Maarten `SXM`
- Suriname `SUR`
- São Tomé and Príncipe `STP`
- Timor-Leste `TLS`
- Trinidad and Tobago `TTO`
- Turks and Caicos Islands `TCA`
- Virgin Islands, U.S. `VIR`

---

## Regions & Grids

Tiling is done using [ODC Gridspec](https://odc-geo.readthedocs.io/en/latest/_api/odc.geo.gridspec.GridSpec.html) to partition the data into spatial chunks for processing. Two gridspecs are used because the Pacific region straddles the antimeridian, which would cause a single grid in EPSG:6933 to break at that boundary.

| Region | Countries/Territories | Grid tiles | Grid GeoJSON | Output | CRS |
| --- | --- | --- | --- | --- | --- |
| Pacific | 22 | 517 | [sids_pacific_tiles.geojson](https://github.com/auspatious/ldn-lulc/blob/main/ldn/sids_pacific_tiles.geojson) | [dep_ls_geomad](https://source.coop/auspatious/geomad-sids/dep_ls_geomad) | EPSG:3832 |
| Non-Pacific | 38 | 300 | [sids_non_pacific_tiles.geojson](https://github.com/auspatious/ldn-lulc/blob/main/ldn/sids_non_pacific_tiles.geojson) | [ci_ls_geomad](https://source.coop/auspatious/geomad-sids/ci_ls_geomad) | EPSG:6933 |

---

## Products

Output files are Cloud-Optimized GeoTIFFs structured by grid index and year (`x/y/year`). Example:

```
https://source.coop/auspatious/geomad-sids/ci_ls_geomad/0-2-1/118/125/2000/ci_ls_geomad_118_125_2000_bcmad.tif
```

All outputs for a tile/year are included in a STAC item JSON e.g.
```https://data.source.coop/auspatious/geomad-sids/ci_ls_geomad/0-2-1/118/125/2000/ci_ls_geomad_118_125_2000.stac-item.json```

### Geometric Median Mosaic

A cloud-free annual surface reflectance composite generated using the geometric median of all valid observations. Unlike independent per-band median compositing, the geometric median preserves spectral relationships between bands, producing more physically realistic reflectance values.

[See more technical details here.](https://knowledge.dea.ga.gov.au/data/product/dea-geometric-median-and-median-absolute-deviation-landsat/?tab=description#geometric-median)

| Band   | Description              |
| ------ | ------------------------ |
| blue   | Blue                     |
| green  | Green                    |
| red    | Red                      |
| nir08  | Near Infrared 0.8μm      |
| swir16 | Short-wave Infrared 1.6μm |
| swir22 | Short-wave Infrared 2.2μm |

### Median Absolute Deviation (MAD)

A robust measure of temporal variability useful for quantifying uncertainty, identifying unstable surfaces, detecting environmental change, and supporting machine learning workflows.

[See more technical details here.](https://knowledge.dea.ga.gov.au/data/product/dea-geometric-median-and-median-absolute-deviation-landsat/?tab=description#median-absolute-deviation)

| Band   | Description                  |
| ------ | ---------------------------- |
| emad   | Euclidean distance           |
| smad   | Cosine (spectral) distance   |
| bcmad  | Bray–Curtis dissimilarity    |

### Other bands

A `count` band is also included, recording the number of valid observations used per pixel.

---

## Applications

These products are designed for change detection and land surface analysis. Auspatious uses them to classify land use/land cover across SIDS and Pacific countries/territories, with annual outputs feeding into Land Degradation Neutrality calculations for [UN SDG Indicator 15.3.1](https://sdgs.unep.org/article/sdg-indicator-1531) (proportion of land that is degraded over total land area).

---

## Method

### Input Data

Landsat Collection 2 Tier 1 data is preferred. Where insufficient timesteps are available, Tier 2 data is included, and if still insufficient, data from ±1 year is incorporated. A small number of tiles remain incalculable even after these relaxations.

### Cloud Masking

Cloud masking is aggressive - missing pixels are preferred over cloudy outputs. Masked layers include cloud, dilated cloud, cirrus, cloud shadow, and snow (flagged from `qa_pixel`). Additional custom whiteness and blueness indices catch residual unmasked cloud. Morphological filters clean up the mask and remove small isolated pixel groups surrounded by cloud. Saturated pixels are masked using `qa_radsat` (although saturation is much less impactful than cloud).

Snow is included as a mask because of its unlikelihood to be present in this AOI, and its common presence in the input data.

---

## Quality

Some areas remain partially incomplete. Cloud-persistent regions such as Fiji can lack sufficient cloud-free observations in certain years even after broadening the input data window.

---

## Data Access

Data is distributed through Source Cooperative: https://source.coop/auspatious/geomad-sids. The 2 regions are subfolders.

### STAC GeoParquet Index

A combined index for both regions is available at:

```
https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet
```

Queryable directly - no need to download the full catalog. Within seconds you can search (spatially and temporally), load, and visualise the data using the following Python code.

### Search the Index

```python
from odc.stac import load
from rustac import search_sync
from pystac import Item

url = "https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet"
bbox = [166.0, -22.5, 167.0, -21.5]  # Subset of New Caledonia example

search_items = search_sync(url, bbox=bbox, datetime="2023")
items = [Item.from_dict(doc) for doc in search_items]

print(f"Found {len(items)} items for the AOI in New Caledonia in 2023")
```

### Load with odc-stac

```python
ds = load(items, chunks={}, bbox=bbox)  # Lazy load with chunks
print(ds)
```

### Visualise on an interactive map

```python
ds.odc.explore(vmin=7500, vmax=12000)
```

### Visualise using Xarray

```python
ds[["red", "green", "blue"]].isel(time=0).to_array().squeeze().plot.imshow(vmin=7500, vmax=12000)
```
