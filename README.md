# Land Degradation Neutrality - Land Use/Land Cover

Creating Land Use/Land Cover (LULC) datasets for the UN Small Island Developing States (SIDS) and Pacific Community (SPC) countries/territories. The installable package lives in the `ldn` folder. There are some helpful notebooks in the `notebooks` folder.

**Tiled Pipeline:**
1. Produce annual GeoMedian/GeoMAD mosaics (2000–2025) per tile and year
2. Build training data from existing LULC products (2020) for representative tiles
3. Train a model using the training data. If future models may be specific to subregions/time periods.
4. Predict LULC for all countries/territories per tile and year.

## Contents

- [Products](#products)
  - [1. Geomedian with GeoMAD](#1-geomedian-with-geomad)
  - [2. Land Use/Land Cover Classification](#2-land-useland-cover-classification)
- [Coverage: Regions & Grids](#coverage-regions--grids)
  - [Area of Interest](#area-of-interest)
  - [Country/Territory Listing](#countryterritory-listing)
  - [Coordinate Reference Systems](#coordinate-reference-systems)
  - [Tile Grids](#tile-grids)
- [Data Access & Output Instances](#data-access--output-instances)
  - [Source.Coop](#1-sourcecoop)
  - [Digital Earth Pacific](#2-digital-earth-pacific)
- [Development](#development)
  - [Quickstart](#quickstart)
  - [Testing & Pre-commit](#testing--pre-commit)
  - [Docker](#docker)
  - [Running on an EC2 VM](#running-on-an-ec2-vm)
  - [Running in Argo](#running-in-argo)
- [Visualisation](#visualisation)
- [CI/CD](#cicd)

---

## Products

This repository creates **two** data products, which share a common footprint and processing grid:

| Property | Value |
| - | - |
| Spatial resolution  | 30 m |
| Temporal coverage   | 2000–2025 |
| Temporal resolution | Annual |
| Sensors             | Landsat 5, 7, 8, 9 |
| Format              | Cloud-Optimized GeoTIFF |
| Metadata            | Spatio-Temporal Asset Catalog (STAC) |
| Countries/territories | 60 |
| Regions (each has its own grid and CRS) | 2 |
| Grid tiles processed | 817 |

Output files are Cloud-Optimized GeoTIFFs structured by grid index and year (`x/y/year`), and are versioned e.g.: https://source.coop/auspatious/geomad-sids/ci_ls_geomad/0-2-1/118/125/2000/ci_ls_geomad_118_125_2000_bcmad.tif

All outputs for a tile/year are bundled into a STAC item JSON, e.g.: https://data.source.coop/auspatious/geomad-sids/ci_ls_geomad/0-2-1/118/125/2000/ci_ls_geomad_118_125_2000.stac-item.json

### Product 1. Geomedian with GeoMAD

#### Geometric Median Mosaic

A cloud-free annual surface reflectance composite generated using the geometric median of all valid observations. Unlike independent per-band median compositing, the geometric median preserves spectral relationships between bands, producing more physically realistic reflectance values. [See more technical details here.](https://knowledge.dea.ga.gov.au/data/product/dea-geometric-median-and-median-absolute-deviation-landsat/?tab=description#geometric-median)

| Band   | Description               |
| ------ | -------------------------- |
| blue   | Blue                       |
| green  | Green                      |
| red    | Red                        |
| nir08  | Near Infrared 0.8μm        |
| swir16 | Short-wave Infrared 1.6μm  |
| swir22 | Short-wave Infrared 2.2μm  |

#### Median Absolute Deviation (MAD)

A robust measure of temporal variability, useful for quantifying uncertainty, identifying unstable surfaces, detecting environmental change, and supporting machine learning workflows. [See more technical details here.](https://knowledge.dea.ga.gov.au/data/product/dea-geometric-median-and-median-absolute-deviation-landsat/?tab=description#median-absolute-deviation)

| Band   | Description                |
| ------ | --------------------------- |
| emad   | Euclidean distance          |
| smad   | Cosine (spectral) distance  |
| bcmad  | Bray-Curtis dissimilarity   |

A `count` band is also included, recording the number of valid observations used per pixel.

#### Input Data

Landsat Collection 2 Tier 1 data is preferred. Where insufficient timesteps are available, Tier 2 data is included; if still insufficient, data from ±1 year is incorporated. A small number of tiles for a small number of years remain incalculable even after these relaxations.

#### Cloud Masking

> Cloud masking is aggressive - **missing pixels are preferred over cloudy outputs.**

- Masked layers: cloud, dilated cloud, cirrus, cloud shadow, and snow (flagged from `qa_pixel`).
- Custom whiteness and blueness indices catch residual unmasked cloud (which lacks good qa data).
- Morphological filters clean up the mask and remove small isolated pixel groups surrounded by cloud.
- Saturated pixels are masked using `qa_radsat` (saturation masking does not have a very large impact compared to cloud).
- Snow is masked because of its unlikelihood in this AOI, and its common presence in the input qa data. This would not be a good idea at higher latitudes.

#### Quality

Some areas remain partially incomplete. Cloud-persistent regions such as Fiji can lack sufficient cloud-free observations in certain years, even after broadening the input data window.

#### Applications

Designed for change detection and land surface analysis. Auspatious uses these mosaics to classify land use/land cover across SIDS and PICTs, with annual outputs feeding Land Degradation Neutrality calculations for [UN SDG Indicator 15.3.1](https://sdgs.unep.org/article/sdg-indicator-1531) (proportion of land that is degraded over total land area).

### Product 2. Land Use/Land Cover Classification

Built from existing LULC products as training data:
- ESA WorldCover
- Impact Observatory Annual Land Use Land Cover
- ESA Climate Change Initiative Land Cover

Our LULC typology is defined [here](https://github.com/auspatious/ldn-lulc/blob/main/typology/typology.md).

Further design docs (private):
- [LULC Datasets](https://docs.google.com/document/d/1VFGNH0yIV0rCFdRfNmv_y78q8dpZwa_twbYsDjHBj2k/edit?tab=t.0#heading=h.szysksgdlwi1)
- [Global Land Use Land Cover Datasets and Their Relevance for SIDS LDN](https://docs.google.com/document/d/14P9XUaBah_9iYNSw8x43k6w2s490wJ4gGxmnOytTKX8/edit?tab=t.0#heading=h.nia7409tueh1)

#### Source LULC Product Attributes

| Attribute | ESA WorldCover | Impact Observatory 10m Annual LULC (v2) | ESA CCI Land Cover |
| - | - | - | - |
| Provider | ESA / VITO | Impact Observatory (with Esri & Microsoft) | ESA Climate Change Initiative |
| Spatial resolution | 10 m | 10 m | 300 m |
| Temporal coverage | 2020, 2021 | 2017–2023 (annual) | 1992–2022 (annual) |
| Update frequency | One-off (2 epochs only) | Annual (new year added each January) | Annual |
| Native classes | 11 | 9 | 37 (UN LCCS nomenclature) |
| Source sensors | Sentinel-1 (SAR) + Sentinel-2 (optical) | Sentinel-2 (optical) | Multi-sensor time series: MERIS, AVHRR, SPOT-VGT, PROBA-V, Sentinel-3 |
| Method | CatBoost classifier on multi-feature stack, expert-rule post-processing | Deep learning model trained on human-labelled pixels, per-year composite | GlobCover unsupervised classification chain |
| Quality/flag bands used in this pipeline | `input_quality.1/2/3` (per-season observation quality) | None available | `processed_flag`, `change_count`, `observation_count` |

2020 is used as the first training data year due to the temoral overlap between the three products in that year.

All three are reprojected/aligned onto the 30m GeoMAD tile grid before being reduced to our 7-class `level1` typology (see mapping tables above) and combined via 2-of-3 agreement to generate training samples. That means that the two 10m products (ESA WC & IO) are downsampled while ESA CCI LC is upsampled from 300m.

#### Training data

For a given tile and year, training samples are generated by finding where the three source LULC products agree, then sampling from that agreement map:

1. **Load & quality-filter each product** onto the tile's GeoMAD grid:

   | Product | Quality filter |
   | - | - |
   | ESA WorldCover | At least 1 valid Sentinel observation in ≥2 of 3 seasons (pixels with all-nodata quality flags are not penalised) |
   | ESA CCI-LC | Processed, stable class (no change), and ≥3 observations - relaxed to ≥1 observation if the strict filter rejects every pixel in the tile |
   | Impact Observatory (IO) | No quality filter available |

2. **Find 2-of-3 agreement:** a pixel is kept where at least 2 of the 3 products agree on class, requiring ≥2 products to have valid data at that pixel. A 3×3 neighbourhood minimum filter is then applied so isolated agreeing pixels (with no agreeing neighbours) are dropped.
3. **Stratified random sampling** from the agreement map, masked to areas with valid GeoMAD data. Default: 2,100 total samples, minimum 300 per class.
4. **Feature extraction:** GeoMAD reflectance bands, spectral indices, and DEM terrain features (elevation, slope, aspect) are sampled at each point's location.
5. **Cleanup:** samples with NaN feature values are dropped. K-Means-based per-class outlier filtering (removing the furthest-from-centroid samples within each class) is implemented but currently disabled pending an ablation study on whether it improves the model.
6. **Output:** written locally as GeoJSON + CSV and uploaded to S3 as CSV. Outputs are versioned and record their region, tile index, and year. Source.Coop is not currently supported as a training-data destination.

Tiles crossing the antimeridian (e.g. parts of Kiribati, Fiji) are handled specially throughout: country geometries are split/fixed at ±180°, and LULC products are loaded and reprojected in east/west halves before merging.

**Representative training tiles** (Pacific region) were hand-picked to span major environment types:

| Country | Environment |
| - | - |
| Papua New Guinea | Dense tropical rainforest, highland montane forest, river delta |
| Kiribati | Low-lying coral atoll / open-ocean & lagoon |
| Vanuatu | Active volcanic islands, crater lakes, lava fields |
| Samoa | Elevated volcanic interior, waterfalls, reef/beach coastline |
| Fiji | Mixed urban + agricultural (sugarcane), mangrove wetlands, Suva urban area |
| Palau | Raised limestone/rock island jungle |
| New Caledonia | Maquis shrubland / lagoon |

#### Model & Prediction

Classification runs per tile/year using a pre-trained **random forest** model (loaded from a local `.joblib` file or downloaded from a S3 URL):

1. **Search:** the tile's GeoMAD item is located via its STAC-GeoParquet index, matched by tile ID and year.
2. **Load:** GeoMAD bands are loaded through an antimeridian-safe loader that queries by geopolygon (not geobox), then crops back to the exact tile extent - this avoids the failures a plain bbox/geobox load hits on tiles spanning ±180°.
3. **Feature prep:** reflectance bands are scaled, spectral indices computed, and DEM terrain features (elevation, slope, aspect) merged in - mirroring the training feature set.
4. **Predict:** the random forest outputs a per-class probability for every pixel. Final classification is the highest-probability class, unless it falls below a configurable `probability_threshold`, in which case the pixel is set to nodata. Per-class probability bands (`probability_1`, `probability_2`, …) are retained alongside the classification band.
5. **Write:** output COGs and STAC metadata are written to S3.

Processing is distributed with Dask (configurable workers/threads/memory), and supports faster low-fidelity modes for testing:
- `decimated`: runs at 10× lower resolution (so 100x less pixels)
- `integration_test`: runs on a 5×5 pixel crop with a single worker, for fast CI checks/testing.

---

## Coverage: Regions & Grids

### Area of Interest

Coverage is defined by two country lists, merged:

1. [Small Island Developing States (UN)](https://www.un.org/ohrlls/content/list-sids)
2. [Pacific Island Countries and Territories (SPC)](https://www.spc.int/our-members/) - included per an agreement between Auspatious and Pacific Community (SPC) to collaborate, in exchange for use of the Digital Earth Pacific platform

Countries/territories belonging to **both** lists are placed in the **Pacific** region. Country boundaries are sourced from [UC Davis GADM](https://geodata.ucdavis.edu/gadm/).

### Country/Territory Listing

<details>
<summary><b>Pacific (n=22)</b></summary>

| Country/Territory | Code |
| - | - |
| American Samoa | `ASM` |
| Cook Islands | `COK` |
| Fiji | `FJI` |
| French Polynesia | `PYF` |
| Guam | `GUM` |
| Kiribati | `KIR` |
| Marshall Islands | `MHL` |
| Micronesia | `FSM` |
| Nauru | `NRU` |
| New Caledonia | `NCL` |
| Niue | `NIU` |
| Northern Mariana Islands | `MNP` |
| Palau | `PLW` |
| Papua New Guinea | `PNG` |
| Pitcairn Islands | `PCN` |
| Samoa | `WSM` |
| Solomon Islands | `SLB` |
| Tokelau | `TKL` |
| Tonga | `TON` |
| Tuvalu | `TUV` |
| Vanuatu | `VUT` |
| Wallis and Futuna | `WLF` |

</details>

<details>
<summary><b>Non-Pacific (n=38)</b></summary>

| Country/Territory | Code |
| - | - |
| Anguilla | `AIA` |
| Antigua and Barbuda | `ATG` |
| Aruba | `ABW` |
| Bahamas | `BHS` |
| Barbados | `BRB` |
| Belize | `BLZ` |
| Bermuda | `BMU` |
| British Virgin Islands | `VGB` |
| Cabo Verde | `CPV` |
| Cayman Islands | `CYM` |
| Comoros | `COM` |
| Cuba | `CUB` |
| Curaçao | `CUW` |
| Dominica | `DMA` |
| Dominican Republic | `DOM` |
| Grenada | `GRD` |
| Guadeloupe | `GLP` |
| Guinea-Bissau | `GNB` |
| Guyana | `GUY` |
| Haiti | `HTI` |
| Jamaica | `JAM` |
| Maldives | `MDV` |
| Martinique | `MTQ` |
| Mauritius | `MUS` |
| Montserrat | `MSR` |
| Puerto Rico | `PRI` |
| Saint Kitts and Nevis | `KNA` |
| Saint Lucia | `LCA` |
| Saint Vincent and the Grenadines | `VCT` |
| Seychelles | `SYC` |
| Singapore | `SGP` |
| Sint Maarten | `SXM` |
| Suriname | `SUR` |
| São Tomé and Príncipe | `STP` |
| Timor-Leste | `TLS` |
| Trinidad and Tobago | `TTO` |
| Turks and Caicos Islands | `TCA` |
| Virgin Islands, U.S. | `VIR` |

</details>

Per-country maps: [browse here](https://github.com/auspatious/ldn-lulc/tree/main/notebooks/maps/countries).

### Coordinate Reference Systems

The two regions use different CRSs because a single geographic-latitude grid would be less accurate, and a single projected grid can't cleanly span the antimeridian:

| Region | Countries/Territories | Grid tiles | CRS | Region map |
| --- | --- | --- | --- | --- |
| Pacific | 22 | 517 | EPSG:3832 (WGS 84 / PDC Mercator) | [map](https://github.com/auspatious/ldn-lulc/blob/main/notebooks/maps/regions/Pacific.jpg) |
| Non-Pacific | 38 | 300 | EPSG:6933 (WGS 84 / NSIDC EASE-Grid 2.0 Global, Cylindrical Equal Area) | [map](https://github.com/auspatious/ldn-lulc/blob/main/notebooks/maps/regions/Non-Pacific.jpg) |

Notes:
- Two gridspecs are used because the Pacific region crosses the antimeridian, which would break a single EPSG:6933 grid at that boundary.
- EPSG:6933 is valid globally up to mid-latitudes (covering all SIDS), but only from -180° to 180° longitude - tiles must "wrap" both directions at the antimeridian, causing overlap. Coordinates are technically not continuous across the antimeridian in this CRS, causing issues for Fiji and other antimeridian crossing areas.
- Tiling uses [ODC Gridspec](https://odc-geo.readthedocs.io/en/latest/_api/odc.geo.gridspec.GridSpec.html) to partition data into spatial chunks for parallel processing.

### Tile Grids

Each tile is 30 m resolution in the target CRS, 96 km (3,200 pixels) per side; 817 tiles total across both regions.

| Grid | Tile count | GeoJSON |
| - | - | - |
| All tiles | 817 | [sids_all_tiles.geojson](https://github.com/auspatious/ldn-lulc/blob/main/ldn/sids_all_tiles.geojson) |
| Pacific | 517 | [sids_pacific_tiles.geojson](https://github.com/auspatious/ldn-lulc/blob/main/ldn/sids_pacific_tiles.geojson) |
| Non-Pacific | 300 | [sids_non_pacific_tiles.geojson](https://github.com/auspatious/ldn-lulc/blob/main/ldn/sids_non_pacific_tiles.geojson) |

---

## Data Access & Output Instances

### 1. Source.Coop

| Region | Output path | CRS |
| --- | --- | --- |
| Pacific | [dep_ls_geomad](https://source.coop/auspatious/geomad-sids/dep_ls_geomad) | EPSG:3832 |
| Non-Pacific | [ci_ls_geomad](https://source.coop/auspatious/geomad-sids/ci_ls_geomad) | EPSG:6933 |

Data products:
1. https://source.coop/auspatious/geomad-sids
2. https://source.coop/auspatious/lulc-sids

Both regions are written to region-specific subfolders but indexed to a common parent directory. See [README_Source.Coop.md](https://github.com/auspatious/ldn-lulc/blob/main/README_Source.Coop.md) for full details.


#### STAC GeoParquet Index

A combined index for both regions, queryable directly - no need to download the full catalog:

```
https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet
```

#### How to use this product in Python:

**Search the index:**

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

**Load with odc-stac:**

```python
ds = load(items, chunks={}, bbox=bbox)  # Lazy load with chunks
print(ds)
```

**Visualise on an interactive map:**

```python
ds.odc.explore(vmin=7500, vmax=12000)
```

**Visualise with Xarray:**

```python
ds[["red", "green", "blue"]].isel(time=0).to_array().squeeze().plot.imshow(vmin=7500, vmax=12000)
```

### 2. Digital Earth Pacific

Environments: **Staging** and **Production**. Only the Pacific region is written here (plus some preliminary non-Pacific LULC outputs). Because of this the STAC-Geoparquet is located within the single region's folder (unlike Source Coop's per-region folders).

**Staging GeoMAD:** https://dep-public-staging.s3.us-west-2.amazonaws.com/index.html?prefix=dep_ls_geomad/

**Staging LULC:** This also holds training data and the random forest model.
https://dep-public-staging.s3.us-west-2.amazonaws.com/index.html?prefix=dep_ls_lulc/

**Production:** The bucket `dep-public-data` can be explored in the STAC API: https://stac-browser.digitalearthpacific.org/collections/dep_ls_geomad

---

## Development

### Quickstart

**1. Install GDAL, Rust (for datacube-compute), and uv**

```bash
brew upgrade gdal

brew install rustup
rustup-init
export PATH="$HOME/.cargo/bin:$PATH"

brew install uv
```

**2. Install dependencies (including dev group)**

```bash
uv sync
```

**3. Set environment variables**

Copy `.env.example` to `.env`, then:

```bash
source .env
```

You need to set `AWS_PROFILE`.

**4. AWS access**

See [DEP's AWS SSO docs](https://github.com/digitalearthpacific/internal-documentation/blob/main/technical/1-systems-access.md).

```bash
aws configure sso --profile <profile_name>
aws sso login --profile <profile_name>
export AWS_PROFILE=<profile_name>
```

**5. Run the CLI tool**

```bash
uv run ldn --help
uv run make grid-list-countries-all
```

**Adding a dependency:**

```bash
uv add --group dev pytest

# Project-specific packages:
uv add "dep-tools @ git+https://github.com/digitalearthpacific/dep-tools.git"
uv add "datacube-compute @ git+https://github.com/auspatious/datacube-compute.git"
```

### Testing & Pre-commit

**Run tests:**

```bash
uv run pytest                              # all tests
uv run pytest ldn/tests/test_mosaic.py     # single file
```

**Pre-commit hooks** (formats Python, YAML, and JSON):

```bash
uv run pre-commit install
```

If hooks in `.pre-commit-config.yaml` change, re-run against all files:

```bash
uv run pre-commit run --all-files
```

### Docker

Build the image locally with [Buildx](https://docs.docker.com/buildx/working-with-buildx/):

```bash
docker buildx build . --tag ldn-lulc:dev
docker run --rm ldn-lulc:dev ldn --help
```

Run any command in the container:

```bash
docker run --rm ldn-lulc:dev ldn grid list-countries
```

### Running on an EC2 VM

Much faster than running locally.

1. Set up SSH access.
2. SSH into the VM, e.g. "Alex Hack Box 2026": `ssh -i ~/.ssh/id_ed25519 ubuntu@44.230.85.235`
3. Pull the code in the VM.
4. Install dependencies (same steps as Quickstart).
5. Run inside `tmux` so the job survives disconnects:

```bash
tmux new -s geomad
uv run make geomad-2000-2025
# Detach: Ctrl+B, D - job keeps running
tmux attach -t geomad       # reattach
tmux kill-session -t geomad # kill from outside
```

### Running in Argo

Argo Workflow templates are defined [here](https://github.com/auspatious/ldn-lulc/tree/main/templates). Workflows exist for:

- Creating geomedian/geomad
- Creating LULC from geomad
- Indexing either dataset to STAC-Geoparquet

These workflows use the built image in GHCR, which packages the LDN Python toolkit.

---

## Visualisation (TODO: Update to Vite app is WIP)

TODO: This is close to being redeployed as a Vite app which reads COGs straight from S3 and renders them in the client with Deck.gl-raster.

---

## CI/CD

A GitHub Actions workflow (`.github/workflows/ldn-lint-build-test-push.yml`) lints, builds, tests, and pushes the Docker image to GitHub Container Registry (GHCR).

**Triggers** (only scoped to changes to the LDN package which affect the image):
- Manual dispatch from the GitHub UI
- Any pull request, to any branch
- Push to `main`
- Release creation

Superseded runs for the same workflow + ref are automatically cancelled.

**Jobs:**

1. **`lint`** - runs [Ruff](https://github.com/astral-sh/ruff-action) over the `ldn` directory.
2. **`build-test-push`** (depends on `lint` passing):
   - Logs in to GHCR (only if running on `main` or for a release).
   - Derives a version string via `git describe --tags`.
   - Builds the `test`-target image and runs the test suite inside it.
   - Builds the `final`-target (production) image and smoketests it with `ldn --help`.
   - **Only pushes to GHCR if on `main` or for a release** - every PR gets built and tested, but nothing is published from a PR.
     - On `main`: tags and pushes `<version>` and `latest`.
     - On release: tags and pushes `<release-tag>` and `latest`.

Action versions are pinned to full 40-character commit SHAs (not tags) for supply-chain security.
