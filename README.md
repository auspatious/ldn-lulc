# Land Degradation Neutrality - Land Use/Land Cover

This repository makes Land Use/Land Cover (LULC) datasets for the UN Small Island Developing States (SIDS) and for the countries and territories of the Pacific Community (SPC). The `ldn` folder contains the installable package. The `notebooks` folder contains notebooks for analysis.

The pipeline processes the data in tiles:
1. Make annual GeoMedian and GeoMAD mosaics (2000 to 2025) for each tile and year.
2. Make training data from existing LULC products (2017 to 2021) for representative tiles.
3. Train a model with the training data. Future models can be specific to a subregion or a time period.
4. Predict LULC for all countries and territories, for each tile and year.

## Contents

- [Products](#products)
  - [Product 1. Geomedian with GeoMAD](#product-1-geomedian-with-geomad)
  - [Product 2. Land Use/Land Cover Classification](#product-2-land-useland-cover-classification)
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
- [Visualisation](#visualisation-wip)
- [CI/CD](#cicd)
  - [Releasing](#releasing)

---

## Products

This repository makes two data products. They use the same footprint and the same processing grid.

| Property | Value |
| - | - |
| Spatial resolution  | 30 m |
| Temporal coverage   | 2000 to 2025 |
| Temporal resolution | Annual |
| Sensors             | Landsat 5, 7, 8, 9 |
| Format              | Cloud-Optimized GeoTIFF (COG) |
| Metadata            | Spatio-Temporal Asset Catalog (STAC) |
| Countries/territories | 60 |
| Regions (each has its own grid and CRS) | 2 |
| Grid tiles processed | 817 |

The output files are COGs in folders by grid index and year (`x/y/year`). Each output has a version. For example: https://source.coop/auspatious/geomad-sids/ci_ls_geomad/0-2-1/118/125/2000/ci_ls_geomad_118_125_2000_bcmad.tif

One STAC item JSON file contains all the outputs for a tile and year. For example: https://data.source.coop/auspatious/geomad-sids/ci_ls_geomad/0-2-1/118/125/2000/ci_ls_geomad_118_125_2000.stac-item.json

#### STAC metadata

Each item records its provenance, so it describes itself without a collection:

- [Processing extension](https://github.com/stac-extensions/processing): `processing:version` (the ldn version),
  `processing:software` (the exact package versions, and the commit for git dependencies) and `processing:datetime` (UTC).
- Providers: Digital Earth Pacific (processor, and host when the data is not on Source Cooperative), Source Cooperative
  (host), Auspatious (processor, with `processing:facility`, level and lineage), USGS and NASA.
- Links:
  - `derived_from` to each input item that was used. For GeoMAD, these are the Landsat scenes. For LULC, these are the
    GeoMAD items of the tile and its neighbours, for the year and for each year used to fill gaps.
  - The LULC model.
  - The [visualisation](https://auspatious.github.io/ldn-lulc).
- LULC only: the [classification extension](https://github.com/stac-extensions/classification) on the `classification`
  asset describes each class (value, name, title, colour and nodata). The title of each `probability_{class value}`
  asset is the name of its class.

### Product 1. Geomedian with GeoMAD

#### Geometric Median Mosaic

The geometric median mosaic is a cloud-free annual surface reflectance composite. It is the geometric median of all valid observations in the year. A median of each band by itself changes the relationships between the bands. The geometric median keeps these relationships, so the reflectance values are more physically realistic. [Technical details.](https://knowledge.dea.ga.gov.au/data/product/dea-geometric-median-and-median-absolute-deviation-landsat/?tab=description#geometric-median)

GeoMAD is available here:
- Source.Coop (Pacific and non-Pacific) v0-2-1 https://source.coop/auspatious/geomad-sids
- DEP Public Staging (Pacific only) v0-2-1 https://dep-public-staging.s3.us-west-2.amazonaws.com/index.html?prefix=dep_ls_geomad/0-2-1/
- DEP Public Production (Pacific only) v0-3-0 https://data.digitalearthpacific.org/#dep_ls_geomad/0-3-0/


| Band   | Description               |
| ------ | -------------------------- |
| blue   | Blue                       |
| green  | Green                      |
| red    | Red                        |
| nir08  | Near Infrared 0.8μm        |
| swir16 | Short-wave Infrared 1.6μm  |
| swir22 | Short-wave Infrared 2.2μm  |

#### Median Absolute Deviation (MAD)

The MAD bands measure how much the surface changes during the year. You can use them to measure uncertainty, to find unstable surfaces, to detect environmental change and as machine learning features. [Technical details.](https://knowledge.dea.ga.gov.au/data/product/dea-geometric-median-and-median-absolute-deviation-landsat/?tab=description#median-absolute-deviation)

| Band   | Description                |
| ------ | --------------------------- |
| emad   | Euclidean distance          |
| smad   | Cosine (spectral) distance  |
| bcmad  | Bray-Curtis dissimilarity   |

The `count` band gives the number of valid observations for each pixel.

#### Input Data

The pipeline uses Landsat Collection 2 Tier 1 data first. If there are not sufficient time steps, it also uses Tier 2 data. If there are still not sufficient time steps, it also uses data from 1 year before and 1 year after. Some tiles have no result for some years, even with these changes.

#### Cloud Masking

> The cloud mask is aggressive. **A missing pixel is better than a cloudy pixel.**

- The mask includes cloud, dilated cloud, cirrus, cloud shadow and snow, from the `qa_pixel` flags.
- Custom whiteness and blueness indices find cloud that the QA data does not flag.
- Morphological filters clean the mask. They remove small groups of pixels that cloud surrounds.
- The `qa_radsat` band masks saturated pixels. This has a much smaller effect than the cloud mask.
- Snow is masked because there is almost no snow in the area of interest, and the QA data often flags snow. Do not do this at high latitudes.

#### Quality

Some areas have gaps. Areas with persistent cloud, for example Fiji, do not have sufficient cloud-free observations in some years, even with the wider input data window.

#### Applications

The mosaics are for change detection and land surface analysis. Auspatious uses them to classify LULC across SIDS and PICTs. The annual outputs are inputs to the Land Degradation Neutrality calculations for [UN SDG Indicator 15.3.1](https://sdgs.unep.org/article/sdg-indicator-1531) (the proportion of land that is degraded over total land area).

### Product 2. Land Use/Land Cover Classification

The training data comes from these existing LULC products:
- ESA WorldCover
- Impact Observatory Annual Land Use Land Cover
- ESA Climate Change Initiative Land Cover

Our LULC typology is [here](https://github.com/auspatious/ldn-lulc/blob/main/typology/typology.md).

Other design documents (private):
- [LULC Datasets](https://docs.google.com/document/d/1VFGNH0yIV0rCFdRfNmv_y78q8dpZwa_twbYsDjHBj2k/edit?tab=t.0#heading=h.szysksgdlwi1)
- [Global Land Use Land Cover Datasets and Their Relevance for SIDS LDN](https://docs.google.com/document/d/14P9XUaBah_9iYNSw8x43k6w2s490wJ4gGxmnOytTKX8/edit?tab=t.0#heading=h.nia7409tueh1)

#### Source LULC Product Attributes

| Attribute | ESA WorldCover | Impact Observatory 10m Annual LULC (v2) | ESA CCI Land Cover |
| - | - | - | - |
| Provider | ESA / VITO | Impact Observatory (with Esri and Microsoft) | ESA Climate Change Initiative |
| Spatial resolution | 10 m | 10 m | 300 m |
| Temporal coverage (on Planetary Computer) | 2020, 2021 | 2017 to 2023 (annual) | 1992 to 2020 (annual) |
| Update frequency | One-off (2 epochs only) | Annual (a new year each January) | Annual |
| Native classes | 11 | 9 | 37 (UN LCCS nomenclature) |
| Source sensors | Sentinel-1 (SAR) and Sentinel-2 (optical) | Sentinel-2 (optical) | Multi-sensor time series: MERIS, AVHRR, SPOT-VGT, PROBA-V, Sentinel-3 |
| Method | CatBoost classifier on a multi-feature stack, with expert-rule post-processing | Deep learning model trained on human-labelled pixels, with a composite for each year | GlobCover unsupervised classification chain |
| Quality/flag bands used in this pipeline | `input_quality.1/2/3` (observation quality for each season) | None available | `processed_flag`, `change_count`, `observation_count` |

There is training data for each year from 2017 to 2021 in which at least 2 products have data (`TRAINING_DATA_YEARS`):
- 2017 to 2019: ESA CCI and IO.
- 2020: all 3 products.
- 2021: ESA WorldCover and IO.

More years show the model more conditions. For example, 2021 is a dry La Nina year in the central Pacific (see `plan-label-propagation2.md`).

The pipeline reprojects the products onto the 30 m GeoMAD tile grid. Then it changes their classes to our 7-class `level1` typology (see `typology_mapping.yaml`). The training samples come from pixels where at least 2 products agree. Thus the two 10 m products (ESA WorldCover and IO) are downsampled, and ESA CCI is upsampled from 300 m.

### 3 datasets: training, test, validation

The datasets are for this workflow:
1. Make training data that represents all regions (2017 to 2021, `make training-data-generate-years`).
2. Classify LULC for all years (2000 to 2025) over a small area.
3. Validate the results against the test data with code.
4. Do steps 2 and 3 again until the results are good.
5. Run representative areas over all years, and look at the results on a map.
6. If the map is good, run all tiles. If not, go back to step 2.
7. Write a formal validation report with the validation data.

#### 1. Training data

For each tile and year, the pipeline finds where the source LULC products with data for that year agree. Then it takes samples from this agreement map.

The training data is here: https://dep-public-staging.s3.us-west-2.amazonaws.com/index.html?prefix=dep_ls_lulc/training_data/

1. The pipeline loads each product onto the GeoMAD grid of the tile, and applies a quality filter:

   | Product | Quality filter |
   | - | - |
   | ESA WorldCover | At least 1 Sentinel-1 observation and at least 2 Sentinel-2 observations. Negative quality values are nodata and are not masked. These are -1 (nodata) and an undocumented -2, which covers full files over northern Fiji |
   | ESA CCI-LC | Processed, a stable class (no change), and at least 3 observations. If this filter removes all pixels in the tile, the minimum is 1 observation |
   | Impact Observatory (IO) | No quality filter is available |

2. It keeps a pixel where at least 2 products agree on its class (both of 2 products, or 2 of 3). Nodata does not count as agreement. Then a 3x3 minimum filter keeps a pixel only if all of its neighbourhood agrees.
3. It takes a stratified random sample from the agreement map, only where there is valid GeoMAD data. The default is 2,100 samples, with a minimum of 300 for each class.
4. It gets the model features (see [Model features](#model-features)) at each sample point.
5. It removes the samples that have NaN feature values. The code also has a K-Means outlier filter for each class, which removes the samples furthest from the class centroid. This filter is disabled until an ablation study shows if it makes the model better.
6. It writes GeoJSON and CSV locally, and CSV to S3. The outputs have a version, and record the region, the tile index and the year. Source.Coop is not a supported destination for training data.

Some tiles cross the antimeridian (for example, parts of Kiribati and Fiji). For these tiles, the pipeline splits the country geometries at 180 degrees. It loads and reprojects the LULC products in an east half and a west half, and then merges the halves.

The Pacific training tiles were selected by hand to include the main environment types:

| Country | Environment |
| - | - |
| Papua New Guinea | Dense tropical rainforest, highland montane forest, river delta |
| Kiribati | Low-lying coral atoll, open ocean and lagoon |
| Vanuatu | Active volcanic islands, crater lakes, lava fields |
| Samoa | Elevated volcanic interior, waterfalls, reef and beach coastline |
| Fiji | Mixed urban and agricultural (sugarcane), mangrove wetlands, Suva urban area |
| Palau | Raised limestone island jungle |
| New Caledonia | Maquis shrubland, lagoon |

#### 2. Test data

https://dep-public-staging.s3.us-west-2.amazonaws.com/index.html?prefix=dep_ls_lulc/test_data/

The test data is a frozen set of held-out tiles (`TEST_TILES` in `ldn/training_data.py`). It is only for comparing data and model versions. Do not train a model with it. `--split train` does not make these tiles.

Make the test data with `make test-data-generate` (`--split test`). It uses a fixed random seed, and is written to `dep_ls_lulc/test_data/{TEST_DATA_VERSION}/{region}/`. It cannot be overwritten. To make it again, change `TEST_DATA_VERSION`. Human or AI validation of the LULC outputs is a different dataset, made later.

The number of tiles to use (the Pacific has about 517 tiles and the non-Pacific region about 300, all with SIDS land):

| Set | Pacific | Non-Pacific | Notes |
| - | - | - | - |
| Test | 15 | 11 | About 3% of tiles. Full tiles are held out (not only points), so that neighbouring pixels do not leak from training. |
| Training | 18 | 18 | The same size as the test set or larger. Add tiles for each class that is rare in the training data. |

- Select tiles by country, not at random. Papua New Guinea and French Polynesia have about 45% of the Pacific tiles. To keep small countries in the test set, the number of test tiles for each country is proportional to the square root of its tile count. Do not use tiles that are next to training tiles.
- The number of points in each class is more important than the number of tiles. Aim for at least 100 test points for the rarest classes (wetland and cropland) across all test tiles. If a class is missing, change the tiles.
- Look at the class histogram of the test set before you use it. If a class is missing or very small, change the tiles and increase `TEST_DATA_VERSION`.
- The test points are sampled in the same way as the training points: stratified by class in each tile, from the agreement map. `min_sample_per_class_n` adds more samples of rare classes.
- Accuracy is measured at 2020, the year in which all source LULC products have data. The eval cell in `notebooks/1_Train_Model.ipynb` gives the precision and recall of each class for a model version.
- The test labels are only for 2020, so temporal consistency is a separate check. Classify the test tiles for all years. Then count the pixels that change class, or that change implausibly between years (`notebooks/LULC_Temporal_Stability.ipynb`).

`notebooks/LULC_Temporal_Stability.ipynb` measures temporal stability. Each LULC version has a stability report in source control. This shows if the stability improves when the training data and the model change.


#### 3. Validation data

[Collect Earth Online](https://www.collect.earth/) (CEO) is the tool for the validation data. `notebooks/Generate_Plots_For_Countries.ipynb` makes the plots (samples) for the CEO projects. There is one project for each country. The plot files are [here](https://drive.google.com/drive/folders/16HU7nQj0cu45jTSjFXZIJN52mIhRTq8x).

To use a zipped shapefile in a new CEO project for a different country:
1. On the "Project Boundary" tab, select "Define AOI from plots file".
2. On the "Plot Generation" tab, upload the zipped shapefile. In the network tab of the browser, wait until the `check-plot-file` request is successful.
3. On the "Sample Design" tab, select "Center".

The map can show a different dataset. This is a CEO bug, and you can ignore it.

The Fiji project has approximately 3,500 equal stratified random points, aligned to the centres of the 30 m pixels: https://app.collect.earth/project-wizard?projectId=64287&institutionId=7195. We will make the validation dataset from these points with local partners.

The data made in CEO is in [the same folder](https://drive.google.com/drive/folders/16HU7nQj0cu45jTSjFXZIJN52mIhRTq8x). When points are labelled, export the data and keep it for the validation:
- ceo-Fiji-LULC-Validation-plot-data-2026-10-09.csv
- ceo-Fiji-LULC-Validation-sample-data-2026-10-09.csv

The validation data has human or AI labelled points for the formal validation report (step 7 of the workflow). It is different from the test data, and it does not exist yet.


#### Model features

Training data and prediction use the same function to make the features (`make_features` in `ldn/raster.py`), so the model always gets the same features. There are 92 features, all float32. The scikit-learn random forest changes its inputs to float32. If a pixel has a NaN feature, it is nodata in the prediction, and it is not in the training data.

| Group | Features | Count | Values |
| - | - | - | - |
| Reflectance | `red`, `green`, `blue`, `nir08`, `swir16`, `swir22` | 6 | GeoMAD geomedian surface reflectance in [0, 1], scaled from the uint16 Landsat Collection 2 digital numbers |
| MAD | `emad`, `smad`, `bcmad` | 3 | GeoMAD Euclidean, spectral and Bray-Curtis median absolute deviations, as stored |
| Spectral indices | `ndvi`, `ndwi`, `mndwi`, `ndti`, `bsi`, `mbi`, `baei`, `bui` | 8 | Normalised differences in [-1, 1], but `mbi` is in [-0.5, 1.5], `bui` is in [-2, 2] and `baei` is positive with no upper limit. NaN where all bands in a denominator are 0 |
| Terrain | `elevation`, `slope`, `northness`, `eastness` | 4 | From Copernicus DEM GLO-30, calculated on its native grid. Units: metres, degrees, and [-1, 1] for northness and eastness (aspect weighted by sin(slope)) |
| Focal statistics | `{band}_{stat}_{size}` | 71 | Statistics in a size x size pixel window around each pixel: the mean and std of each index at 3, 7 and 15 (48), the mean of each reflectance band at 3, 7 and 15 (18), the mean of each MAD band at 7 (3), and the elevation TPI (elevation minus its focal mean) at 15 and 51 (2) |

The focal statistics are candidates (`FOCAL_FEATURES`). Model training keeps the ones that make the accuracy better. Prediction calculates only the ones that the model uses. The training data CSVs also have `lulc`, the label (a class value from 1 to 7, see `typology_mapping.yaml`), and `geometry`, the sample point in WGS84.

#### Model & Prediction

Classification runs for each tile and year with a pre-trained random forest model. The model comes from a local `.joblib` file or from an S3 URL.

1. Search: the STAC GeoParquet index gives the GeoMAD item of the tile and the items of its neighbours, by tile ID and year. The neighbours give the data for the focal statistics at the tile edges.
2. Load: the GeoMAD bands load directly onto the tile geobox, which is the native grid of the GeoMAD items. There is no reprojection or cropping, and tiles that cross the antimeridian load correctly.
3. Features: the model features (see [Model features](#model-features)) are made in the same way as for the training data. Only the focal statistics that the model uses are calculated.
4. Predict: the random forest gives a probability for each class at each pixel. The result is the class with the highest probability. If this probability is less than `probability_threshold`, the pixel is nodata. The output also has a probability band for each class, named by class value: `probability_1` is Tree cover, `probability_2` is Grassland, and so on (see `typology_mapping.yaml`).
5. Write: the output COGs and the STAC metadata go to S3.

Dask distributes the processing. You can configure the workers, threads and memory. There are two faster, low-fidelity modes for tests:
- `decimated`: runs at 10 times lower resolution (100 times fewer pixels).
- `integration_test`: runs on a 5 x 5 pixel crop with a single worker, for fast CI checks and tests.

---

## Coverage: Regions & Grids

### Area of Interest

Two country lists, merged, define the coverage:

1. [Small Island Developing States (UN)](https://www.un.org/ohrlls/content/list-sids)
2. [Pacific Island Countries and Territories (SPC)](https://www.spc.int/our-members/). These are included because of an agreement between Auspatious and the Pacific Community (SPC) to work together, in exchange for use of the Digital Earth Pacific platform.

Countries and territories that are in **both** lists are in the **Pacific** region. The country boundaries come from [UC Davis GADM](https://geodata.ucdavis.edu/gadm/).

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

[Maps for each country](https://github.com/auspatious/ldn-lulc/tree/main/notebooks/maps/countries).

### Coordinate Reference Systems

The two regions use different CRSs. A single latitude/longitude grid causes distortion, and no single projected grid can cross the antimeridian and also cover the whole world.

| Region | Countries/Territories | Grid tiles | CRS | Region map |
| --- | --- | --- | --- | --- |
| Pacific | 22 | 517 | EPSG:3832 (WGS 84 / PDC Mercator) | [map](https://github.com/auspatious/ldn-lulc/blob/main/notebooks/maps/regions/Pacific.jpg) |
| Non-Pacific | 38 | 300 | EPSG:6933 (WGS 84 / NSIDC EASE-Grid 2.0 Global, Cylindrical Equal Area) | [map](https://github.com/auspatious/ldn-lulc/blob/main/notebooks/maps/regions/Non-Pacific.jpg) |

- The Pacific region crosses the antimeridian, which breaks a single EPSG:6933 grid. Thus there are two gridspecs.
- EPSG:6933 is valid for the whole world up to mid-latitudes, which includes all SIDS. But its longitude is only from -180 to 180 degrees. Tiles must wrap in both directions at the antimeridian, and thus they overlap. In this CRS, coordinates are not continuous across the antimeridian. This causes problems for Fiji and other areas that cross the antimeridian.
- Tiling uses [ODC GridSpec](https://odc-geo.readthedocs.io/en/latest/_api/odc.geo.gridspec.GridSpec.html) to divide the data into spatial chunks for parallel processing.

### Tile Grids

Each tile has 30 m pixels in the CRS of its region, and is 96 km (3,200 pixels) on each side. There are 817 tiles in the two regions.

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

Each region has its own subfolder, but one index in the parent folder includes the two regions. See [README_Source.Coop.md](https://github.com/auspatious/ldn-lulc/blob/main/README_Source.Coop.md) for more details.


#### STAC GeoParquet Index

There is one index for the two regions. You can query it directly. You do not have to download the full catalog.

```
https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet
```

#### How to use this product in Python

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

**Show on an interactive map:**

```python
ds.odc.explore(vmin=7500, vmax=12000)
```

**Show with Xarray:**

```python
ds[["red", "green", "blue"]].isel(time=0).to_array().squeeze().plot.imshow(vmin=7500, vmax=12000)
```

### 2. Digital Earth Pacific

There are two environments: Staging and Production. Only the Pacific region is written here, with some preliminary non-Pacific LULC outputs. Thus the STAC GeoParquet file is in the folder of the region. On Source.Coop, the index is in the parent folder of the two regions.

**Staging GeoMAD:** https://dep-public-staging.s3.us-west-2.amazonaws.com/index.html?prefix=dep_ls_geomad/

**Staging LULC** (this also contains the training data and the random forest model):
https://dep-public-staging.s3.us-west-2.amazonaws.com/index.html?prefix=dep_ls_lulc/

**Production:** you can explore the `dep-public-data` bucket in the STAC browser: https://stac-browser.digitalearthpacific.org/collections/dep_ls_geomad

---

## Development

### Quickstart

**1. Install GDAL, Rust (for datacube-compute) and uv**

```bash
brew upgrade gdal

brew install rustup
rustup-init
export PATH="$HOME/.cargo/bin:$PATH"

brew install uv
```

**2. Install the dependencies (including the dev group)**

```bash
uv sync
```

**3. Set the environment variables**

Copy `.env.example` to `.env`, then:

```bash
source .env
```

You must set `AWS_PROFILE`.

**4. Get AWS access**

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

**Add a dependency:**

```bash
uv add --group dev pytest

# Project-specific packages:
uv add "dep-tools @ git+https://github.com/digitalearthpacific/dep-tools.git"
uv add "datacube-compute @ git+https://github.com/auspatious/datacube-compute.git"
```

### Testing & Pre-commit

**Run the tests:**

```bash
uv run pytest                              # all tests
uv run pytest ldn/tests/test_geomad.py     # single file
```

**Install the pre-commit hooks** (they format Python, YAML and JSON):

```bash
uv run pre-commit install
```

If the hooks in `.pre-commit-config.yaml` change, run them on all files:

```bash
uv run pre-commit run --all-files
```

### Docker

Build the image locally with [Buildx](https://docs.docker.com/buildx/working-with-buildx/):

```bash
docker buildx build . --tag ldn-lulc:dev
docker run --rm ldn-lulc:dev ldn --help
```

Run a command in the container:

```bash
docker run --rm ldn-lulc:dev ldn grid list-countries
```

### Running on an EC2 VM

This is much faster than a local run.

1. Set up SSH access.
2. Connect to the VM with SSH. For example, for "Alex Hack Box 2026": `ssh -i ~/.ssh/id_ed25519 ubuntu@44.230.85.235`
3. Pull the code on the VM.
4. Install the dependencies (as in the Quickstart).
5. Run the job in `tmux`, so that the job continues if you disconnect:

```bash
tmux new -s geomad
uv run make geomad-2000-2025
# Detach: Ctrl+B, then D. The job continues.
tmux attach -t geomad       # reattach
tmux kill-session -t geomad # kill from outside
```

### Running in Argo

The Argo Workflow templates are [here](https://github.com/auspatious/ldn-lulc/tree/main/templates). There are workflows to:

- Make the geomedian and GeoMAD
- Make LULC from GeoMAD
- Index each dataset to STAC GeoParquet

The workflows use the image in GHCR, which contains the LDN Python toolkit. The templates set `image-tag` to a
release version (for example `0.2.0`), not `latest`, so that runs are reproducible.

---

## Visualisation (WIP)

TODO: The visualisation is almost ready to deploy again as a Vite app. It reads COGs directly from S3 and shows them in the browser with deck.gl-raster.

---

## CI/CD

A GitHub Actions workflow (`.github/workflows/ldn-lint-build-test-push.yml`) lints, builds and tests the Docker image. Then it pushes the image to the GitHub Container Registry (GHCR).

**Triggers** (only for changes to the LDN package that affect the image):
- A manual run from the GitHub UI
- A pull request to any branch
- A push to `main`
- A published release

If there is a newer run for the same workflow and ref, the older run is cancelled.

**Jobs:**

1. **`lint`**: runs [Ruff](https://github.com/astral-sh/ruff-action) on the `ldn` directory.
2. **`build-test-push`** (runs after `lint` passes):
   - Logs in to GHCR (only on `main` or for a release).
   - Reads the version from `pyproject.toml`. A release fails if its tag is not `v<version>`.
   - Builds the `test` target image and runs the tests in it.
   - Builds the `final` target (production) image. It runs `ldn --help` as a smoke test, and checks that the image
     reports the `pyproject.toml` version.
   - Pushes to GHCR only on `main` or for a release. Each PR is built and tested, but nothing is published from a PR.
     - On `main`: pushes the tag `<version>-<short sha>` (for example `0.2.0-abc1234`).
     - On a release: pushes the tags `<version>` (for example `0.2.0`) and `latest`. `latest` is always the most recent release.

The action versions are pinned to full 40-character commit SHAs, not tags, for supply-chain security.

The Docker image installs the dependencies with `uv sync --locked`. Thus the build fails if `uv.lock` does not agree
with `pyproject.toml`. Run `uv lock` after you change the dependencies or the version. The git dependencies
(`dep-tools` and `datacube-compute`) are pinned to commits in `[tool.uv.sources]`. STAC items record the installed
commit in `processing:software`.

### Releasing

1. Increase the version in `pyproject.toml`, for example with `uv version --bump minor`. This also updates `uv.lock`.
2. Merge to `main`.
3. Publish a GitHub release with the tag `v<version>`, for example `v0.2.0`. CI pushes the image as `0.2.0` and `latest`.
4. Change `image-tag` in the [Argo templates](templates/) to the new version.
