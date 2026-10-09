# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.

# Workflow for 2 regions writing to different buckets/paths.
# Workflow:
# 1. Run GeoMAD for all tiles/years
# 2. Run index GeoMAD (STAC-Geoparquet)
# 3. Make training data
# 4. Train model (in notebooks/training_data/1_Train_Model.ipynb)
# 5. Run LULC prediction for all tiles/years
# 6. Run index LULC (STAC-Geoparquet)
# 7. Run make-mosaic for geomad and LULC datasets
# 8. Visualisation app will update automatically when mosaics are updated (unless version/path is different).

# You need to manually set AWS_PROFILE first.
-include .env
export
echo "Using AWS_PROFILE=$(AWS_PROFILE) and BUCKET=$(BUCKET)";

aws-login:
	unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN && \
	aws sso login --profile $(AWS_PROFILE)


GEOMAD_VERSION := $(shell python3 -c "from ldn.utils import GEOMAD_VERSION; print(GEOMAD_VERSION)");
LULC_VERSION := $(shell python3 -c "from ldn.utils import LULC_VERSION; print(LULC_VERSION)");

# Training and test tiles for both regions
TRAINING_TILES := $(shell python3 -c "from ldn.training_data import TRAINING_TILES; print(' '.join([f\"{t[0]}:{t[1]}:{list(t[2].keys())[0].replace(' ','_')}:{list(t[2].values())[0]}\" for t in TRAINING_TILES]))");
PACIFIC_TRAINING_TILES := $(shell python3 -c "from ldn.training_data import PACIFIC_TRAINING_TILES; print(' '.join([f\"{t[0]}:{t[1]}:{list(t[2].keys())[0].replace(' ','_')}:{list(t[2].values())[0]}\" for t in PACIFIC_TRAINING_TILES]))");
NON_PACIFIC_TRAINING_TILES := $(shell python3 -c "from ldn.training_data import NON_PACIFIC_TRAINING_TILES; print(' '.join([f\"{t[0]}:{t[1]}:{list(t[2].keys())[0].replace(' ','_')}:{list(t[2].values())[0]}\" for t in NON_PACIFIC_TRAINING_TILES]))");

TEST_TILES := $(shell python3 -c "from ldn.training_data import TEST_TILES; print(' '.join([f\"{t[0]}:{t[1]}:{list(t[2].keys())[0].replace(' ','_')}:{list(t[2].values())[0]}\" for t in TEST_TILES]))");

DECIMATED ?= --no-decimated;

# Get grid tiles - all
grid-get-tiles-all:
	ldn grid get-grid-tiles --format="gdf" --grids="all" --overwrite;

# List countries in grids
grid-list-countries-all:
	ldn grid list-countries --grids="all";

grid-list-countries-pacific:
	ldn grid list-countries --grids="pacific";

grid-list-countries-non-pacific:
	ldn grid list-countries --grids="non-pacific";

print-tasks-test-dep-staging:
	ldn print-tasks \
		--years="2000" \
		--region="pacific" \
		--geomad-version 0-3-1-test \
		--dataset geomad \
		--no-overwrite \
		--bucket dep-public-staging;

geomad-test-ausp:
	ldn geomad run \
		--tile-id 064_020 \
		--region pacific \
		--year 2000 \
		--version 0-3-1-test \
		--decimated \
		--bucket data.ldn.auspatious.com \
		--overwrite;

# To test Antimeridian, use Pacific tiles:
# 065_020 (just before the antimeridian)
# 066_020 (crosses the antimeridian)
# 067_020 (just after the antimeridian)
geomad-test-dep-staging:
	for col in 064 065 066 067; do \
		for row in 020 021 022; do \
			ldn geomad run \
				--tile-id $${col}_$${row} \
				--region pacific \
				--year 2025 \
				--version 0-3-1-test \
				--collection-url-root="https://stac.staging.digitalearthpacific.io/collections" \
				--decimated \
				--bucket dep-public-staging \
				--no-overwrite; \
		done; \
	done;

index-geomad-test-ausp:
	ldn index-to-stac-geoparquet \
	--dataset geomad \
	--geomad-version 0-3-1-test \
	--no-single-region \
	--bucket data.ldn.auspatious.com;
index-geomad-test-dep-staging:
	ldn index-to-stac-geoparquet \
	--dataset geomad \
	--geomad-version 0-3-1-test \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging;

collection-geomad-test-ausp:
	ldn collection create-collection \
	--dataset geomad \
	--geomad-version 0-3-1-test \
	--no-single-region \
	--bucket data.ldn.auspatious.com \
	--no-has-stac-api;
collection-geomad-test-dep-staging:
	ldn collection create-collection \
	--dataset geomad \
	--geomad-version 0-3-1-test \
	--url-root="https://stac.staging.digitalearthpacific.io" \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging \
	--has-stac-api;


make-mosaics-geomad:
	ldn make-mosaics \
	--dataset geomad \
	--geomad-version 0-3-0 \
	--lulc-version 0-0-9 \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging;


#### Training Data
# Geomad version: 0-2-1 in DEP staging and Source.Coop. 0-3-0 in DEP public. # TODO: Should we use prod?
training-data-generate-pacific:
	for site in $(PACIFIC_TRAINING_TILES) do \
		tile_id=$$(echo $$site | cut -d: -f1); \
		region=$$(echo $$site | cut -d: -f2); \
		country_name=$$(echo $$site | cut -d: -f3 | tr '_' ' '); \
		country_code=$$(echo $$site | cut -d: -f4); \
		ldn training generate-training-data \
			--tile-id $$tile_id \
			--region $$region \
			--country-name "$$country_name" \
			--country-code "$$country_code" \
			--geomad-version 0-2-1 \
			--geomad-bucket dep-public-staging \
			--output-bucket dep-public-staging \
			--single-region \
			--product-owner dep \
			--no-overwrite; \
	done;
training-data-generate-non-pacific:
	for site in $(NON_PACIFIC_TRAINING_TILES) do \
		tile_id=$$(echo $$site | cut -d: -f1); \
		region=$$(echo $$site | cut -d: -f2); \
		country_name=$$(echo $$site | cut -d: -f3 | tr '_' ' '); \
		country_code=$$(echo $$site | cut -d: -f4); \
		ldn training generate-training-data \
			--tile-id $$tile_id \
			--region $$region \
			--country-name "$$country_name" \
			--country-code "$$country_code" \
			--geomad-version 0-2-1 \
			--geomad-bucket us-west-2.opendata.source.coop \
			--output-bucket dep-public-staging \
			--no-single-region \
			--product-owner ci \
			--no-overwrite; \
	done;

#### Make the model using ldn-lulc/notebooks/1_Train_Model.ipynb

# Frozen held-out test set. Never train on this. (needs the non-Pacific
# GeoMAD bucket and product owner flags).
test-data-generate:
	for site in $(TEST_TILES) do \
		tile_id=$$(echo $$site | cut -d: -f1); \
		region=$$(echo $$site | cut -d: -f2); \
		country_name=$$(echo $$site | cut -d: -f3 | tr '_' ' '); \
		country_code=$$(echo $$site | cut -d: -f4); \
		ldn training generate-training-data \
			--tile-id $$tile_id \
			--region $$region \
			--country-name "$$country_name" \
			--country-code "$$country_code" \
			--geomad-version 0-2-1 \
			--geomad-bucket dep-public-staging \
			--output-bucket dep-public-staging \
			--single-region \
			--product-owner dep \
			--split test \
			--no-overwrite; \
	done;



# ###### LULC Classification/Prediction

# # Predict LULC for the test tiles and one year (2025).

print-tasks-lulc-2000-non-pacific:
	ldn print-tasks \
		--years="2000" \
		--region="non-pacific" \
		--geomad-version 0-2-1 \
		--dataset lulc \
		--no-overwrite \
		--bucket dep-public-staging;


# # Classify
lulc-predict-test-pacific:
	ldn lulc run \
		--tile-id 028_030 \
		--year 2000 \
		--region pacific \
		--version 0-0-9 \
		--geomad-version 0-2-1 \
		--geomad-bucket dep-public-staging \
		--output-bucket dep-public-staging \
		--product-owner dep \
		--model-path="/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-9/pacific/2020/lulc_random_forest_model_pacific_2020.joblib" \
		--no-overwrite;

lulc-predict-test-2:
	for site in $(PACIFIC_TRAINING_TILES) do \
		tile_id=$$(echo $$site | cut -d: -f1); \
		region=$$(echo $$site | cut -d: -f2); \
		for year in 2000 2025; do \
			ldn lulc run \
				--tile-id $$tile_id \
				--year $$year \
				--region $$region \
				--version 0-0-9 \
				--geomad-version 0-2-1 \
				--geomad-bucket dep-public-staging \
				--output-bucket dep-public-staging \
				--product-owner dep \
				--model-path="/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-9/pacific/2020/lulc_random_forest_model_pacific_2020.joblib" \
				--no-overwrite;
		done;
	done;

# TODO: Use Pacific model?
# TODO: Read from Source Coop.
# lulc-predict-test-non-pacific:
# 	ldn lulc run \
# 		--tile-id 312_106 \
# 		--year 2000 \
# 		--region non-pacific \
# 		--version 0-0-9 \
# 		--geomad-version 0-2-1 \
# 		--geomad-bucket us-west-2.opendata.source.coop \
# 		--output-bucket dep-public-staging \
# 				--product-owner TODO? \
# 		--model-path="/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-9/pacific/2020/lulc_random_forest_model_pacific_2020.joblib" \
# 		--no-overwrite;
# # 		--model-path="https://dep-public-staging.s3.us-west-2.amazonaws.com/dep_ls_lulc/models/0-0-9/pacific/2020/lulc_random_forest_model_pacific_2020.joblib" \


index-lulc-test-dep-staging:
	ldn index-to-stac-geoparquet \
	--dataset lulc \
 	--geomad-version 0-2-1 \
	--lulc-version 0-0-9 \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging;


index-lulc-test-ci-staging:
	ldn index-to-stac-geoparquet \
	--dataset lulc \
 	--geomad-version 0-2-1 \
	--lulc-version 0-0-9 \
	--single-region \
	--product-owner ci \
	--bucket dep-public-staging;


make-mosaics-lulc-dep:
	ldn make-mosaics \
	--dataset lulc \
	--geomad-version 0-2-1 \
	--lulc-version 0-0-9 \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging;

make-mosaics-lulc-ci:
	ldn make-mosaics \
	--dataset lulc \
	--geomad-version 0-2-1 \
	--lulc-version 0-0-9 \
	--single-region \
	--product-owner ci \
	--bucket dep-public-staging;
