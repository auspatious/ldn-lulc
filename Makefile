# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.

# Workflow for 2 regions writing to different buckets/paths.
# Workflow:
# 1. Run GeoMAD for all tiles/years
# 2. Run index GeoMAD (STAC-Geoparquet)
# 3. Make training data (in notebooks/training_data/0_Generate_Training_Points.ipynb)
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

PACIFIC_TRAINING_TILES := $(shell python3 -c "from ldn.training_data import PACIFIC_TRAINING_TILES; print(' '.join([f\"{t[0]}:{t[1]}:{list(t[2].keys())[0].replace(' ','_')}:{list(t[2].values())[0]}\" for t in PACIFIC_TRAINING_TILES]))");

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
		--geomad-version 0-3-0-test \
		--dataset geomad \
		--no-overwrite \
		--bucket dep-public-staging;

geomad-test-ausp:
	ldn geomad run \
		--tile-id 031_038 \
		--region pacific \
		--year 2000 \
		--version 0-3-0-test \
		--decimated \
		--bucket data.ldn.auspatious.com \
		--overwrite;
geomad-test-dep-staging:
	ldn geomad run \
		--tile-id 031_038 \
		--region pacific \
		--year 2000 \
		--version 0-3-0-test \
		--collection-url-root="https://stac.staging.digitalearthpacific.io/collections" \
		--decimated \
		--bucket dep-public-staging \
		--overwrite;

index-geomad-test-ausp:
	ldn index-to-stac-geoparquet \
	--dataset geomad \
	--geomad-version 0-3-0-test \
	--no-single-region \
	--bucket data.ldn.auspatious.com;
index-geomad-test-dep-staging:
	ldn index-to-stac-geoparquet \
	--dataset geomad \
	--geomad-version 0-3-0-test \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging;

collection-geomad-test-ausp:
	ldn collection create-collection \
	--dataset geomad \
	--geomad-version 0-3-0-test \
	--no-single-region \
	--bucket data.ldn.auspatious.com \
	--no-has-stac-api;
collection-geomad-test-dep-staging:
	ldn collection create-collection \
	--dataset geomad \
	--geomad-version 0-3-0-test \
	--url-root="https://stac.staging.digitalearthpacific.io" \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging \
	--has-stac-api;


# # TODO: Make mosaics for GeoMAD
# make-mosaics-geomad:
# 	ldn make-mosaics \
# 	--dataset geomad;
# # poetry run ldn make-mosaics --dataset geomad --geomad-version test-integration --single-region --product-owner dep;
# # poetry run ldn make-mosaics --dataset geomad --geomad-version 0-3-0-test --single-region --product-owner dep;




#### Training Data
# Geomad version: 0-2-1 in DEP staging, 0-3-0 in DEP public.
training-data-generate:
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

#### Make the model using ldn-lulc/notebooks/1_Train_Model.ipynb



# ###### LULC Classification/Prediction

# # Predict LULC for the test tiles and one year (2025).

# # Print tasks
# print-tasks-lulc-2020:
# 	ldn print-tasks \
# 	--years="2020" \
# 	--region="pacific" \
# 	--dataset="lulc";


# # Classify
lulc-predict-test:
	ldn lulc run \
		--tile-id 028_030 \
		--year 2000 \
		--region pacific \
		--version 0-0-9 \
		--geomad-version 0-2-1 \
		--bucket dep-public-staging \
		--model-path="/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-9/pacific/2020/lulc_random_forest_model_pacific_2020.joblib" \
		--no-overwrite;

# 		--model-path="https://dep-public-staging.s3.us-west-2.amazonaws.com/dep_ls_lulc/models/0-0-9/pacific/2020/lulc_random_forest_model_pacific_2020.joblib" \

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
				--bucket dep-public-staging \
				--model-path="/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-9/pacific/2020/lulc_random_forest_model_pacific_2020.joblib" \
				--no-overwrite;
		done;
	done;

index-lulc-test-dep-staging:
	ldn index-to-stac-geoparquet \
	--dataset lulc \
 	--geomad-version 0-2-1 \
	--lulc-version 0-0-9 \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging;
