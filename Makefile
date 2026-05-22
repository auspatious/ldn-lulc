# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.

# Workflow:
# 1. Run GeoMAD for all tiles/years
# 2. Run index GeoMAD (STAC-Geoparquet)
# 3. Make training data (in notebooks/training_data/0_Generate_Training_Points.ipynb)
# 4. Train model (in notebooks/training_data/1_Train_Predict.ipynb)
# 5. Run prediction for all tiles/years
# 6. Run index prediction (STAC-Geoparquet)
# 7. Run make-mosaic for geomad and prediction datasets
# 8. Visualisation app will update automatically when mosaics are updated (unless version/path is different).

VERSION_GEOMAD := $(shell python3 -c "from ldn.utils import GEOMAD_VERSION; print(GEOMAD_VERSION)")
VERSION_PREDICTION := $(shell python3 -c "from ldn.utils import PREDICTION_VERSION; print(PREDICTION_VERSION)")
VERSION_MODEL := $(shell python3 -c "from ldn.utils import MODEL_VERSION; print(MODEL_VERSION)")
# TEST_TILES is a list of tuples: (tile_id, region, {country_name: country_code}) e.g. ("089_016", "pacific", {"Cook Islands": "COK"})
TEST_TILES := $(shell python3 -c "from ldn.utils import TEST_TILES; print(' '.join([f'{t[0]}:{t[1]}' for t in TEST_TILES]))")
# TEST_TILES := $(shell python3 -c "from ldn.utils import TEST_TILES; print(' '.join([f'{t[0]}:{t[1]}' for t in TEST_TILES if t[0] == '312_106']))")
# TEST_TILES_PACIFIC := $(shell python3 -c "from ldn.utils import TEST_TILES_PACIFIC; print(' '.join([f'{t[0]}:{t[1]}' for t in TEST_TILES_PACIFIC]))")
# TEST_TILES = $(TEST_TILES_PACIFIC)


DECIMATED ?= --no-decimated


# Get grid tiles - all
grid-get-tiles-all:
	ldn grid get-grid-tiles --format="gdf" --grids="all" --overwrite

# List countries in grids
grid-list-countries-all:
	ldn grid list-countries --grids="all"

grid-list-countries-pacific:
	ldn grid list-countries --grids="pacific"

grid-list-countries-non-pacific:
	ldn grid list-countries --grids="non-pacific"

print-tasks-2000-2025-all:
	ldn print-tasks --years="2000-2025" --region="all"

print-tasks-2025-pacific:
	ldn print-tasks --years="2025" --region="pacific"

filter-tasks:
	ldn filter-tasks \
	--tasks-json "$$(cat tasks.json)" \
	--version "0-1-0" \
	--bucket "dep-public-staging" \
	--dataset "geomad" \
	--no-overwrite


# Run geomad for all test case sites for years 2000-2025.
geomad-2000-2025:
	for site in $(TEST_TILES); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		for year in $$(seq 2000 2025); do \
			ldn geomad \
				--tile-id $$tile_id \
				--region $$region \
				--year $$year \
				--version $(VERSION_GEOMAD) \
				--bucket "dep-public-staging" \
				--no-decimated \
				--include-shadow \
				--all-bands \
				--memory-limit "10GB" \
				--n-workers 2 \
				--threads-per-worker 16 \
				--xy-chunk-size 2048 \
				--geomad-threads 10 \
				--ls7-buffer-years 1 \
				--overwrite; \
		done; \
	done

# geomad-test:
# 	for year in 2000 2010 2020; do \
# 		ldn geomad \
# 			--tile-id 063_020 \
# 			--region pacific \
# 			--year $$year \
# 			--version $(VERSION_GEOMAD) \
# 			--product-owner ausp \
# 			--overwrite; \
# 	done

# geomad-test-2:
# 	ldn geomad \
# 		--tile-id 058_043 \
# 		--region pacific \
# 		--year 2010 \
# 		--version $(VERSION_GEOMAD) \
# 		--product-owner ausp \
# 		--overwrite;

index-geomad:
	ldn index-to-stac-geoparquet \
	--dataset "geomad" \
	--region "all" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION) \
	--bucket-pacific "dep-public-staging" \
	--bucket-non-pacific "data.ldn.auspatious.com" \
	--prefix-pacific-geomad "dep_ls_geomad" \
	--prefix-non-pacific-geomad "ci_ls_geomad" \
	--aws-region "us-west-2"


###### Classification/Prediction

# 1. Training data is created in notebooks/training_data/0_Generate_Training_Points.ipynb.

# 2. Train a model with the training data made in the notebook above.
# train-model:
# 	ldn classify train-model


# 3. Predict LULC for the test tiles and one year (2025).
# TODO: Run for all years in future
predict-lulc-test-tiles:
	for site in $(TEST_TILES); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		for year in $$(seq 2023 2025); do \
			ldn classify classify \
				--tile-id $$tile_id \
				--year $$year \
				--version $(VERSION_PREDICTION) \
				--version-geomad $(VERSION_GEOMAD) \
				--region $$region \
				--output-bucket="data.ldn.auspatious.com" \
				--output-prefix="ausp" \
				--geomad-bucket="data.ldn.auspatious.com" \
				--geomad-prefix="ausp_ls_geomad" \
				--geomad-aws-region="us-west-2" \
				--model-path="ldn/models/$(VERSION_MODEL)/lulc_random_forest_model.joblib" \
				--xy-chunk-size 1024 \
				$(DECIMATED) \
				--overwrite; \
		done; \
	done

VERSION_GEOMAD_NEW ?= 0-2-0

predict-lulc-test-tiles-dep-staging:
	for site in $(TEST_TILES); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		for year in $$(seq 2023 2025); do \
			ldn classify classify \
				--tile-id $$tile_id \
				--year $$year \
				--version $(VERSION_PREDICTION) \
				--version-geomad $(VERSION_GEOMAD_NEW) \
				--region $$region \
				--output-bucket="dep-public-staging" \
				--output-prefix="dep" \
				--geomad-bucket="dep-public-staging" \
				--geomad-prefix="dep_ls_geomad" \
				--geomad-aws-region="us-west-2" \
				--model-path="ldn/models/$(VERSION_MODEL)/lulc_random_forest_model.joblib" \
				--xy-chunk-size 1024 \
				$(DECIMATED) \
				--overwrite; \
		done; \
	done
# 				--model-path="https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/models/0-0-3/lulc_random_forest_model.joblib" \


# 4. Update the STAC-Geoparquet index after all tiles/years have run.
index-predictions:
	ldn index-to-stac-geoparquet \
	--dataset "prediction" \
	--region "all" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION) \
	--bucket-pacific "dep-public-staging" \
	--bucket-non-pacific "data.ldn.auspatious.com" \
	--prefix-pacific-prediction "dep_ls_lulc_prediction" \
	--prefix-non-pacific-prediction "ci_ls_lulc_prediction" \
	--aws-region "us-west-2"


# Visualisation
make-mosaics-geomad:
	ldn make-mosaics \
	--dataset geomad \
	--region "all"

make-mosaics-prediction:
	ldn make-mosaics \
	--dataset prediction \
	--region "all"
