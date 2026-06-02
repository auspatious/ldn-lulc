# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.

# Workflow for 2 regions writing to different buckets/paths.
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
TEST_TILES_WITH_COUNTRY := $(shell python3 -c "from ldn.utils import TEST_TILES; print(' '.join([f\"{t[0]}:{t[1]}:{list(t[2].keys())[0].replace(' ','_')}:{list(t[2].values())[0]}\" for t in TEST_TILES]))")
# TEST_TILES := $(shell python3 -c "from ldn.utils import TEST_TILES; print(' '.join([f'{t[0]}:{t[1]}' for t in TEST_TILES if t[0] == '312_106']))")

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

print-tasks-2000-2025-pacific:
	ldn print-tasks --years="2000-2025" --region="pacific"


TEST_TILES_2_REGIONS := 076_024:pacific 144_127:non-pacific

# TODO: Run these non-decimated. Just testing bucket stuff here.
# TODO: Get write access for Will to dep-public-staging.
geomad-2-regions-decimated:
	for site in $(TEST_TILES_2_REGIONS); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		ldn geomad \
			--tile-id $$tile_id \
			--region $$region \
			--year 2010 \
			--version $(VERSION_GEOMAD) \
			--decimated \
			--overwrite; \
	done


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
				--overwrite; \
		done; \
	done


index-geomad:
	ldn index-to-stac-geoparquet \
	--dataset "geomad" \
	--region "all" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION)



#### Training Data
# Format: tile_id:region:country_name:country_code (spaces in names replaced with _)
# training-data-generate:
# 	for site in $(TEST_TILES_WITH_COUNTRY); do \
# 		tile_id=$$(echo $$site | cut -d: -f1); \
# 		region=$$(echo $$site | cut -d: -f2); \
# 		country_name=$$(echo $$site | cut -d: -f3 | tr '_' ' '); \
# 		country_code=$$(echo $$site | cut -d: -f4); \
# 		ldn training generate-training-data \
# 			--tile-id $$tile_id \
# 			--region $$region \
# 			--country-name "$$country_name" \
# 			--country-code "$$country_code"; \
# 	done
training-data-generate:
	ldn training generate-training-data \
		--tile-id 028_030 \
		--region pacific \
		--country-name "Papua New Guinea" \
		--country-code PNG
# TODO: Figure out why this one OOM kills.
# training-data-generate:
# 	ldn training generate-training-data \
# 		--tile-id 058_043 \
# 		--region pacific \
# 		--country-name Kiribati \
# 		--country-code KIR
# training-data-generate:
# 	ldn training generate-training-data \
# 		--tile-id 076_024 \
# 		--region pacific \
# 		--country-name "American Samoa" \
# 		--country-code ASM
# TODO: Run geomad for 2020 in DEP Bucket for this:
training-data-generate-am-crossing:
	ldn training generate-training-data \
		--tile-id 066_022 \
		--region pacific \
		--country-name "Fiji" \
		--country-code FJI

###### Classification/Prediction

# 1. Training data is created in notebooks/training_data/0_Generate_Training_Points.ipynb.

# 2. Train a model with the training data made in the notebook above.
# train-model:
# 	ldn classify train-model


# 3. Predict LULC for the test tiles and one year (2025).

# 3a. print-tasks
print-tasks-prediction-2025:
	ldn print-tasks \
	--years="2025" \
	--region="pacific" \
	--dataset="prediction"


# 3c.
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
				$(DECIMATED) \
				--overwrite; \
		done; \
	done

# TODO: Get write access for Will to dep-public-staging.
prediction-2-regions-decimated:
	for site in $(TEST_TILES_2_REGIONS); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		ldn classify classify \
			--tile-id $$tile_id \
			--year 2010 \
			--version $(VERSION_PREDICTION) \
			--version-geomad $(VERSION_GEOMAD) \
			--region $$region \
			--decimated \
			--overwrite; \
	done



# 4. Update the STAC-Geoparquet index after all tiles/years have run.
index-predictions:
	ldn index-to-stac-geoparquet \
	--dataset "prediction" \
	--region "all" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION)


# Visualisation
make-mosaics-geomad:
	ldn make-mosaics \
	--dataset geomad \
	--region "all"

make-mosaics-prediction:
	ldn make-mosaics \
	--dataset prediction \
	--region "all"






# Non-Pacific workflow testing

# poetry run ldn geomad \
#         --tile-id 145_127 \
#         --region non-pacific \
#         --year 2000 \
#         --version "0-2-1" \
#         --decimated \
#         --overwrite;
# poetry run ldn geomad \
#         --tile-id 145_127 \
#         --region non-pacific \
#         --year 2010 \
#         --version "0-2-1" \
#         --decimated \
#         --overwrite;
# poetry run ldn geomad \
#         --tile-id 145_127 \
#         --region non-pacific \
#         --year 2025 \
#         --version "0-2-1" \
#         --decimated \
#         --overwrite;


# poetry run ldn index-to-stac-geoparquet \
# 	--dataset "geomad" \
# 	--region "non-pacific" \
# 	--version-geomad "0-2-1" \
# 	--version-prediction "0-0-4"

# poetry run ldn make-mosaics \
# 	--dataset "geomad" \
# 	--region "non-pacific"


# poetry run ldn classify classify \
# 	--tile-id 145_127 \
# 	--year 2000 \
# 	--version "0-0-4" \
# 	--version-geomad "0-2-1" \
# 	--region non-pacific \
# 	--model-path "/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-4/pacific/lulc_random_forest_model.joblib" \
#         --decimated \
# 	--overwrite;
# poetry run ldn classify classify \
# 	--tile-id 145_127 \
# 	--year 2010 \
# 	--version "0-0-4" \
# 	--version-geomad "0-2-1" \
# 	--region non-pacific \
# 	--model-path "/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-4/pacific/lulc_random_forest_model.joblib" \
#         --decimated \
# 	--overwrite;
# poetry run ldn classify classify \
# 	--tile-id 145_127 \
# 	--year 2025 \
# 	--version "0-0-4" \
# 	--version-geomad "0-2-1" \
# 	--region non-pacific \
# 	--model-path "/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-4/pacific/lulc_random_forest_model.joblib" \
#         --decimated \
# 	--overwrite;


# poetry run ldn index-to-stac-geoparquet \
# 	--dataset "prediction" \
# 	--region "non-pacific" \
# 	--version-geomad "0-2-1" \
# 	--version-prediction "0-0-4"

# poetry run ldn make-mosaics \
# 	--dataset "prediction" \
# 	--region "non-pacific"
