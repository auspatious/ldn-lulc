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

VERSION_GEOMAD := $(shell python3 -c "from ldn.utils import GEOMAD_VERSION; print(GEOMAD_VERSION)");
VERSION_LULC := $(shell python3 -c "from ldn.utils import LULC_VERSION; print(LULC_VERSION)");
VERSION_MODEL := $(shell python3 -c "from ldn.utils import MODEL_VERSION; print(MODEL_VERSION)");

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

print-tasks-2000-2025-all:
	ldn print-tasks --years="2000-2025" --region="all";

print-tasks-2000-2025-pacific:
	ldn print-tasks --years="2000-2025" --region="pacific";


TEST_TILES_2_REGIONS := 076_024:pacific 144_127:non-pacific

# TODO: Run these non-decimated. Just testing bucket stuff here.
# TODO: Get write access for Will to dep-public-staging.
geomad-2-regions-decimated:
	for site in $(TEST_TILES_2_REGIONS); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		ldn geomad run \
			--tile-id $$tile_id \
			--region $$region \
			--year 2010 \
			--version $(VERSION_GEOMAD) \
			--decimated \
			--overwrite; \
	done;


# Run geomad for all test case sites for years 2000-2025.
geomad-2000-2025:
	for site in $(TEST_TILES); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		for year in $$(seq 2000 2025); do \
			ldn geomad run \
				--tile-id $$tile_id \
				--region $$region \
				--year $$year \
				--version $(VERSION_GEOMAD) \
				--overwrite; \
		done; \
	done;


index-geomad:
	ldn index-to-stac-geoparquet \
	--dataset "geomad" \
	--region "all" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-lulc $(VERSION_LULC);


#### Training Data
training-data-generate:
	for site in $(PACIFIC_TRAINING_TILES); do \
		tile_id=$$(echo $$site | cut -d: -f1); \
		region=$$(echo $$site | cut -d: -f2); \
		country_name=$$(echo $$site | cut -d: -f3 | tr '_' ' '); \
		country_code=$$(echo $$site | cut -d: -f4); \
		ldn training generate-training-data \
			--tile-id $$tile_id \
			--region $$region \
			--country-name "$$country_name" \
			--country-code "$$country_code"; \
	done;


###### LULC Classification/Prediction

# Predict LULC for the test tiles and one year (2025).

# 1. Print tasks
print-tasks-lulc-2020:
	ldn print-tasks \
	--years="2020" \
	--region="pacific" \
	--dataset="lulc";


# 2. Classify
# TODO: Run for all years in future
predict-lulc-test-tiles-2020:
	for site in $(TEST_TILES); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		ldn lulc run \
			--tile-id $$tile_id \
			--year 2020 \
			--version $(VERSION_LULC) \
			--version-geomad $(VERSION_GEOMAD) \
			--region $$region \
			$(DECIMATED) \
			--overwrite; \
	done;

# TODO: Get write access for Will to dep-public-staging.
lulc-2-regions-decimated:
	for site in $(TEST_TILES_2_REGIONS); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		ldn lulc run \
			--tile-id $$tile_id \
			--year 2010 \
			--version $(VERSION_LULC) \
			--version-geomad $(VERSION_GEOMAD) \
			--region $$region \
			--decimated \
			--overwrite; \
	done;



# 3. Update the STAC-Geoparquet index after all tiles/years have run.
index-lulc:
	ldn index-to-stac-geoparquet \
	--dataset "lulc" \
	--region "all" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-lulc $(VERSION_LULC);


# 4. Visualisation
make-mosaics-geomad:
	ldn make-mosaics \
	--dataset geomad;

make-mosaics-lulc:
	ldn make-mosaics \
	--dataset lulc;



# Source.Coop testing:
SOURCE_TEST_VERSION ?= 0-2-1-test
SOURCE_TEST_VERSION_P ?= 0-0-4-test
SOURCE_TEST_TILE ?= 028_030
geomad-source-coop-test:
	poetry run ldn geomad run \
		--tile-id $(SOURCE_TEST_TILE) \
    	--region pacific \
    	--year 2025 \
    	--version $(SOURCE_TEST_VERSION) \
		--decimated;
geomad-source-coop-test-np:
	poetry run ldn geomad run \
		--tile-id 334_092 \
    	--region non-pacific \
    	--year 2025 \
    	--version $(SOURCE_TEST_VERSION) \
		--decimated;

# Test geomad works for LS7
test-geomad-ls7-source-coop:
	poetry run ldn geomad run \
		--tile-id 050_015 \
    	--region pacific \
    	--year 2010 \
    	--version 0-2-1-test \
		--decimated;

index-geomad-source-coop-test:
	ldn index-to-stac-geoparquet \
	--dataset "geomad" \
	--version-geomad $(SOURCE_TEST_VERSION);

mosaic-geomad-source-coop-test:
	ldn make-mosaics \
	--dataset geomad \
	--version-geomad $(SOURCE_TEST_VERSION);

lulc-source-coop-test:
	ldn lulc run \
		--tile-id $(SOURCE_TEST_TILE) \
		--year 2025 \
		--version $(SOURCE_TEST_VERSION_P) \
		--version-geomad $(SOURCE_TEST_VERSION) \
		--region pacific \
		--model-path "/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-0-4/pacific/2020/lulc_random_forest_model_pacific_2020.joblib" \
		--no-decimated \
		--overwrite; \

index-lulc-source-coop-test:
	ldn index-to-stac-geoparquet \
	--dataset "lulc" \
	--version-geomad $(SOURCE_TEST_VERSION) \
	--version-lulc $(SOURCE_TEST_VERSION_P);

mosaic-lulc-source-coop-test:
	ldn make-mosaics \
	--dataset lulc \
	--version-geomad $(SOURCE_TEST_VERSION) \
	--version-lulc $(SOURCE_TEST_VERSION_P);

# export AWS_WRITE_ACCESS_KEY_ID=""
# export AWS_WRITE_SECRET_ACCESS_KEY=""
# export AWS_WRITE_SESSION_TOKEN=""



# # Test for all 3 bucket styles
# print tasks works for both s3 and source.coop
# poetry run ldn print-tasks --years="2024" --region="pacific" --version-geomad="0-0-2";

# works for both i think
# poetry run ldn geomad run \
# 			--tile-id 066_022 \
# 			--region pacific \
# 			--year 2025 \
# 			--version test \
# 			--decimated \
# 			--overwrite;

# works for both i think
# poetry run ldn index-to-stac-geoparquet \
# 	--dataset "geomad" \
# 	--region "all" \
# 	--version-geomad test \
# 	--version-lulc test;

# works for both i think
# poetry run ldn make-mosaics \
# 	--dataset geomad \
# 	--version-geomad test;

# works for both i think
# poetry run ldn training generate-training-data \
# 			--tile-id 066_022 \
# 			--region pacific \
#             --year 2025 \
# 			--country-name "Fiji" \
# 			--country-code "FJI" \
#             --geomad-version test \
#             --training-data-version test;


# poetry run pytest
