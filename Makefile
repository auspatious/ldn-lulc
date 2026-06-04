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

PACIFIC_TRAINING_TILES := $(shell python3 -c "from ldn.utils import PACIFIC_TRAINING_TILES; print(' '.join([f\"{t[0]}:{t[1]}:{list(t[2].keys())[0].replace(' ','_')}:{list(t[2].values())[0]}\" for t in PACIFIC_TRAINING_TILES]))")

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
	done


###### Classification/Prediction

# Predict LULC for the test tiles and one year (2025).

# 1. Print tasks
print-tasks-prediction-2020:
	ldn print-tasks \
	--years="2020" \
	--region="pacific" \
	--dataset="prediction"


# 2. Classify
# TODO: Run for all years in future
predict-lulc-test-tiles-2020:
	for site in $(TEST_TILES); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		ldn classify classify \
			--tile-id $$tile_id \
			--year 2020 \
			--version $(VERSION_PREDICTION) \
			--version-geomad $(VERSION_GEOMAD) \
			--region $$region \
			$(DECIMATED) \
			--overwrite; \
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



# 3. Update the STAC-Geoparquet index after all tiles/years have run.
index-predictions:
	ldn index-to-stac-geoparquet \
	--dataset "prediction" \
	--region "all" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION)


# 4. Visualisation
make-mosaics-geomad:
	ldn make-mosaics \
	--dataset geomad \
	--region "all"

make-mosaics-prediction:
	ldn make-mosaics \
	--dataset prediction \
	--region "all"
