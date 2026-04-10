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
# TEST_TILES is a list of tuples: (tile_id, region, {country_name: country_code}) e.g. ("089_016", "pacific", {"Cook Islands": "COK"})
TEST_TILES := $(shell python3 -c "from ldn.utils import TEST_TILES; print(' '.join([f'{t[0]}:{t[1]}' for t in TEST_TILES]))")

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

print-tasks-2000-2024-all-grids:
	ldn print-tasks --years="2000-2024" --grids="all"


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
				--product-owner ausp \
				--include-shadow \
				--ls7-buffer-years 1 \
				--overwrite; \
		done; \
	done


index-geomad:
	ldn index-to-stac-geoparquet \
	--prefix "ausp_ls_geomad" \
	--output-filename "ausp_ls_geomad" \
	--version $(VERSION_GEOMAD)


###### Classification/Prediction

# 1. Training data is created in notebooks/training_data/0_Generate_Training_Points.ipynb.

# 2. Train a model with the training data made in the notebook above.
# train-model:
# 	ldn classify train-model


# 3. Predict LULC for the test tiles and one year (2025).
# predict 20205 and 2024? If the Rarotonga Geomedian looks good then.

# TODO: Run for all years in future
predict-lulc-test-tiles-a-few-years:
	for site in $(TEST_TILES); do \
		tile_id=$${site%%:*}; \
		region=$${site#*:}; region=$${region%%:*}; \
		for year in $$(seq 2024 2025); do \
			ldn classify classify \
				--tile-id $$tile_id \
				--year year \
				--version $(VERSION_PREDICTION) \
				--version-geomad $(VERSION_GEOMAD) \
				--region $$region \
				--output-bucket="data.ldn.auspatious.com" \
				--model-path="ldn/models/$(VERSION_PREDICTION)/lulc_random_forest_model.joblib" \
				--xy-chunk-size 1024 \
				$(DECIMATED) \
				--overwrite; \
		done; \
	done


# 4. Update the STAC-Geoparquet index after all tiles/years have run.
index-predictions:
	ldn index-to-stac-geoparquet \
	--prefix "ausp_ls_lulc_prediction" \
	--output-filename "ausp_ls_lulc_prediction" \
	--version $(VERSION_PREDICTION)


# Visualisation
# make-mosaics-all:
# 	ldn make-mosaics \
# 	--dataset all \
# 	--years "2000-2025" \
# 	--version-geomad $(VERSION_GEOMAD) \
# 	--version-prediction $(VERSION_PREDICTION)
make-mosaics-geomad-all-years:
	ldn make-mosaics \
	--dataset geomad \
	--years "2000-2025" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION)
# TODO: Run for all years in future
make-mosaics-prediction-some-years:
	ldn make-mosaics \
	--dataset prediction \
	--years "2024-2025" \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION)
