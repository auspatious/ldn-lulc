# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.

# Workflow:
# 1. Run GeoMAD for all tiles/years
# 2. Run index GeoMAD (STAC-Geoparquet)
# 3. Run prediction for all tiles/years
# 4. Run index prediction (STAC-Geoparquet)
# 5. Run make-mosaic for geomad and prediction datasets
# 6. Visualisation app will update automatically when mosaics are updated (unless version/path is different).

VERSION_GEOMAD ?= 0-0-4
VERSION_PREDICTION ?= 0-0-1
DECIMATED ?= --no-decimated
YEAR ?= 2020
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

# Test case sites as tile_id:region pairs.
KIRIBATI_ATOLLS      := 58_43:pacific
FIJI_VOLCANIC        := 63_20:pacific
FIJI_ANTIMERIDIAN    := 66_22:pacific
BELIZE_ATOLLS        := 119_126:non-pacific
SURINAME             := 152_110:non-pacific
CAPE_VERDE           := 185_125:non-pacific
COMOROS              := 251_88:non-pacific
SINGAPORE            := 312_105:non-pacific 312_106:non-pacific

# TEST_SITES := $(KIRIBATI_ATOLLS) $(FIJI_VOLCANIC) $(FIJI_ANTIMERIDIAN) \
# 	$(BELIZE_ATOLLS) $(SURINAME) $(CAPE_VERDE) $(COMOROS) $(SINGAPORE)
TEST_SITES := $(FIJI_VOLCANIC)
# # TEST_SITES := $(KIRIBATI_ATOLLS) $(FIJI_ANTIMERIDIAN) \
# # 	$(BELIZE_ATOLLS) $(SURINAME) $(CAPE_VERDE) $(COMOROS) $(SINGAPORE)

# TEST_YEARS := 2000 2011 2024 # Semi-representative years to assess quality.
TEST_YEARS := 2020
# TEST_YEARS := 2011 # Semi-representative years to assess quality.
# TEST_YEARS := 2000 2024 # Semi-representative years to assess quality.

# Run geomad for all test case sites for the one YEAR.
geomad-test-case-sites-3-years:
	for site in $(TEST_SITES); do \
		tile_id=$${site%%:*}; \
		region=$${site##*:}; \
		for year in $(TEST_YEARS); do \
			ldn geomad \
				--tile-id $$tile_id \
				--region $$region \
				--year $$year \
				--version $(VERSION_GEOMAD) \
				--product-owner ausp \
				--include-shadow \
				--ls7-buffer-years 1 \
				$(DECIMATED) \
				--overwrite; \
		done; \
	done

# Run geomad for all test case sites for years 2000-2025.
geomad-2000-2025:
	for site in $(TEST_SITES); do \
		tile_id=$${site%%:*}; \
		region=$${site##*:}; \
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

# 3. Predict LULC for the test tiles and 2020.
predict-lulc-test-tiles-2020:
	for site in $(TEST_SITES); do \
		tile_id=$${site%%:*}; \
		region=$${site##*:}; \
		ldn classify classify \
			--tile-id $$tile_id \
			--year $(YEAR) \
			--version $(VERSION_PREDICTION) \
			--version-geomad $(VERSION_GEOMAD) \
			--region $$region \
			--output-bucket="data.ldn.auspatious.com" \
			--model-path="ldn/lulc_random_forest_model.joblib" \
			--xy-chunk-size 1024 \
			$(DECIMATED) \
			--overwrite; \
	done


# 4. Update the STAC-Geoparquet index after all tiles/years have run.
index-predictions:
	ldn index-to-stac-geoparquet \
	--prefix "ausp_ls_lulc_prediction" \
	--output-filename "ausp_ls_lulc_prediction" \
	--version $(VERSION_PREDICTION)

# Visualisation
make-mosaic-all-2020:
	ldn make-mosaics \
	--dataset all \
	--years $(YEAR) \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION)
make-mosaic-geomad-2020:
	ldn make-mosaics \
	--dataset geomad \
	--years $(YEAR) \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION)
make-mosaic-prediction-2020:
	ldn make-mosaics \
	--dataset prediction \
	--years $(YEAR) \
	--version-geomad $(VERSION_GEOMAD) \
	--version-prediction $(VERSION_PREDICTION)
