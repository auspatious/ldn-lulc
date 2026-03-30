# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.

# Workflow:
# 1. Run GeoMAD for all tiles/years
# 2. Run index GeoMAD (STAC-Geoparquet)
# 3. Run prediction for all tiles/years
# 4. Run index prediction (STAC-Geoparquet)
# 5. Run make-mosaic for geomad and prediction datasets
# 6. Visualisation app will update automatically when mosaics are updated (unless version/path is different).

VERSION ?= 0-0-2b
DECIMATED ?= --decimated
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

# Geomad tile
geomad-non-pacific-test-carribbean-atolls-belize:
	ldn geomad \
	--tile-id 119_126 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--product-owner ausp \
	--no-all-bands \
	--region non-pacific

geomad-non-pacific-test-carribbean-land-suriname:
	ldn geomad \
	--tile-id 152_110 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--product-owner ausp \
	--all-bands \
	--region non-pacific

geomad-non-pacific-test-cape-verde:
	ldn geomad \
	--tile-id 185_125 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--product-owner ausp \
	--all-bands \
	--region non-pacific

geomad-non-pacific-test-comoros:
	ldn geomad \
	--tile-id 251_88 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--product-owner ausp \
	--all-bands \
	--region non-pacific

geomad-pacific-test-fiji-antimeridian:
	ldn geomad \
	--tile-id 66_22 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--product-owner ausp \
	--all-bands \
	--region pacific

geomad-pacific-test-fiji-volcanic:
	ldn geomad \
	--tile-id 63_20 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--product-owner ausp \
	--all-bands \
	--region pacific

geomad-pacific-test-kiribati-atolls:
	ldn geomad \
	--tile-id 58_43 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--product-owner ausp \
	--all-bands \
	--region pacific

geomad-singapore:
	ldn geomad \
	--tile-id 312_106 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	--all-bands \
	$(DECIMATED) \
	--product-owner ausp \
	--region non-pacific

geomad-singapore-2:
	ldn geomad \
	--tile-id 312_105 \
	--year $(YEAR) \
	--version $(VERSION) \
	--overwrite \
	--all-bands \
	$(DECIMATED) \
	--product-owner ausp \
	--region non-pacific


geomad-test-case-sites-2020:
	$(MAKE) geomad-pacific-test-kiribati-atolls
	$(MAKE) geomad-pacific-test-fiji-volcanic
	$(MAKE) geomad-pacific-test-fiji-antimeridian
	$(MAKE) geomad-non-pacific-test-carribbean-atolls-belize
	$(MAKE) geomad-non-pacific-test-carribbean-land-suriname
	$(MAKE) geomad-non-pacific-test-cape-verde
	$(MAKE) geomad-non-pacific-test-comoros


geomad-2000-2025:
	for site in 58_43:pacific 63_20:pacific 66_22:pacific 119_126:non-pacific 152_110:non-pacific 185_125:non-pacific 251_88:non-pacific 312_105:non-pacific 312_106:non-pacific; do \
		tile_id=$${site%%:*}; \
		region=$${site##*:}; \
		for year in $$(seq 2000 2025); do \
			ldn geomad \
				--tile-id $$tile_id \
				--region $$region \
				--year $$year \
				--version $(VERSION) \
				--product-owner ausp \
				--overwrite; \
		done; \
	done


# TODO: Might have to do this per 2 regions (pacific and non-pacific) because the prefix is different.
index-geomad:
	ldn index-to-stac-geoparquet \
	--prefix "ausp_ls_geomad" \
	--output-filename "ausp_ls_geomad" \
	--version "0-0-2"


###### Train and Predict

# 1. Training data is created in notebooks/training_data/0_Generate_Training_Points.ipynb.

# 2. Train a model with the training data made in the notebook above.
train-model:
	ldn train-predict train-model

# 3. Predict LULC for a tile and year.
predict-lulc-pacific-test-tiles-2020:
	for site in 66_22 58_43 63_20; do \
		ldn train-predict predict \
		--tile-id $$site \
		--year $(YEAR) \
		--version 0-0-1 \
		--region pacific \
		--output-bucket="data.ldn.auspatious.com" \
		--model-path="ldn/lulc_random_forest_model.joblib" \
		--xy-chunk-size 1024 \
		--decimated \
		--overwrite; \
	done

# TODO: No non-pacific tile will work until we redo the geomad, stac-geoparquet, and tile index.
# predict-lulc-non-pacific-test-tiles-2020:
# 	for site in 119_126 152_110 185_125 251_88; do \
# 		ldn train-predict predict \
# 		--tile-id $$site \
# 		--year $(YEAR) \
# 		--version 0-0-1 \
# 		--region non-pacific \
# 		--output-bucket="data.ldn.auspatious.com" \
# 		--model-path="ldn/lulc_random_forest_model.joblib" \
# 		--xy-chunk-size 1024 \
# 		--decimated \
# 		--overwrite; \
# 	done


# 4. Update the STAC-Geoparquet index after all tiles/years have run.
# TODO: Update the index STAC-Geoparquet after all tiles/years have run.
index-predictions:
	ldn index-to-stac-geoparquet \
	--prefix "ausp_ls_lulc_prediction" \
	--output-filename "ausp_ls_lulc_prediction" \
	--version "0-0-1"

# Visualisation
make-mosaic-all-2020:
	ldn make-mosaics \
	--dataset all \
	--years $(YEAR)
make-mosaic-geomad-2020:
	ldn make-mosaics \
	--dataset geomad \
	--years $(YEAR)
make-mosaic-prediction-2020:
	ldn make-mosaics \
	--dataset prediction \
	--years $(YEAR)
