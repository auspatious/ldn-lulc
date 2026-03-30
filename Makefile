# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.

VERSION ?= 0-0-1
DECIMATED ?= --decimated
YEAR ?= 2024

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
	--tile-id 127_134 \
	--year 2024 \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--no-all-bands \
	--region non-pacific

geomad-non-pacific-test-carribbean-land-suriname:
	ldn geomad \
	--tile-id 162_117 \
	--year 2024 \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--all-bands \
	--region non-pacific

geomad-non-pacific-test-cape-verde:
	ldn geomad \
	--tile-id 197_133 \
	--year 2024 \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--all-bands \
	--region non-pacific

geomad-non-pacific-test-comoros:
	ldn geomad \
	--tile-id 268_94 \
	--year 2024 \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--all-bands \
	--region non-pacific

geomad-pacific-test-fiji-antimeridian:
	ldn geomad \
	--tile-id 66_22 \
	--year 2024 \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--all-bands \
	--region pacific

geomad-pacific-test-fiji-volcanic:
	ldn geomad \
	--tile-id 63_20 \
	--year 2024 \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--all-bands \
	--region pacific

geomad-pacific-test-kiribati-atolls:
	ldn geomad \
	--tile-id 58_43 \
	--year 2024 \
	--version $(VERSION) \
	--overwrite \
	$(DECIMATED) \
	--all-bands \
	--region pacific

geomad-singapore:
	ldn geomad \
	--tile-id 333_113 \
	--year 2020 \
	--version $(VERSION) \
	--overwrite \
	--all-bands \
	$(DECIMATED) \
	--region non-pacific

geomad-singapore-2:
	ldn geomad \
	--tile-id 333_112 \
	--year 2020 \
	--version $(VERSION) \
	--overwrite \
	--all-bands \
	$(DECIMATED) \
	--region non-pacific


geomad-test-case-sites:
	$(MAKE) geomad-pacific-test-kiribati-atolls
	$(MAKE) geomad-pacific-test-fiji-volcanic
	$(MAKE) geomad-pacific-test-fiji-antimeridian
	$(MAKE) geomad-non-pacific-test-carribbean-atolls-belize
	$(MAKE) geomad-non-pacific-test-carribbean-land-suriname
	$(MAKE) geomad-non-pacific-test-cape-verde
	$(MAKE) geomad-non-pacific-test-comoros

# 333_112, 333_113 is Singapore
# 63,20 is SW Fiji
# GEOMAD_CASE_STUDY_TILE_ID ?= 63_20
# GEOMAD_CASE_STUDY_REGION ?= pacific
# GEOMAD_CASE_STUDY_TILE_ID ?= 333_112
# GEOMAD_CASE_STUDY_REGION ?= non-pacific
GEOMAD_CASE_STUDY_TILE_ID ?= 333_113
GEOMAD_CASE_STUDY_REGION ?= non-pacific


geomad-2000-2025:
	for site in 58_43:pacific 63_20:pacific 66_22:pacific 127_134:non-pacific 162_117:non-pacific 197_133:non-pacific 268_94:non-pacific; do \
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


# Visualisation
make-mosaic-all-2020:
	ldn make-mosaics \
	--dataset all \
	--years "2020"
make-mosaic-geomad-2020:
	ldn make-mosaics \
	--dataset geomad \
	--years "2020"
make-mosaic-prediction-2020:
	ldn make-mosaics \
	--dataset prediction \
	--years "2020"


###### Train and Predict

# 1. Training data is created in notebooks/training_data/0_Generate_Training_Points.ipynb.

# 2. Train a model with the training data made in the notebook above.
train-model:
	ldn train-predict train-model
# 3. Predict LULC for a tile and year.
# TODO: Update parameters.
predict-lulc-singapore-2020:
	ldn train-predict predict \
	--tile-id 333_112 \
	--year 2020 \
	--version 0-0-1 \
	--region non-pacific \
