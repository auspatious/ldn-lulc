# Commands for working with the grids, GeoMAD, training data, and LULC classification.

# Workflow for 2 regions writing to different buckets/paths:
# 1. Run GeoMAD for all tiles/years
# 2. Run index GeoMAD (STAC-Geoparquet)
# 3. Make training data for many tiles for 2020
# 4. Train a single model for both regions (in notebooks/training_data/1_Train_Model.ipynb)
# 5. Run LULC classification for all tiles/years
# 6. Run index LULC (STAC-Geoparquet)
# 7. Create collection JSONs per dataset
# 8. Visualisation app directly reads STAC-Geoparquets and COGs

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


#### Training Data
# Geomad version: 0-2-1 in DEP staging and Source.Coop. 0-3-0 in DEP public.
# TODO: read from prod because it is more up-to-date than staging, should we use prod?
# Or read from and write to the same env?
# Training and test samples for a list of tiles (both regions). Both regions are written under dep_ls_lulc.
# Pacific GeoMAD is read from dep_ls_geomad. Non-Pacific GeoMAD is read from the many-region index on Source.Coop.
# Usage: $(call generate_samples,<tiles>,<train|test>)
define generate_samples
	for site in $(1); do \
		tile_id=$$(echo $$site | cut -d: -f1); \
		region=$$(echo $$site | cut -d: -f2); \
		country_name=$$(echo $$site | cut -d: -f3 | tr '_' ' '); \
		country_code=$$(echo $$site | cut -d: -f4); \
		if [ "$$region" = pacific ]; then \
			geomad_args="--geomad-bucket dep-public-staging --single-region-geomad --product-owner-geomad dep"; \
		else \
			geomad_args="--geomad-bucket us-west-2.opendata.source.coop --no-single-region-geomad"; \
		fi; \
		ldn training generate-training-data \
			--tile-id $$tile_id \
			--region $$region \
			--country-name "$$country_name" \
			--country-code "$$country_code" \
			--geomad-version 0-2-1 \
			$$geomad_args \
			--output-bucket dep-public-staging \
			--single-region-output \
			--product-owner-output dep \
			--split $(2) \
			--no-overwrite; \
	done;
endef

training-data-generate:
	$(call generate_samples,$(TRAINING_TILES),train)

#### Make the model using ldn-lulc/notebooks/1_Train_Model.ipynb

# Frozen held-out test set. Never train on this. Written to dep_ls_lulc/test_data/.
test-data-generate:
	$(call generate_samples,$(TEST_TILES),test)


###### LULC Classification

print-tasks-lulc-2000-non-pacific:
	ldn print-tasks \
		--years="2000" \
		--region="non-pacific" \
		--geomad-version 0-2-1 \
		--dataset lulc \
		--no-overwrite \
		--bucket dep-public-staging;


# Classify

# Classify all test tiles for 2020, the year of the test labels (score with notebooks/LULC_Testing/LULC_Test.ipynb).
# Pacific GeoMAD is read from dep_ls_geomad and written to dep_ls_lulc. Non-Pacific GeoMAD is read from the
# many-region index on Source.Coop and written to ci_ls_lulc.
# Use local model to avoid 1GB download from S3 per tile

# 	for site in $(TEST_TILES) do \
lulc-predict-test:
	for site in 050_041:pacific; do \
		tile_id=$$(echo $$site | cut -d: -f1); \
		region=$$(echo $$site | cut -d: -f2); \
		if [ "$$region" = pacific ]; then \
			geomad_args="--geomad-bucket dep-public-staging --product-owner dep --single-region-geomad"; \
		else \
			geomad_args="--geomad-bucket us-west-2.opendata.source.coop --product-owner ci --no-single-region-geomad"; \
		fi; \
		for year in 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025; do \
			ldn lulc run \
				--tile-id $$tile_id \
				--year $$year \
				--region $$region \
				--version 0-1-1 \
				--geomad-version 0-2-1 \
				$$geomad_args \
				--output-bucket dep-public-staging \
				--model-path="/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/models/0-1-1/2020/lulc_random_forest_model_2020.joblib" \
				--no-overwrite; \
		done; \
	done;


index-lulc-test-dep-staging:
	ldn index-to-stac-geoparquet \
	--dataset lulc \
	--lulc-version 0-1-1 \
	--single-region \
	--product-owner dep \
	--bucket dep-public-staging;
index-lulc-test-dep-staging-ci:
	ldn index-to-stac-geoparquet \
	--dataset lulc \
	--lulc-version 0-1-1 \
	--single-region \
	--product-owner ci \
	--bucket dep-public-staging;

# Visualisation app: check formatting, lint and types, or apply fixes.
viz-lint:
	cd viz && pnpm lint && pnpm typecheck;

viz-format:
	cd viz && pnpm format;
