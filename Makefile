# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.

# List countries in grids
grid-list-countries-all:
	ldn grid list-countries --grids="all"

grid-list-countries-pacific:
	ldn grid list-countries --grids="pacific"
grid-list-countries-non-pacific:
	ldn grid list-countries --grids="non-pacific"

# Print tasks for given years and grids
print-tasks-2000-2024-all-grids:
	ldn print-tasks --years="2000-2024" --grids="all"

print-tasks-2024-non-pacific:
	ldn print-tasks --years="2024" --grids="non-pacific"

# Geomad tile
geomad-non-pacific-test-carribbean-atolls-belize:
	ldn geomad \
	--tile-id 127_134 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--no-all-bands \
	--region non-pacific
# Looks good! https://data.ldn.auspatious.com/ci_ls_geomad/0-0-0/127/134/2024/ci_ls_geomad_127_134_2024.stac-item.json

geomad-non-pacific-test-carribbean-land-suriname:
	ldn geomad \
	--tile-id 162_117 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--region non-pacific

geomad-non-pacific-test-cape-verde:
	ldn geomad \
	--tile-id 197_133 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--region non-pacific

geomad-non-pacific-test-comoros:
	ldn geomad \
	--tile-id 268_94 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--region non-pacific

geomad-pacific-test-fiji-antimeridian:
	ldn geomad \
	--tile-id 66_22 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--region pacific
# Looks good: https://data.ldn.auspatious.com/dep_ls_geomad/0-0-0/066/022/2024/dep_ls_geomad_066_022_2024.stac-item.json

geomad-pacific-test-fiji-volcanic:
	ldn geomad \
	--tile-id 63_20 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--region pacific
# Looks good: https://data.ldn.auspatious.com/dep_ls_geomad/0-0-0/063/020/2024/dep_ls_geomad_063_020_2024.stac-item.json

geomad-pacific-test-kiribati-atolls:
	ldn geomad \
	--tile-id 58_43 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--region pacific

geomad-singapore:
	ldn geomad \
	--tile-id 333_113 \
	--year 2000 \
	--version 0.0.1test \
	--overwrite \
	--all-bands \
	--decimated \
	--region non-pacific

geomad-singapore-2012:
	ldn geomad \
	--tile-id 333_113 \
	--year 2012 \
	--version 0.0.1 \
	--overwrite \
	--all-bands \
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


geomad-test-site-2000-2025:
	for year in $$(seq 2000 2025); do \
		ldn geomad \
			--tile-id $(GEOMAD_CASE_STUDY_TILE_ID) \
			--region $(GEOMAD_CASE_STUDY_REGION) \
			--year $$year \
			--version 0.0.1 \
			--overwrite \
			--all-bands; \
	done
