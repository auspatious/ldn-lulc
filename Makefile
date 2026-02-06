# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.
grid-list-countries-all:
	poetry run ldn grid list-countries

grid-list-countries-dep:
	poetry run ldn grid list-countries --set="dep"

grid-list-countries-non-dep:
	poetry run ldn grid list-countries --set="non_dep"


print-tasks-2000-2024-both-grids:
	poetry run ldn print-tasks --years="2000-2024" --grids="both"

print-tasks-2024-ci:
	poetry run ldn print-tasks --years="2024" --grids="ci"


geomad-ci-test-carribbean-atolls-belize:
	poetry run ldn geomad \
	--tile-id 127_134 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--no-all-bands \
	--grid-name ci
# Looks good! https://data.ldn.auspatious.com/ci_ls_geomad/0-0-0/127/134/2024/ci_ls_geomad_127_134_2024.stac-item.json

geomad-ci-test-carribbean-land-suriname:
	poetry run ldn geomad \
	--tile-id 162_117 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--grid-name ci

geomad-ci-test-cape-verde:
	poetry run ldn geomad \
	--tile-id 197_133 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--grid-name ci

geomad-ci-test-comoros:
	poetry run ldn geomad \
	--tile-id 268_94 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--grid-name ci

geomad-dep-test-fiji-antimeridian:
	poetry run ldn geomad \
	--tile-id 66_22 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--grid-name dep
# Looks good: https://data.ldn.auspatious.com/dep_ls_geomad/0-0-0/066/022/2024/dep_ls_geomad_066_022_2024.stac-item.json

geomad-dep-test-fiji-volcanic:
	poetry run ldn geomad \
	--tile-id 63_20 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--grid-name dep
# Looks good: https://data.ldn.auspatious.com/dep_ls_geomad/0-0-0/063/020/2024/dep_ls_geomad_063_020_2024.stac-item.json

geomad-dep-test-kiribati-atolls:
	poetry run ldn geomad \
	--tile-id 92_43 \
	--year 2024 \
	--version 0.0.0 \
	--overwrite \
	--decimated \
	--all-bands \
	--grid-name dep
