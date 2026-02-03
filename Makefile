# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.
grid-list-countries-all:
	poetry run ldn grid list-countries

grid-list-countries-dep:
	poetry run ldn grid list-countries --set="dep"

grid-list-countries-non-dep:
	poetry run ldn grid list-countries --set="non_dep"

print-tasks:
	poetry run ldn print-tasks --years="2000-2024"

print-tasks-2024:
	poetry run ldn print-tasks --years="2024"

geomad-ci:
	poetry run ldn geomad --tile-id 136_142 --year 2024 --version 0.0.0 \
        --overwrite \
        --decimated \
        --no-all-bands

# No longer needed since splitting to DEP and CI grids, because the Fijian tiles use the DEP grid.
# geomad-ci-antimeridian:
# 	poetry run ldn geomad --tile-id 29_86 --year 2024 --version 0.0.0 \
#         --overwrite \
#         --no-decimated \
#         --all-bands

geomad-dep:
	poetry run ldn geomad --tile-id 65_19 --year 2024 --version 0.0.0 \
        --overwrite \
        --decimated \
        --all-bands \
        --grid-name dep

geomad-dep-2:
	poetry run ldn geomad --tile-id 66_19 --year 2024 --version 0.0.0 \
        --overwrite \
        --decimated \
        --all-bands \
        --grid-name dep
