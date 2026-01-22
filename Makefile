# Here we will store commands for working with the grid, GeoMAD, training data, and ML models.
grid-list-countries:
	poetry run ldn grid list-countries

print-tasks:
	poetry run ldn print-tasks --years="2000-2024"

geomad:
	poetry run ldn geomad --tile-id 136_142 --year 2024 --version 0.0.0 \
        --overwrite \
        --decimated \
        --no-all-bands
