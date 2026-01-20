# Land Degradation Neutrality - Land Use/Land Cover

This repo contains scripts relevant to the development of new LULC datasets for the UN Small Island Developing States (SIDS).

The ldn folder contains an installable 



## Quickstart

1. (Recommended) Create and activate a virtual environment:
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
```

#### Using Poetry (recommended)

<!-- 2. Ensure GDAL and its Python bindings are installed (not sure if this is needed yet). -->
2. Install Poetry if you don't have it already:
  ```bash
  pip install poetry
  ```
<!-- 4. (Optional) Install development dependencies:
  ```bash
  poetry install --with dev
  ``` -->
3. Install dependencies:
  ```bash
  poetry install
  ```
4. Run the CLI tool:
  ```bash
  poetry run ldn --help
  ```

#### Using pip (alternative)
<!-- 2. Ensure GDAL and its Python bindings are installed (not sure if this is needed yet). -->
2. Install the package and dependencies:
  ```bash
  pip install -e .
  ```
3. Run the CLI tool:
  ```bash
  ldn --help
  ```


### Development

For development purposes, you can install the package with development dependencies:


```bash
# Using Poetry
poetry install --with dev
```

```bash
# Using pip
pip install -e ".[dev]"
```

### To add a dependency

Run: `poetry add --group dev pytest`


### To run tests

Simply run: `pytest`


## Running Commands

You can run these:
- `poetry run ldn --help`
- `poetry run ldn version`
- `poetry run ldn grid list-countries` or `make grid-list-countries`

Future commands could look like:
- Run our random forest model to predict/classify a tile: `ldn process --tile-id xxx`.
- Get a class: `ldn grid <class_name>` e.g. forest or grassland


## Building and Running the Docker Image

To build the Docker image locally using [Buildx](https://docs.docker.com/buildx/working-with-buildx/), run:

```bash
docker buildx build . --tag ldn-lulc:latest
```

Once built, you can run any command in the container:

`docker run --rm ldn-lulc:latest ldn grid list-countries`
