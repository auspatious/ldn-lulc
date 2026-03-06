# Land Degradation Neutrality - Land Use/Land Cover

This repo contains scripts relevant to the development of new LULC datasets for the UN Small Island Developing States (SIDS).

The ldn folder contains an installable 



## Quickstart
1. Ensure GDAL and its Python bindings are installed
```bash
brew upgrade gdal
```

1b. Ensure Rust is installed (for datacube-compute):
```bash
brew install rustup
rustup-init
export PATH="$HOME/.cargo/bin:$PATH"
```

2. Install Poetry if you don't have it already and create and activate a virtual environment:
  ```bash
  pip install poetry
  poetry env use $(brew --prefix python@3.12)/bin/python3.12
  poetry env info
  ```

#### Using Poetry (recommended)
3a. (Optional) Install development dependencies:
  ```bash
  poetry install --with dev
  ```
3b. Install dependencies:
  ```bash
  poetry install
  ```
4. Run the CLI tool:
  ```bash
  poetry run ldn --help
  ```
5. Run Makefile commands:
  ```bash
  poetry run make geomad-singapore
  ```


### Development

For development purposes, you can install the package with development dependencies:


```bash
# Using Poetry
poetry install --with dev
```

### To add a dependency

Run: `poetry add --dev pytest`

Others:
poetry add "dep-tools@git+https://github.com/digitalearthpacific/dep-tools.git"
poetry add "datacube-compute@git+https://github.com/auspatious/datacube-compute.git"

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
