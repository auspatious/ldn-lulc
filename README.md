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


# Notes 2026-02-27:

## Goal of training data, model, and prediction:

#### Sites:
- Fiji
- Singapore
- etc.
Use get_gadm function. Not by AOI or any other search. method. Buffer country boundary 100m.

### Steps: 
1. Training data:
For each site extract training data.
  -> Find product agreement. mask to gadm 100m buffer.
  -> Add geomad, indices, elevation etc.
  -> Write csv per site of training data.
do this in a notebook. don't commit training data.

2. Training model:
Try one model for all sites. (append all CSVs)
export model dump (python pickle?)
in future we may need to make different models for different regions and year ranges.
train the model using the geomad of the year of the input products.

3. Predict:
per grid tile:
  per year:
    load geomad/indices/elevation etc.
    make using get_gadm (buffered 100m)
    predict
This is as a command. 

First version do all of this in a single notebook.


### High prio:
- Remove NetCDF and raster reprojection.
- Then merge this big PR. Then do all the next version tweaks in a new PR.
