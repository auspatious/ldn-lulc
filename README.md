# Land Degradation Neutrality - Land Use/Land Cover

This repo contains scripts relevant to the development of new LULC datasets for the UN Small Island Developing States (SIDS).

The ldn folder contains an installable 


## Quickstart

1. Install GDAL (and its Python bindings) via Homebrew
```bash
  brew upgrade gdal
```

2. Install Rust (for datacube-compute):
```bash
brew install rustup
rustup-init
export PATH="$HOME/.cargo/bin:$PATH"
```

3. Install Poetry if you don't have it already:
  ```bash
  pip install poetry

4. Create and activate a Poetry virtual environment pointing at Homebrew's Python 3.12 and install dependencies:
  poetry env use $(brew --prefix python@3.12)/bin/python3.12
  poetry install
  ```

  For development dependencies:
```bash
   poetry install --with dev
```

5. Run the CLI tool:
```bash
   poetry run ldn --help
   poetry run make geomad-singapore
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
