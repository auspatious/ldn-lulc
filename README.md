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
```

4. Create and activate a Poetry virtual environment pointing at Homebrew's Python 3.12 and install dependencies:
This installs main group. Deps like `cogeo-mosaic` and `boto3` are in both main and visualisation group.
```bash
  poetry env use $(brew --prefix python@3.12)/bin/python3.12
  poetry install
```

  For development dependencies:
```bash
   poetry install --with dev
```

  For visualisation dependencies:
```bash
   poetry install --no-root --only visualisation
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

Simply run: `poetry run pytest` or for a specific file: `poetry run pytest ldn/tests/test_mosaic.py`


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


## Visualisation

A tile server for viewing GeoMedian/GeoMAD and predicted LULC mosaics, built with
[TiTiler](https://developmentseed.org/titiler/) and deployed as an AWS Lambda behind API Gateway.

### Prerequisites

- AWS credentials configured (`aws configure` or environment variables)
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- Docker

### Deploy

From the project root:
```bash
poetry install --with visualisation # Needed for ldn make-mosaics command.
bash visualisation/deploy.sh
```

This will:
1. Build mosaic JSON files and upload to S3
2. Create an ECR repository (if it doesn't exist)
3. Build and push the Docker image
4. Deploy the Lambda + API Gateway via Terraform

### Run locally

```bash
poetry install --with visualisation
poetry run uvicorn visualisation.app:app --host 0.0.0.0 --port 8081 --reload
```

### Current deployment

https://mmufb4pjqf.execute-api.us-west-2.amazonaws.com/