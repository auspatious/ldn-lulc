# Land Degradation Neutrality - Land Use/Land Cover

This repo contains scripts relevant to the development of new LULC datasets for the UN Small Island Developing States (SIDS).

The ldn folder contains an installable package.

We will make GeoMedian/Geomad for all tiles that cover the SIDS and Pacific countries, for the years 2000-2024. Then we will create training data from existing products (for 2020). Then we will train a model and predict for all SIDS and Pacific countries for all years.


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

3. Install uv if you don't have it already:
```bash
  brew install uv
```
or
```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
```

4. Sync dependencies. uv will create a `.venv` automatically using the Python version pinned by `requires-python` in `pyproject.toml` (installing it if needed).

This installs the main dependency group plus the `dev` group (synced by default). Deps like `cogeo-mosaic` and `boto3` are in both the main group and the `visualisation` group.
```bash
  uv sync
```

  For main dependencies only (no dev tools):
```bash
   uv sync --no-dev
```

  For visualisation dependencies only (no project, no dev group):
```bash
   uv sync --only-group visualisation
```

5. Run the CLI tool:
```bash
   uv run ldn --help
   uv run make {command from Makefile}
```

## AWS

Docs on AWS SSO here: https://github.com/digitalearthpacific/internal-documentation/blob/main/technical/1-systems-access.md

- AWS credentials configured (per profile) `aws configure sso`
- AWS SSO `aws sso login --profile xxx`


### To add a dependency

Run: `uv add --group dev pytest`

Others:
```bash
uv add "dep-tools @ git+https://github.com/digitalearthpacific/dep-tools.git"
uv add "datacube-compute @ git+https://github.com/auspatious/datacube-compute.git"
```

### To run tests

Simply run: `uv run pytest` or for a specific file: `uv run pytest ldn/tests/test_mosaic.py`


### Pre-commit hooks

Formats Python, YAML, and JSON.

To use pre-commit to automatically run ruff, and other checks on each commit, make sure the development dependencies are installed and then run:

```bash
uv run pre-commit install
```

Note that you will need to run `uv run pre-commit run --all-files` if any of the hooks in `.pre-commit-config.yaml` change.


## Running Commands

You can run these:
- `uv run ldn --help`
- `uv run ldn version`
- `uv run ldn grid list-countries` or `make grid-list-countries`

Future commands could look like:
- Get a class: `ldn grid <class_name>` e.g. forest or grassland


## Building and Running the Docker Image

To build the Docker image locally using [Buildx](https://docs.docker.com/buildx/working-with-buildx/), run:

```bash
docker buildx build . --tag ldn-lulc:latest
docker run --rm ldn-lulc:latest ldn --help
```

Once built, you can run any command in the container:

`docker run --rm ldn-lulc:latest ldn grid list-countries`


## Running in EC2 VM

Much faster than running locally.

1. Need to set up SSH access
2. SSH in to VM "Alex Hack Box 2026"
3. Your public key path equivalent of `ssh -i ~/.ssh/id_ed25519 ubuntu@44.230.85.235`
4. Pull code
5. Install
6. Run. Use `tmux` so that even if it disconnects the command will keep running.
Run `tmux new -s geomad` then when inside run `uv run make geomad-2000-2025`. To detach, run `Ctrl+B, D` and it'll keep running. To reattach run `tmux attach -t geomad`. To kill it from outside run `tmux kill-session -t geomad`.

## Visualisation

A tile server for viewing GeoMedian/GeoMAD and predicted LULC mosaics, built with
[TiTiler](https://developmentseed.org/titiler/) and deployed as an AWS Lambda behind API Gateway.

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- Docker

### Run locally

```bash
uv sync --group visualisation
uv run uvicorn visualisation.app:app --host 0.0.0.0 --port 8081 --reload
```

### Deploy

From the project root:
```bash
uv sync --group visualisation # Needed for ldn make-mosaics command.
uv run bash visualisation/deploy.sh
```

This will:
1. Build mosaic JSON files and upload to S3
2. Create an ECR repository (if it doesn't exist)
3. Build and push the Docker image
4. Deploy the Lambda + API Gateway via Terraform

### Current deployment

https://mmufb4pjqf.execute-api.us-west-2.amazonaws.com/


# Instances:
## 1. Source.Coop

Data product: https://source.coop/auspatious/geomad-sids

2 regions.

Info here: https://github.com/auspatious/ldn-lulc/Source.Coop_README.md


## 2. Digital Earth Pacific

Environments: Staging and Production.

1 region.

TODO: Detail DEP.


## Environment Variables

See .env.example on how to set env vars. After cloning this repo you need to copy .env.example to .env

`source .env`

you need to set `AWS_PROFILE` using `export AWS_PROFILE=XXX`
