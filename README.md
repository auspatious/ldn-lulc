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

This installs the main dependency group plus the `dev` group (synced by default).
```bash
  uv sync
```

For main dependencies only (no dev tools):
```bash
   uv sync --no-dev
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

Simply run: `uv run pytest` or for a specific file: `uv run pytest ldn/tests/test_geomad.py`


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


## Viz

Client-side viewer for GeoMAD/LULC COGs. Reads STAC GeoParquet directly in
the browser (duckdb-wasm) and renders with deck.gl-raster - no tiling
server.

### Quickstart

```
cd viz
npm install
npm run dev
```


### Current deployment

Not yet deployed. Old titiler version here: https://mmufb4pjqf.execute-api.us-west-2.amazonaws.com/


### Features

This is for agents to read so new features don't cause regressions.

#### Done:

- Display GeoMAD (z=0) and LULC (z=1) COGs directly in the browser. Uses STAC-Geoparquet file as a reference.
- Display LULC legend.
- For each layer have an opacity and visibility control.
- Allow user to select year to visualise.
- Allow user to swipe/compare different datasets and years.
- Add layer config and map view into url parameters.


#### WIP:

3 main problems:
1. antimeridian-spanning COGs render badly or not at all
2. need to render world copies so we see data on both sides of the antimeridian
3. geomad needs nearest, not linear sampling so it is crisp and artifact-free.

The viz/vendor folder contains a build of https://github.com/willjnz/deck.gl-raster/tree/willjnz/frankenstein which should solve all 3. If these fixes get merged, we can reference that future version. For now we do this hacky way.

- Add tiles json layer (z=2). https://raw.githubusercontent.com/auspatious/ldn-lulc/refs/heads/main/ldn/sids_all_tiles.geojson

- Make the ui nicer. e.g. map controls and logo etc.
- Add basemap switcher

-  the data in 3832 renders fine (apart from the am-crossing items). the data in 6933 throws the projection error and doesn't show. "installHook.js:1 deck: loading COG sources: undefined Could not get projection name from: [object Object]"
