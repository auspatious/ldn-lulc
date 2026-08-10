FROM ghcr.io/osgeo/gdal:ubuntu-small-3.13.1

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    git \
    && apt-get clean \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
ENV UV_PROJECT_ENVIRONMENT=/var/task/.venv \
    UV_LINK_MODE=copy

WORKDIR /var/task

COPY pyproject.toml uv.lock ./
# --only-group excludes the project itself AND default groups (dev),
# giving you just the visualisation group — same as the old
# `poetry install --no-root --only visualisation`.
RUN uv sync --frozen --only-group visualisation

COPY visualisation/app.py ./
COPY visualisation/static/ ./static/

ENTRYPOINT [".venv/bin/python", "-m", "awslambdaric"]
CMD ["app.handler"]
