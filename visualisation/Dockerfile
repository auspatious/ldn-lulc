FROM ghcr.io/osgeo/gdal:ubuntu-small-3.13.1

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    python3-dev \
    gcc \
    git \
    && apt-get clean \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
ENV UV_PROJECT_ENVIRONMENT=/var/task/.venv \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-system

WORKDIR /var/task

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --only-group visualisation && \
    chmod +x .venv/bin/python*

COPY visualisation/app.py ./
COPY visualisation/static/ ./static/

ENTRYPOINT [".venv/bin/python", "-m", "awslambdaric"]
CMD ["app.handler"]
