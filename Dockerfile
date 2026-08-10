FROM ghcr.io/osgeo/gdal:ubuntu-small-3.13.1

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    git \
    curl \
    build-essential \
    pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/{apt,dpkg,cache,log}
# Install Rust via rustup (needed to build datacube-compute)
ENV CARGO_HOME="/usr/local/cargo" RUSTUP_HOME="/usr/local/rustup"
ENV PATH="$CARGO_HOME/bin:$PATH"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
ENV UV_PROJECT_ENVIRONMENT=/code/.venv \
    UV_LINK_MODE=copy

WORKDIR /code

COPY pyproject.toml uv.lock ./
# Install dependencies first to leverage Docker caching.
# --no-dev keeps the "dev" group (matplotlib, pytest, etc.) out of the image.
RUN uv sync --frozen --no-install-project --no-dev

# Rust is no longer needed after dependencies are built
RUN rustup self uninstall -y && rm -rf /usr/local/cargo /usr/local/rustup

COPY . .
# Install the package itself.
RUN uv sync --frozen --no-dev

ENV PATH="/code/.venv/bin:$PATH"

# Smoketest
RUN ldn --help
