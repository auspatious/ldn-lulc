FROM ghcr.io/osgeo/gdal:ubuntu-small-3.13.1 AS base

RUN apt-get update && apt-get install -y \
    python3-pip python3-venv git curl build-essential pkg-config \
    && apt-get clean && rm -rf /var/lib/{apt,dpkg,cache,log}

ENV CARGO_HOME="/usr/local/cargo" RUSTUP_HOME="/usr/local/rustup"
ENV PATH="$CARGO_HOME/bin:$PATH"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
ENV UV_PROJECT_ENVIRONMENT=/code/.venv UV_LINK_MODE=copy
WORKDIR /code

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev
RUN rustup self uninstall -y && rm -rf /usr/local/cargo /usr/local/rustup

COPY . .
RUN uv sync --frozen --no-dev
ENV PATH="/code/.venv/bin:$PATH"

# ---- test stage: adds dev deps on top of the production layer ----
FROM base AS test
RUN uv sync --frozen --group dev
CMD ["uv", "run", "pytest", "ldn/tests/", "-v"]

# ---- final stage = base, i.e. production image with no dev deps ----
FROM base AS final
RUN ldn --help
