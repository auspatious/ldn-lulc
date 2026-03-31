FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.3

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

# Install Rust via rustup (needed to build datacube-compute)
ENV CARGO_HOME="/usr/local/cargo" RUSTUP_HOME="/usr/local/rustup"
ENV PATH="$CARGO_HOME/bin:$PATH"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

RUN pip install --break-system-packages poetry

WORKDIR /code

COPY pyproject.toml poetry.lock ./
# Install dependencies first to leverage Docker caching. 
# Keep layer separate from installing the package itself to avoid re-building dependencies when our code changes.
# Make venv in-project (in container's working directory).
RUN poetry config virtualenvs.in-project true && \
    poetry install --no-root

# Rust is no longer needed after dependencies are built
RUN rustup self uninstall -y && rm -rf /usr/local/cargo /usr/local/rustup

COPY . .
# Install the package itself. Keep separate from dependencies to avoid re-building dependencies when our code changes.
RUN poetry install --only-root

ENV PATH="/code/.venv/bin:$PATH"

# Smoketest
RUN ldn --help
