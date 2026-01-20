FROM python:3.11-slim
# Change to this once we need GDAL
# FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.3

# Minimal system dependencies for running Python CLI
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    # Installs SSL/TLS certificates, which are needed for secure HTTPS connections (e.g., downloading packages, accessing APIs).
    ca-certificates \
    # Cleans up the local repository of retrieved package files, reducing image size by removing cached .deb files after installation.
    && apt-get clean \
    # Remove these dirs (caches, logs) after installing deps to reduce image size
    && rm -rf /var/lib/{apt,dpkg,cache,log}


# Copy source code into the container
WORKDIR /code
COPY . .


# Install Python dependencies (assumes pyproject.toml present)
RUN pip install --upgrade pip && pip install .


# Smoketest
RUN ldn --help
