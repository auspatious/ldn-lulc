# FROM public.ecr.aws/lambda/python:3.12 # Could use this image but then GDAL is hard.
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.3

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv \
    git \
    && apt-get clean \
    && rm -rf /var/lib/{apt,dpkg,cache,log}

RUN pip install --break-system-packages poetry

WORKDIR /var/task

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.in-project true && \
    poetry install --no-root --only visualisation
RUN .venv/bin/pip install awslambdaric

# Copy app.py and static HTML.
COPY visualisation/app.py ./
COPY visualisation/static/index.html ./static/index.html

ENTRYPOINT [".venv/bin/python", "-m", "awslambdaric"]
CMD ["app.handler"]
