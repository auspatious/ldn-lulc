# Custom TiTiler with STACReader (RGB from separate band COGs)

Run the custom app in `visualisation/titiler.py` which uses `STACReader` as the
dataset reader, allowing RGB composites from separate per-band COGs via STAC items.

```bash
  cd visualisation && poetry run uvicorn app:app --host 0.0.0.0 --port 8081 --reload
```

RGB GeoMedian:
http://localhost:8081/map?dataset=geomad&year=2020&assets=red&assets=green&assets=blue&rescale=7000,12500&rescale=7000,12500&rescale=7000,12500

Predicted LULC:
http://localhost:8081/map?dataset=prediction&year=2020&assets=lulc&colormap_name=lulc

# Deploying with Docker to Lambda.

We should look into enabling provisioned concurrency. Otherwise Lambda cold starts will make use very slow.

ECS Fargate is a good alternative.

Deploying using Docker. See `visualisation/deploy.sh` and `visualisation/Dockerfile`.
