# Custom TiTiler with STACReader (RGB from separate band COGs)

Run the custom app in `visualisation/titiler.py` which uses `STACReader` as the
dataset reader, allowing RGB composites from separate per-band COGs via STAC items.

```bash
  cd visualisation && poetry run uvicorn app:app --host 0.0.0.0 --port 8081 --reload
```

RGB GeoMedian:
http://localhost:8081/mosaic/WebMercatorQuad/map.html?dataset=geomad&year=2020&assets=red&assets=green&assets=blue&rescale=7000,12500&rescale=7000,12500&rescale=7000,12500

Predicted LULC:
http://localhost:8081/mosaic/WebMercatorQuad/map.html?dataset=prediction&year=2020&assets=lucl&colormap_name=customTODO

# Deploying with Lambda

Follow this: https://developmentseed.org/titiler/deployment/aws/lambda/

https://github.com/developmentseed/titiler/tree/main/deployment/aws/lambda
