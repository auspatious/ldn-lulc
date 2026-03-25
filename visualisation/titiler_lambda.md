# Custom TiTiler with STACReader (RGB from separate band COGs)

Run the custom app in `visualisation/titiler.py` which uses `STACReader` as the
dataset reader, allowing RGB composites from separate per-band COGs via STAC items.

```bash
  cd /Users/wj/Projects/ldn-lulc/ldn-lulc/visualisation && poetry run uvicorn app:app --host 0.0.0.0 --port 8081 --reload
```

The mosaic.json must contain STAC item self-link URLs (not COG URLs).
Regenerate with `make_mosaic_json.ipynb` which now uses the `self` link accessor.

RGB mosaic:
http://localhost:8081/mosaic/WebMercatorQuad/map.html?url=/Users/wj/Downloads/mosaics/mosaic_2020.json&assets=red&assets=green&assets=blue&rescale=5000,12000&rescale=5000,12000&rescale=5000,12000

Single band with colormap:
http://localhost:8081/mosaic/WebMercatorQuad/map.html?url=/Users/wj/Downloads/mosaics/mosaic_2020.json&assets=red&rescale=5000,12000&colormap_name=reds

# Deploying with Lambda

Follow this: https://developmentseed.org/titiler/deployment/aws/lambda/

https://github.com/developmentseed/titiler/tree/main/deployment/aws/lambda
