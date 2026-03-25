# Local testing with Docker

Run TiTiler Docker image and make mosaic JSONs created in make_mosaic_json.ipynb available:
```bash
docker run --platform=linux/amd64 -p 8080:80 \
    -v /Users/wj/Downloads/mosaics:/data \
    --rm -it ghcr.io/developmentseed/titiler:latest
```

Mosaic (slow because data is 10m):
http://localhost:8080/mosaicjson/WebMercatorQuad/map.html?url=file:///data/mosaic_2020.json&rescale=0,10000&colormap_name=reds

Can support multi band visualisation if a COG has all bands e.g. RGB: `bidx=1&bidx=2&bidx=3`. We could write RGB COGs from the GeoMAD process.

http://localhost:8080/mosaicjson/WebMercatorQuad/map.html?url=file%3A%2F%2F%2Fdata%2Fmosaic_test.json?colormap_name=reds
bidx=1&rescale=0,10000&

Single COG:
http://localhost:8080/cog/WebMercatorQuad/map.html?url=https://data.ldn.auspatious.com/ausp_ls_geomad/0-0-2/058/043/2000/ausp_ls_geomad_058_043_2000_red.tif&bidx=1&rescale=0,10000&colormap_name=reds

# Deploying with Lambda

Follow this: https://developmentseed.org/titiler/deployment/aws/lambda/

https://github.com/developmentseed/titiler/tree/main/deployment/aws/lambda

### Questions

Do we need to make this available for GeoMAD results too? I am not sure how to do multiband.