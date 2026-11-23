"""Register LULC COGs already copied to GCS (by copy_aws_to_gcp.py) as COG-backed GEE assets."""

import json

import ee
import obstore
from copy_aws_to_gcp import DST_BUCKET, VERSION, dst

GEE_PROJECT = ""  # TODO: Fill in your GEE project ID here.
OWNERS = ["dep", "ci"]


def register_item(key: str, collection_id: str) -> None:
    """Register the STAC item at a GCS key as one multi-band COG-backed image in a GEE collection."""
    item = json.loads(bytes(obstore.get(dst, key).bytes()))
    _, _, col, row, year, _ = key.split("/")
    # STAC hrefs still point at S3, so build GCS URIs from the item's GCS folder and each asset's filename.
    folder = key.rsplit("/", 1)[0]
    tilesets = [
        {"id": name, "sources": [{"uris": [f"gs://{DST_BUCKET}/{folder}/{asset['href'].rsplit('/', 1)[1]}"]}]}
        for name, asset in item["assets"].items()
    ]
    manifest = {
        "name": f"{collection_id}/{item['id']}",
        "tilesets": tilesets,
        "bands": [{"id": t["id"], "tileset_id": t["id"], "tileset_band_index": 0} for t in tilesets],
        "start_time": item["properties"]["datetime"],
        "properties": {
            "tile_id": f"{col}_{row}",
            "year": int(year),
            "version": VERSION,
            "stac_item": f"gs://{DST_BUCKET}/{key}",
        },
    }
    ee.data.startExternalImageIngestion(manifest, True)
    print(f"Registered {manifest['name']}")


def main() -> None:
    """Register every STAC item for this version in GCS into one GEE ImageCollection per owner (region)."""
    ee.Initialize(project=GEE_PROJECT)
    for owner in OWNERS:
        collection_id = f"projects/{GEE_PROJECT}/assets/{owner}_ls_lulc_{VERSION}"
        try:
            ee.data.getAsset(collection_id)
        except ee.EEException:
            ee.data.createAsset({"type": "IMAGE_COLLECTION"}, collection_id)
        for batch in obstore.list(dst, f"{owner}_ls_lulc/{VERSION}/"):
            for meta in batch:
                if meta["path"].endswith(".stac-item.json"):
                    register_item(meta["path"], collection_id)


if __name__ == "__main__":
    main()
