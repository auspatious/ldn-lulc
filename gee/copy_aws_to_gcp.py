"""Copy a subset of LULC COGs and STAC items from S3 to GCS, unchanged."""
# TODO: Should we use rclone for copying all data? Which is more performant?

import obstore
from obstore.store import GCSStore, S3Store

SRC_BUCKET = "dep-public-staging"
DST_BUCKET = "ldn-lulc-public-dev"
VERSION = "0-2-1"

# (owner, tile_id). Pacific tiles are under dep_ls_lulc, non-Pacific under ci_ls_lulc.
TILES = [("dep", "064_022"), ("ci", "141_128"), ("dep", "030_030")]  # Fiji, Puerto Rico, Papua New Guinea
YEARS = ["2000", "2010", "2020"]

src = S3Store(SRC_BUCKET, region="us-west-2", skip_signature=True)
dst = GCSStore(DST_BUCKET)


def copy_item(owner: str, tile_id: str, year: str) -> None:
    """Copy one tile-year's files to GCS."""
    col, row = tile_id.split("_")
    found_item = False
    for batch in obstore.list(src, f"{owner}_ls_lulc/{VERSION}/{col}/{row}/{year}/"):
        for meta in batch:
            key = meta["path"]
            obstore.put(dst, key, obstore.get(src, key).bytes())
            found_item |= key.endswith(".stac-item.json")
            print(f"Copied {key}")
    if not found_item:
        raise ValueError(f"No STAC item found for {owner} {tile_id} {year}")


def main() -> None:
    """Copy each tile-year in the subset to GCS."""
    for owner, tile_id in TILES:
        for year in YEARS:
            copy_item(owner, tile_id, year)


if __name__ == "__main__":
    main()
