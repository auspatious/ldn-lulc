"""Print the test tiles that have no test data in S3, for TEST_DATA_VERSION and 2020. Uses AWS_PROFILE."""

import boto3

from ldn.training_data import TEST_TILES
from ldn.utils import TEST_DATA_VERSION

prefix = f"dep_ls_lulc/test_data/{TEST_DATA_VERSION}/"
resp = boto3.client("s3").list_objects_v2(Bucket="dep-public-staging", Prefix=prefix)
keys = {obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith("samples.csv")}

missing = []
for tile_id, region, country in TEST_TILES:
    x, y = tile_id.split("_")
    if f"{prefix}{region}/{x}/{y}/2020/samples.csv" not in keys:
        missing.append(f"{tile_id} {region} {next(iter(country))}")

print(f"Test data {TEST_DATA_VERSION}: {len(TEST_TILES) - len(missing)} of {len(TEST_TILES)} tiles")
print("\n".join(f"Missing: {m}" for m in missing))
