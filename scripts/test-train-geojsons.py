import geopandas as gpd

from ldn.training_data import TEST_TILES, TRAINING_TILES

tiles = gpd.read_file("/Users/wj/Projects/ldn-lulc/ldn-lulc/ldn/sids_all_tiles.geojson")
tiles["tile_id"] = tiles["x_index"].map("{:03d}".format) + "_" + tiles["y_index"].map("{:03d}".format)

for name, wanted in (("training", TRAINING_TILES), ("test", TEST_TILES)):
    keys = {(t, r) for t, r, _ in wanted}
    gdf = tiles[[k in keys for k in zip(tiles["tile_id"], tiles["region"])]]
    print(name, len(gdf), "of", len(keys))
    gdf.to_file(f"{name}_tiles.geojson", driver="GeoJSON")
