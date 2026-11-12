Tags:
- `delete`: dead code, unused flexibility or a speculative feature. Nothing replaces it.
- `stdlib`: hand-written code that the standard library has.
- `yagni`: an abstraction, option or layer with only one use.
- `shrink`: the same logic in fewer lines.

## Findings

2. **`delete`** `filter_outliers` in `ldn/training_data.py` (and its tests), with its KMeans, silhouette, `cdist` and
   `StandardScaler` imports. It is disabled until an ablation study. Git keeps it if the study shows it helps. This is
   a decision for the team.
5. **`shrink`** `_find_existing_tasks` in `ldn/cli.py` builds a `PrefixedS3ItemPath` for each task only to get its
   STAC key. It also builds the prefix by hand, next to a "TODO: Use build_prefix()". Use `build_prefix` and compare
   item IDs. About -20 lines.
9. **`yagni`** `get_gridspec(resolution, crs)` in `ldn/grids.py` is never called with values other than the
   defaults. Its docstring also has a list of claims ("Seamless mosaicking", "Globally comparable results") to remove.
   About -12 lines.
10. **`delete`** `AwsStacTask` in `ldn/raster.py`:
    - The `post_processor` parameter is never passed.
    - `kwargs.pop("writer", AwsDsCogWriter(itempath))` makes the default writers even when callers give their own,
      and every caller gives its own. Make `writer` and `stac_creator` required parameters.
    - The `if self.stac_creator is not None` check is then always true.
    - `processor_kwargs` (send the area to the processor) is never used.

    About -10 lines.
12. **`shrink`** The block that makes the collection URL is copied in `cli_geomad.run`, `run_classify_task` and
    `create_collection`. Two copies have the comment "TODO: Could this block be a function". Make one
    `public_url(bucket, dataset)` helper. About -8 lines.
13. **`delete`** Unused functions and wrappers:
    - `http_to_s3_url` in `ldn/geomad.py` is not used. The notebook has its own copy.
    - `__version_tuple__` in `ldn/__init__.py` is not used.
    - `get_version()` only returns `__version__`.

    About -10 lines.
14. **`yagni`** `get_analysis_epsg` in `ldn/utils.py` repeats the CRS of each region, which
    `get_gridspec(region).crs` already gives. About -7 lines.
15. **`stdlib`** `_get_pc_client` in `ldn/training_data.py` caches the client in a module-level global. Use
    `@lru_cache`. `_load_dem_terrain` in `ldn/raster.py` can use the same client instead of opening a new one.
    About -5 lines.
16. **`delete`** Repeated logging and checks in `ldn/training_data.py`:
    - The "Overriding/Using GeoMAD version" message is logged in both `generate_training_data` and
      `search_and_load_geomad_indices_dem`.
    - `if not output_bucket or not geomad_bucket` cannot be true: `get_env_var` already raises, and `geomad_bucket`
      has a default.

    About -8 lines.
17. **`shrink`** `load_and_prepare` in `ldn/training_data.py` drops variables twice and casts types three times. Load
    only `[native_band, *quality_bands]` with `stac_load(bands=...)`. About -6 lines.
18. **`yagni`** The `logger` parameter of `LulcProcessor` in `ldn/lulc.py` is always the module logger. About -3
    lines.
