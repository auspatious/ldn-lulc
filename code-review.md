# Code review: unnecessary complexity in `ldn/`

A review of the `ldn` package for code that can be removed or simplified. Each finding was checked for uses in the
package, the notebooks, the Makefile and the Argo templates. Findings are ranked by the number of lines they cut.

Tags:
- `delete`: dead code, unused flexibility or a speculative feature. Nothing replaces it.
- `stdlib`: hand-written code that the standard library has.
- `yagni`: an abstraction, option or layer with only one use.
- `shrink`: the same logic in fewer lines.

## Findings

1. **`delete`** `ldn/random_sampling.py`: only `stratified_random` (training data) and `equal_stratified_random` (the
   CEO plots notebook) are used.
   - Remove the `random` and `manual` modes and the commented-out try/except blocks.
   - Remove the `out_fname`, `class_attr` and `drop_value` parameters, which are never set.
   - Change the string-concatenated log lines to f-strings.

   About 240 lines become about 100.
2. **`delete`** `filter_outliers` in `ldn/training_data.py` (and its tests), with its KMeans, silhouette, `cdist` and
   `StandardScaler` imports. It is disabled until an ablation study. Git keeps it if the study shows it helps. This is
   a decision for the team.
3. **`yagni`** `run_classify_task` in `ldn/lulc.py` repeats `cli_lulc.run` argument for argument: 22 parameters passed
   straight through, and the docstring is written twice. Its signature still has `typer.Option` annotations. Make the
   CLI command the function itself, in `lulc.py`. About -60 lines.
4. **`shrink`** `do_prediction` in `ldn/lulc.py` stacks the data, puts the prediction into a `Series`, reshapes it,
   takes the argmax and then applies the threshold again from the reshaped probabilities. Instead:
   - Reshape `predict_proba` once to (class, y, x), with NaN for invalid pixels.
   - Set `classification = classes[argmax]` where the max probability is at least the threshold and the pixel is
     valid.

   `reshape_array_to_2d` is then not needed. About -40 lines.
5. **`shrink`** `_find_existing_tasks` in `ldn/cli.py` builds a `PrefixedS3ItemPath` for each task only to get its
   STAC key. It also builds the prefix by hand, next to a "TODO: Use build_prefix()". Use `build_prefix` and compare
   item IDs. About -20 lines.
6. **`stdlib`** `_load_stac_docs_async` in `ldn/cli.py` uses asyncio, a semaphore and a thread pool to run blocking
   boto3 calls. `ThreadPoolExecutor(64).map(fetch, keys)` does the same. About -12 lines.
7. **`shrink`** Repeated validation of `Literal` values that typer already checks:
   - `list_countries` in `ldn/cli_grid.py` checks them again and has three logging branches. It can be
     `return dict(sorted(MAP[grids].items()))`.
   - `get_grid_tiles` in `ldn/grids.py` checks its `Literal` parameters again too.

   About -20 lines.
8. **`yagni`** `GeoMADProcessor` in `ldn/geomad.py`:
   - `preprocessor` is stored but never used.
   - `send_area_to_processor` and `load_data_before_writing` are never set.
   - The mutable default dicts for `geomad_options` and `mask_clouds_kwargs` repeat the values that `cli_geomad`
     always passes in.

   Make them required parameters with no defaults. About -15 lines.
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
11. **`yagni`** Options that are never changed:
    - The `--all-bands` GeoMAD option is never turned off in the Makefile, the templates or the code. The band list
      it changes to is written out inline.
    - `mask_snow=False` in `mask_qa_pixel` is never used.
    - The missing-band check in `mask_blue_white_cloud` cannot fail: red, green and blue are always loaded.

    About -20 lines.
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

**Net:** about -460 lines. No dependencies are removed: scikit-learn and scipy are still necessary. `requests` (used
only in `_load_joblib_model`) can change to `urllib.request` if nothing else needs it.
