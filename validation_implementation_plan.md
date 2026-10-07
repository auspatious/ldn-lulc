# Stable held-out test set

## Context
Training points come from `PACIFIC_TRAINING_TILES` (`ldn/training_data.py`), sampled with no seed, so they change per run. We need a fixed test set, never used for training, to compare data and model versions.

This test set is step 3 of the validation workflow (programmatic validation) and the fixed yardstick for the iteration loop in steps 2-4:
1. Re-generate training data, representative for all regions.
2. Run LULC for all years over a small region.
3. Validate, programmatically if possible (this plan).
4. Iterate 2-4.
5. If results are good, run representative regions over all years and visualise.
6. If the visualisation is good, run everything; otherwise go back and iterate.
7. Formal validation report.

Human/AI validation is a separate later dataset, for the step 7 report.

## Approach: hold out whole tiles
Tile-level holdout avoids spatial leakage from neighbouring pixels.
- Unit: point (same as training).
- Strata: LULC class within each tile, using the existing agreement map. Min per class via `min_sample_per_class_n`, which oversamples rare classes (wetland, cropland).
- Scope: a few tiles per region (pacific and non-pacific, per step 1) not in the training lists, together covering all classes (the existing `MODEL_TEST_TILES` TODO).
- Accuracy: score a model on the test points at the 2020 labels (the year the LULC products overlap).
- Consistency over time (steps 2 and 5): reuse the same test tiles and classify all years. Report how many pixels change class between years and flag implausible swings, since labels exist only for 2020.

## Changes
1. `ldn/training_data.py`: define `PACIFIC_TEST_TILES` and a non-pacific equivalent (replacing the `MODEL_TEST_TILES` TODO). Use 2-4 tiles per region, disjoint from the training tiles, covering all classes. Add a test that the lists do not overlap.
2. `ldn/random_sampling.py`: add a `seed` param (default None) and pass `random_state=seed` to the 5 `.sample(...)` calls. This makes the points reproducible.
3. `ldn/training_data.py`: add a `split: Literal["train", "test"]` option to `generate_training_data` and `make_training_data`. For test, use a fixed seed and write to `test_data/{TEST_DATA_VERSION}/...` (own version constant in `ldn/utils.py`, independent of `TRAINING_DATA_VERSION`). Refuse to overwrite for test.
4. `Makefile`: add a `test-data-generate` target looping over the test tiles.
5. Scoring (the programmatic validation): a small function that loads `test_data/` and returns overall accuracy, per-class precision/recall and the confusion matrix for a model version. Put it in the `1_Train_Model.ipynb` eval cell first, and move it to a module only if the pipeline reuses it.
6. `README.md`: document the train/test/validation split and where it fits in the workflow.

## Verification
- `ldn/tests/test_training_data.py`: the tile lists are disjoint; same seed gives identical samples; the test split writes to the `test_data/` prefix.
- Generate one test tile twice and confirm the CSVs are identical.
- `uv run pytest`.

## Open items
- Which test tiles to choose (needs domain input on class coverage).
- Whether the temporal-consistency check is in scope now or a follow-up.
