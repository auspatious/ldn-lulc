# Hyperparameter grid: model 0-2-1

Training data 0-2-1 (2020 and 2021, 2021 anchored on 2020), 207,279 samples from 40 tiles. 5-fold
`StratifiedGroupKFold` holding out whole tiles (both years together), 200 trees per fit, training tiles only. Run time
about 25 minutes. Hard F1 is macro F1 over cropland, wetland and other.

| class_weight | min_samples_leaf | Macro F1 | Macro F1 std | Hard F1 | Hard F1 std |
|---|---|---|---|---|---|
| balanced | 1 | 0.770 | 0.043 | 0.699 | 0.083 |
| balanced | 3 | 0.779 | 0.042 | 0.720 | 0.080 |
| balanced | 5 | **0.783** | 0.040 | 0.730 | 0.074 |
| balanced | 10 | 0.781 | 0.040 | **0.732** | 0.073 |
| balanced_subsample | 1 | 0.771 | 0.042 | 0.700 | 0.080 |
| balanced_subsample | 3 | 0.779 | 0.043 | 0.718 | 0.080 |
| balanced_subsample | 5 | 0.780 | 0.042 | 0.725 | 0.077 |
| balanced_subsample | 10 | 0.781 | 0.040 | 0.732 | 0.074 |

Chosen: `class_weight="balanced"`, `min_samples_leaf=10`.

## Findings
- Larger leaves help, mostly the hard classes. From leaf 1 to 10, hard F1 rises from 0.699 to 0.732 (+0.033) and macro
  F1 from 0.770 to 0.781. The rise is the same for both class weights, which supports it being real, even though
  it is smaller than the fold standard deviation. It fits noisy agreement labels: small leaves memorise them.
- The gain levels off between 5 and 10 (hard F1 +0.002, macro F1 -0.001). These are noise, so 5 and 10 are equally
  good. 10 is chosen because it is the simpler model, which is also smaller and faster to predict with.
- Leaf 1 is the worst setting for both scores. Drop it from future grids.
- `class_weight` makes no difference (at most 0.005 apart). Keep "balanced" and drop "balanced_subsample".
- The fold standard deviation is large (hard F1 0.07 to 0.08), so scores vary a lot by which tiles are held out. A
  difference between versions needs to be well over 0.01 to mean anything.
- The tile cross-validation macro F1 (0.78) is far below the 0-2-0 OOB accuracy (0.943), as expected: OOB scored
  points with trees trained on their neighbours. It isn't comparable with the 0-2-0 test F1 (0.761), which used
  different tiles and data.

## Feature importances (final 500-tree model)
- Top: ndwi_mean_3 0.036, slope 0.035, swir22_mean_3 0.034, elevation 0.034, ndwi 0.033. Water indices, swir and
  terrain lead, as in 0-2-0.
- Bottom: the std features at window 3 and 7 (mbi_std_3 0.0013, ndti_std_3 0.0014, bsi_std_3 0.0015, baei_std_3
  0.0016, bsi_std_7 0.0018). These are candidates to drop in the feature ablation (`feature-testing.md`).

## Next
- Hard F1 is still rising slightly at 10, the edge of the grid. One cheap check is `min_samples_leaf` {10, 20} with
  "balanced" only (2 combinations, about 6 minutes). Otherwise keep 10.
- Score the final model on test data 0-0-3 once it exists.
