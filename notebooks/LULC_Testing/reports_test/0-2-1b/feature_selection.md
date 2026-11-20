# Feature selection confirmation, training data 0-2-1

All the drops that were within noise alone in reports_test/0-2-1/feature_selection.md, together. Same setup and folds.

| | features | hard_f1_mean | hard_f1_vs_D | hard_f1_vs_D_std | hard_f1_folds_better | macro_f1_vs_D | macro_f1_vs_D_std | tree_cover_f1_vs_D | grassland_f1_vs_D | cropland_f1_vs_D | wetland_f1_vs_D | built_up_f1_vs_D | water_f1_vs_D | other_f1_vs_D | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| D: all | 92 | 0.732 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | reference |
| - ndti, bsi, northness, eastness, per-pixel MAD, elevation_tpi_51 | 72 | 0.731 | -0.001 | 0.006 | 2 | 0.000 | 0.004 | -0.001 | 0.000 | -0.006 | -0.001 | 0.006 | 0.000 | 0.004 | same |
