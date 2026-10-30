# Plan: training data for more years

## Why
On Nauru 050_041 most LULC change over 2000-2025 is artificial. It follows dry La Nina years (2000-2001, 2007-2011,
2021-2022), when vegetation browns: NDVI drops and visible reflectance rises, so tree cover and grassland are called
built-up or other. Training labels are 2020 only, an ENSO-neutral to moderate year, so the model has never seen dry
years. There are no jumps at sensor changes, and the MAD bands don't track the LULC changes. Real change exists too:
cleared patches growing from 2017.

## Phase 1: real labels for 2017-2021
Generate training data for each year 2017-2021 from the products available that year, where at least 2 agree:

| Year | Products | Agreement |
|---|---|---|
| 2017-2019 | ESA CCI, IO | both |
| 2020 | WorldCover, ESA CCI, IO | 2 of 3 (as now) |
| 2021 | WorldCover, IO | both |

These are real labels, not propagated, so real change (e.g. the clearing since 2017) is labelled correctly each year.
2021 is a La Nina year, and 2017-2018 had a weak one, so this adds dry conditions. All years are Landsat 7 and 8.

Changes:
- `make_training_data` loads only the products that have data for the year (a `LULC_PRODUCTS` year range per product)
  instead of asserting all 3 exist.
- `find_agreement` takes any 2 or 3 products: a pixel is labelled where at least 2 products have data and at least 2
  agree, with the same 3x3 neighbourhood filter.
- `generate-training-data --year` already exists, and outputs are already per year
  (`.../{tile}/{year}/samples.csv`).
- Makefile: the `generate_samples` macro takes a year, and a new `training-data-generate-years` target loops over
  2017-2021 for the training tiles. Test data stays 2020 only, so models stay comparable.
- `notebooks/1_Train_Model.ipynb` reads every year's CSV and adds a `year` column, which must not become a feature.
  Classes stay balanced through the existing `class_weight="balanced"`, which gives each class equal total weight
  however many years repeat it.
- Versions: part of the pending 0-2-0 retrain (training data, model and LULC 0-2-0 aren't generated yet), so no bump.

## Phase 2 (only if Phase 1 isn't enough): propagate to older La Nina years
Phase 1 has no Landsat 5 or Landsat 7 SLC-off years and no strong La Nina. If the 2000-2011 La Nina years still flip,
copy labels to 2000, 2008 and 2011 (one per La Nina episode, also covering those sensors) for points that are:
- agreed with the same class in all 5 years 2017-2021, and
- the same class in ESA CCI in at least 90% of 2000-2016.

No spectral agreement test: in dry years the spectra are meant to differ, and that difference is what the model has to
learn, so a spectral test would reject exactly the points needed. Only published products decide stability (fixed
versions, so reproducible), not our predictions. Cap copied rows per class at that class's 2020 count.

## Evaluate (2020-only model vs Phase 1, then Phase 2)
- 2020 test accuracy, overall and per class. Rare classes must not drop.
- Temporal stability report on Nauru and other test tiles, 2000-2025: flip rate, flicker and the share of built-up and
  other in La Nina years. It should flatten without hiding the post-2017 clearing.
