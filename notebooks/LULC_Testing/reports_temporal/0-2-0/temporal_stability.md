# LULC temporal stability report: version 0-2-0

- LULC: ci_ls_lulc, dep_ls_lulc version 0-2-0
- Tiles: 050_041, 026_059, 040_049, 030_030, 033_031, 050_029, 051_017, 070_026, 064_022, 073_019, 089_016, 092_043, 112_031, 101_013, 335_092, 141_128, 128_132, 131_135, 133_128, 138_129, 149_107, 151_106, 183_123, 193_120, 264_097
- Years: 2000, 2005, 2010, 2015, 2020, 2025

LULC is every 5 years, so the tests that count changes between consecutive years (flip rate, flicker and implausible transitions) are not run.

## Years loaded

|  | years loaded | years expected | years |
| - | - | - | - |
| 050_041 | 6 | 6 | 2000-2025 |
| 026_059 | 6 | 6 | 2000-2025 |
| 040_049 | 6 | 6 | 2000-2025 |
| 030_030 | 6 | 6 | 2000-2025 |
| 033_031 | 6 | 6 | 2000-2025 |
| 050_029 | 6 | 6 | 2000-2025 |
| 051_017 | 6 | 6 | 2000-2025 |
| 070_026 | 6 | 6 | 2000-2025 |
| 064_022 | 6 | 6 | 2000-2025 |
| 073_019 | 6 | 6 | 2000-2025 |
| 089_016 | 6 | 6 | 2000-2025 |
| 092_043 | 6 | 6 | 2000-2025 |
| 112_031 | 6 | 6 | 2000-2025 |
| 101_013 | 6 | 6 | 2000-2025 |
| 335_092 | 6 | 6 | 2000-2025 |
| 141_128 | 6 | 6 | 2000-2025 |
| 128_132 | 6 | 6 | 2000-2025 |
| 131_135 | 6 | 6 | 2000-2025 |
| 133_128 | 6 | 6 | 2000-2025 |
| 138_129 | 6 | 6 | 2000-2025 |
| 149_107 | 6 | 6 | 2000-2025 |
| 151_106 | 6 | 6 | 2000-2025 |
| 183_123 | 6 | 6 | 2000-2025 |
| 193_120 | 6 | 6 | 2000-2025 |
| 264_097 | 6 | 6 | 2000-2025 |

## Tests

|  | flagged when |
| - | - |
| long_reversal_or_drift | modal class covers less than 80% of years, more than one change |

## Unstable pixels per tile (count)

A pixel can fail several tests, so the test columns do not sum to `any`. `valid` is the number of pixels tested.

| tile | long_reversal_or_drift | any | valid |
| - | - | - | - |
| 050_041 | 9644 | 9644 | 28699 |
| 026_059 | 46700 | 46700 | 299545 |
| 040_049 | 14434 | 14434 | 478622 |
| 030_030 | 306544 | 306544 | 7377890 |
| 033_031 | 1153 | 1153 | 36855 |
| 050_029 | 1459 | 1459 | 50678 |
| 051_017 | 8570 | 8570 | 578123 |
| 070_026 | 11664 | 11664 | 117396 |
| 064_022 | 168354 | 168354 | 2286368 |
| 073_019 | 12013 | 12013 | 127117 |
| 089_016 | 8025 | 8025 | 97374 |
| 092_043 | 90778 | 90778 | 571757 |
| 112_031 | 76538 | 76538 | 542921 |
| 101_013 | 7618 | 7618 | 73967 |
| 335_092 | 1704364 | 1704364 | 5532270 |
| 141_128 | 376657 | 376657 | 3189628 |
| 128_132 | 2225466 | 2225466 | 7809145 |
| 131_135 | 11483 | 11483 | 209599 |
| 133_128 | 289210 | 289210 | 2322346 |
| 138_129 | 549707 | 549707 | 2691328 |
| 149_107 | 6169 | 6169 | 10240000 |
| 151_106 | 7849 | 7849 | 1827461 |
| 183_123 | 54924 | 54924 | 641029 |
| 193_120 | 2482711 | 2482711 | 9059557 |
| 264_097 | 7314 | 7314 | 141286 |

## Unstable pixels per tile (% of valid pixels)

| tile | long_reversal_or_drift | any |
| - | - | - |
| 050_041 | 33.6 | 33.6 |
| 026_059 | 15.6 | 15.6 |
| 040_049 | 3.0 | 3.0 |
| 030_030 | 4.2 | 4.2 |
| 033_031 | 3.1 | 3.1 |
| 050_029 | 2.9 | 2.9 |
| 051_017 | 1.5 | 1.5 |
| 070_026 | 9.9 | 9.9 |
| 064_022 | 7.4 | 7.4 |
| 073_019 | 9.5 | 9.5 |
| 089_016 | 8.2 | 8.2 |
| 092_043 | 15.9 | 15.9 |
| 112_031 | 14.1 | 14.1 |
| 101_013 | 10.3 | 10.3 |
| 335_092 | 30.8 | 30.8 |
| 141_128 | 11.8 | 11.8 |
| 128_132 | 28.5 | 28.5 |
| 131_135 | 5.5 | 5.5 |
| 133_128 | 12.5 | 12.5 |
| 138_129 | 20.4 | 20.4 |
| 149_107 | 0.1 | 0.1 |
| 151_106 | 0.4 | 0.4 |
| 183_123 | 8.6 | 8.6 |
| 193_120 | 27.4 | 27.4 |
| 264_097 | 5.2 | 5.2 |

## Unstable pixel map

![Unstable pixels 050_041](unstable_pixels_050_041.png)

![Unstable pixels 026_059](unstable_pixels_026_059.png)

![Unstable pixels 040_049](unstable_pixels_040_049.png)

![Unstable pixels 030_030](unstable_pixels_030_030.png)

![Unstable pixels 033_031](unstable_pixels_033_031.png)

![Unstable pixels 050_029](unstable_pixels_050_029.png)

![Unstable pixels 051_017](unstable_pixels_051_017.png)

![Unstable pixels 070_026](unstable_pixels_070_026.png)

![Unstable pixels 064_022](unstable_pixels_064_022.png)

![Unstable pixels 073_019](unstable_pixels_073_019.png)

![Unstable pixels 089_016](unstable_pixels_089_016.png)

![Unstable pixels 092_043](unstable_pixels_092_043.png)

![Unstable pixels 112_031](unstable_pixels_112_031.png)

![Unstable pixels 101_013](unstable_pixels_101_013.png)

![Unstable pixels 335_092](unstable_pixels_335_092.png)

![Unstable pixels 141_128](unstable_pixels_141_128.png)

![Unstable pixels 128_132](unstable_pixels_128_132.png)

![Unstable pixels 131_135](unstable_pixels_131_135.png)

![Unstable pixels 133_128](unstable_pixels_133_128.png)

![Unstable pixels 138_129](unstable_pixels_138_129.png)

![Unstable pixels 149_107](unstable_pixels_149_107.png)

![Unstable pixels 151_106](unstable_pixels_151_106.png)

![Unstable pixels 183_123](unstable_pixels_183_123.png)

![Unstable pixels 193_120](unstable_pixels_193_120.png)

![Unstable pixels 264_097](unstable_pixels_264_097.png)

## Class stability

Mean share of years each pixel is in its modal class, grouped by modal class. Lower values mean pixels of that class
spend more years in other classes, from real change or misclassification.

| tile | class | pixels | mean_modal_share |
| - | - | - | - |
| 050_041 | Tree cover | 4937 | 0.605 |
| 050_041 | Grassland | 14210 | 0.842 |
| 050_041 | Cropland | 3 | 0.333 |
| 050_041 | Wetland | 27 | 0.747 |
| 050_041 | Built-up | 3713 | 0.79 |
| 050_041 | Water | 4822 | 0.983 |
| 050_041 | Other | 987 | 0.71 |
| 026_059 | Tree cover | 195094 | 0.898 |
| 026_059 | Grassland | 39506 | 0.77 |
| 026_059 | Cropland | 838 | 0.587 |
| 026_059 | Wetland | 254 | 0.856 |
| 026_059 | Built-up | 24326 | 0.859 |
| 026_059 | Water | 38799 | 0.992 |
| 026_059 | Other | 728 | 0.763 |
| 040_049 | Tree cover | 340087 | 0.988 |
| 040_049 | Grassland | 2096 | 0.793 |
| 040_049 | Cropland | 6 | 0.5 |
| 040_049 | Wetland | 67823 | 0.912 |
| 040_049 | Built-up | 2270 | 0.84 |
| 040_049 | Water | 66151 | 0.984 |
| 040_049 | Other | 189 | 0.716 |
| 030_030 | Tree cover | 6450839 | 0.981 |
| 030_030 | Grassland | 771508 | 0.893 |
| 030_030 | Cropland | 4704 | 0.618 |
| 030_030 | Wetland | 84416 | 0.939 |
| 030_030 | Built-up | 7808 | 0.609 |
| 030_030 | Water | 52386 | 0.919 |
| 030_030 | Other | 6229 | 0.694 |
| 033_031 | Tree cover | 25734 | 0.967 |
| 033_031 | Grassland | 121 | 0.738 |
| 033_031 | Wetland | 640 | 0.808 |
| 033_031 | Water | 10360 | 0.996 |
| 050_029 | Tree cover | 27418 | 0.975 |
| 050_029 | Grassland | 61 | 0.654 |
| 050_029 | Wetland | 3167 | 0.914 |
| 050_029 | Built-up | 81 | 0.733 |
| 050_029 | Water | 19935 | 0.986 |
| 050_029 | Other | 16 | 0.662 |
| 051_017 | Tree cover | 533706 | 0.993 |
| 051_017 | Grassland | 6002 | 0.766 |
| 051_017 | Cropland | 194 | 0.57 |
| 051_017 | Wetland | 74 | 0.656 |
| 051_017 | Built-up | 2518 | 0.813 |
| 051_017 | Water | 35360 | 0.996 |
| 051_017 | Other | 269 | 0.78 |
| 070_026 | Tree cover | 78209 | 0.939 |
| 070_026 | Grassland | 7576 | 0.721 |
| 070_026 | Cropland | 84 | 0.515 |
| 070_026 | Wetland | 751 | 0.765 |
| 070_026 | Built-up | 3368 | 0.843 |
| 070_026 | Water | 27397 | 0.989 |
| 070_026 | Other | 11 | 0.621 |
| 064_022 | Tree cover | 1869052 | 0.953 |
| 064_022 | Grassland | 203570 | 0.819 |
| 064_022 | Cropland | 14719 | 0.594 |
| 064_022 | Wetland | 99009 | 0.959 |
| 064_022 | Built-up | 986 | 0.74 |
| 064_022 | Water | 98953 | 0.982 |
| 064_022 | Other | 79 | 0.693 |
| 073_019 | Tree cover | 75706 | 0.926 |
| 073_019 | Grassland | 4679 | 0.672 |
| 073_019 | Cropland | 387 | 0.554 |
| 073_019 | Wetland | 4118 | 0.875 |
| 073_019 | Built-up | 4095 | 0.824 |
| 073_019 | Water | 38111 | 0.988 |
| 073_019 | Other | 21 | 0.673 |
| 089_016 | Tree cover | 77704 | 0.958 |
| 089_016 | Grassland | 3170 | 0.693 |
| 089_016 | Cropland | 524 | 0.58 |
| 089_016 | Wetland | 262 | 0.628 |
| 089_016 | Built-up | 3853 | 0.825 |
| 089_016 | Water | 11757 | 0.989 |
| 089_016 | Other | 104 | 0.758 |
| 092_043 | Tree cover | 320 | 0.52 |
| 092_043 | Grassland | 181161 | 0.825 |
| 092_043 | Cropland | 16 | 0.357 |
| 092_043 | Wetland | 447 | 0.564 |
| 092_043 | Built-up | 64055 | 0.74 |
| 092_043 | Water | 181629 | 0.98 |
| 092_043 | Other | 144129 | 0.824 |
| 112_031 | Tree cover | 297468 | 0.921 |
| 112_031 | Grassland | 155262 | 0.823 |
| 112_031 | Cropland | 74 | 0.523 |
| 112_031 | Wetland | 165 | 0.738 |
| 112_031 | Built-up | 9842 | 0.743 |
| 112_031 | Water | 72587 | 0.988 |
| 112_031 | Other | 7523 | 0.802 |
| 101_013 | Tree cover | 54519 | 0.922 |
| 101_013 | Grassland | 3542 | 0.719 |
| 101_013 | Cropland | 53 | 0.544 |
| 101_013 | Wetland | 192 | 0.658 |
| 101_013 | Built-up | 857 | 0.79 |
| 101_013 | Water | 14786 | 0.994 |
| 101_013 | Other | 18 | 0.593 |
| 335_092 | Tree cover | 2991352 | 0.816 |
| 335_092 | Grassland | 2211647 | 0.849 |
| 335_092 | Cropland | 130543 | 0.709 |
| 335_092 | Wetland | 12086 | 0.839 |
| 335_092 | Built-up | 114226 | 0.729 |
| 335_092 | Water | 50733 | 0.971 |
| 335_092 | Other | 21683 | 0.738 |
| 141_128 | Tree cover | 2173372 | 0.933 |
| 141_128 | Grassland | 338524 | 0.804 |
| 141_128 | Cropland | 2329 | 0.546 |
| 141_128 | Wetland | 21634 | 0.802 |
| 141_128 | Built-up | 577142 | 0.924 |
| 141_128 | Water | 71626 | 0.965 |
| 141_128 | Other | 5001 | 0.756 |
| 128_132 | Tree cover | 2939408 | 0.794 |
| 128_132 | Grassland | 3792819 | 0.808 |
| 128_132 | Cropland | 603064 | 0.729 |
| 128_132 | Wetland | 210760 | 0.904 |
| 128_132 | Built-up | 76898 | 0.856 |
| 128_132 | Water | 178035 | 0.911 |
| 128_132 | Other | 8161 | 0.744 |
| 131_135 | Tree cover | 47978 | 0.896 |
| 131_135 | Grassland | 8344 | 0.791 |
| 131_135 | Cropland | 9 | 0.404 |
| 131_135 | Wetland | 5759 | 0.853 |
| 131_135 | Built-up | 11638 | 0.851 |
| 131_135 | Water | 132561 | 0.991 |
| 131_135 | Other | 3310 | 0.853 |
| 133_128 | Tree cover | 1564873 | 0.899 |
| 133_128 | Grassland | 672350 | 0.903 |
| 133_128 | Cropland | 2191 | 0.665 |
| 133_128 | Wetland | 3616 | 0.756 |
| 133_128 | Built-up | 36414 | 0.843 |
| 133_128 | Water | 42147 | 0.988 |
| 133_128 | Other | 755 | 0.717 |
| 138_129 | Tree cover | 1604200 | 0.885 |
| 138_129 | Grassland | 644762 | 0.82 |
| 138_129 | Cropland | 253961 | 0.795 |
| 138_129 | Wetland | 40461 | 0.84 |
| 138_129 | Built-up | 52007 | 0.838 |
| 138_129 | Water | 95321 | 0.983 |
| 138_129 | Other | 616 | 0.756 |
| 149_107 | Tree cover | 10197553 | 0.999 |
| 149_107 | Grassland | 1743 | 0.86 |
| 149_107 | Wetland | 2 | 0.417 |
| 149_107 | Built-up | 9 | 0.57 |
| 149_107 | Water | 40642 | 0.97 |
| 149_107 | Other | 51 | 0.716 |
| 151_106 | Tree cover | 1663687 | 0.998 |
| 151_106 | Grassland | 163773 | 0.972 |
| 151_106 | Built-up | 1 | 0.667 |
| 183_123 | Tree cover | 3642 | 0.526 |
| 183_123 | Grassland | 441553 | 0.932 |
| 183_123 | Cropland | 67 | 0.529 |
| 183_123 | Wetland | 3 | 0.633 |
| 183_123 | Built-up | 10477 | 0.72 |
| 183_123 | Water | 62125 | 0.909 |
| 183_123 | Other | 123162 | 0.879 |
| 193_120 | Tree cover | 416355 | 0.592 |
| 193_120 | Grassland | 8334875 | 0.848 |
| 193_120 | Cropland | 261162 | 0.695 |
| 193_120 | Wetland | 25465 | 0.781 |
| 193_120 | Built-up | 14146 | 0.734 |
| 193_120 | Water | 6366 | 0.915 |
| 193_120 | Other | 1188 | 0.789 |
| 264_097 | Tree cover | 104804 | 0.964 |
| 264_097 | Grassland | 2378 | 0.725 |
| 264_097 | Wetland | 554 | 0.801 |
| 264_097 | Built-up | 8695 | 0.841 |
| 264_097 | Water | 24731 | 0.988 |
| 264_097 | Other | 124 | 0.758 |

## Distinct classes per pixel

![Distinct classes 050_041](distinct_classes_050_041.png)

![Distinct classes 026_059](distinct_classes_026_059.png)

![Distinct classes 040_049](distinct_classes_040_049.png)

![Distinct classes 030_030](distinct_classes_030_030.png)

![Distinct classes 033_031](distinct_classes_033_031.png)

![Distinct classes 050_029](distinct_classes_050_029.png)

![Distinct classes 051_017](distinct_classes_051_017.png)

![Distinct classes 070_026](distinct_classes_070_026.png)

![Distinct classes 064_022](distinct_classes_064_022.png)

![Distinct classes 073_019](distinct_classes_073_019.png)

![Distinct classes 089_016](distinct_classes_089_016.png)

![Distinct classes 092_043](distinct_classes_092_043.png)

![Distinct classes 112_031](distinct_classes_112_031.png)

![Distinct classes 101_013](distinct_classes_101_013.png)

![Distinct classes 335_092](distinct_classes_335_092.png)

![Distinct classes 141_128](distinct_classes_141_128.png)

![Distinct classes 128_132](distinct_classes_128_132.png)

![Distinct classes 131_135](distinct_classes_131_135.png)

![Distinct classes 133_128](distinct_classes_133_128.png)

![Distinct classes 138_129](distinct_classes_138_129.png)

![Distinct classes 149_107](distinct_classes_149_107.png)

![Distinct classes 151_106](distinct_classes_151_106.png)

![Distinct classes 183_123](distinct_classes_183_123.png)

![Distinct classes 193_120](distinct_classes_193_120.png)

![Distinct classes 264_097](distinct_classes_264_097.png)

## Class area time series

Year-to-year jumps that coincide with Landsat sensor changes or low GeoMAD observation counts are artefacts, not
land change.

![Class area 050_041](class_area_050_041.png)

![Class area 026_059](class_area_026_059.png)

![Class area 040_049](class_area_040_049.png)

![Class area 030_030](class_area_030_030.png)

![Class area 033_031](class_area_033_031.png)

![Class area 050_029](class_area_050_029.png)

![Class area 051_017](class_area_051_017.png)

![Class area 070_026](class_area_070_026.png)

![Class area 064_022](class_area_064_022.png)

![Class area 073_019](class_area_073_019.png)

![Class area 089_016](class_area_089_016.png)

![Class area 092_043](class_area_092_043.png)

![Class area 112_031](class_area_112_031.png)

![Class area 101_013](class_area_101_013.png)

![Class area 335_092](class_area_335_092.png)

![Class area 141_128](class_area_141_128.png)

![Class area 128_132](class_area_128_132.png)

![Class area 131_135](class_area_131_135.png)

![Class area 133_128](class_area_133_128.png)

![Class area 138_129](class_area_138_129.png)

![Class area 149_107](class_area_149_107.png)

![Class area 151_106](class_area_151_106.png)

![Class area 183_123](class_area_183_123.png)

![Class area 193_120](class_area_193_120.png)

![Class area 264_097](class_area_264_097.png)

## LULC and GeoMAD inputs by year

LULC with the GeoMAD 0-2-1 true colour, NDVI, BSI and EMAD it was classified from. Each input row uses one
colour scale for all years.

![LULC and GeoMAD inputs by year 050_041](lulc_inputs_years_050_041.png)

![LULC and GeoMAD inputs by year 026_059](lulc_inputs_years_026_059.png)

![LULC and GeoMAD inputs by year 040_049](lulc_inputs_years_040_049.png)

![LULC and GeoMAD inputs by year 030_030](lulc_inputs_years_030_030.png)

![LULC and GeoMAD inputs by year 033_031](lulc_inputs_years_033_031.png)

![LULC and GeoMAD inputs by year 050_029](lulc_inputs_years_050_029.png)

![LULC and GeoMAD inputs by year 051_017](lulc_inputs_years_051_017.png)

![LULC and GeoMAD inputs by year 070_026](lulc_inputs_years_070_026.png)

![LULC and GeoMAD inputs by year 064_022](lulc_inputs_years_064_022.png)

![LULC and GeoMAD inputs by year 073_019](lulc_inputs_years_073_019.png)

![LULC and GeoMAD inputs by year 089_016](lulc_inputs_years_089_016.png)

![LULC and GeoMAD inputs by year 092_043](lulc_inputs_years_092_043.png)

![LULC and GeoMAD inputs by year 112_031](lulc_inputs_years_112_031.png)

![LULC and GeoMAD inputs by year 101_013](lulc_inputs_years_101_013.png)

![LULC and GeoMAD inputs by year 335_092](lulc_inputs_years_335_092.png)

![LULC and GeoMAD inputs by year 141_128](lulc_inputs_years_141_128.png)

![LULC and GeoMAD inputs by year 128_132](lulc_inputs_years_128_132.png)

![LULC and GeoMAD inputs by year 131_135](lulc_inputs_years_131_135.png)

![LULC and GeoMAD inputs by year 133_128](lulc_inputs_years_133_128.png)

![LULC and GeoMAD inputs by year 138_129](lulc_inputs_years_138_129.png)

![LULC and GeoMAD inputs by year 149_107](lulc_inputs_years_149_107.png)

![LULC and GeoMAD inputs by year 151_106](lulc_inputs_years_151_106.png)

![LULC and GeoMAD inputs by year 183_123](lulc_inputs_years_183_123.png)

![LULC and GeoMAD inputs by year 193_120](lulc_inputs_years_193_120.png)

![LULC and GeoMAD inputs by year 264_097](lulc_inputs_years_264_097.png)

Median of each input per year for the pixels of each 2020 class. Steps that coincide with sensor changes (shaded
eras) point to input artefacts rather than land change.

![GeoMAD medians 050_041](geomad_medians_050_041.png)

![GeoMAD medians 026_059](geomad_medians_026_059.png)

![GeoMAD medians 040_049](geomad_medians_040_049.png)

![GeoMAD medians 030_030](geomad_medians_030_030.png)

![GeoMAD medians 033_031](geomad_medians_033_031.png)

![GeoMAD medians 050_029](geomad_medians_050_029.png)

![GeoMAD medians 051_017](geomad_medians_051_017.png)

![GeoMAD medians 070_026](geomad_medians_070_026.png)

![GeoMAD medians 064_022](geomad_medians_064_022.png)

![GeoMAD medians 073_019](geomad_medians_073_019.png)

![GeoMAD medians 089_016](geomad_medians_089_016.png)

![GeoMAD medians 092_043](geomad_medians_092_043.png)

![GeoMAD medians 112_031](geomad_medians_112_031.png)

![GeoMAD medians 101_013](geomad_medians_101_013.png)

![GeoMAD medians 335_092](geomad_medians_335_092.png)

![GeoMAD medians 141_128](geomad_medians_141_128.png)

![GeoMAD medians 128_132](geomad_medians_128_132.png)

![GeoMAD medians 131_135](geomad_medians_131_135.png)

![GeoMAD medians 133_128](geomad_medians_133_128.png)

![GeoMAD medians 138_129](geomad_medians_138_129.png)

![GeoMAD medians 149_107](geomad_medians_149_107.png)

![GeoMAD medians 151_106](geomad_medians_151_106.png)

![GeoMAD medians 183_123](geomad_medians_183_123.png)

![GeoMAD medians 193_120](geomad_medians_193_120.png)

![GeoMAD medians 264_097](geomad_medians_264_097.png)

## Caveats

- Nodata years are skipped, so year-to-year tests compare each valid year with the previous valid year.
Pixels with no valid years are not tested.
- Tiles are cropped and masked to their country buffered by 200 m.
- Class areas are from LULC reprojected to equal area EPSG:6933 at 30 m with nearest neighbour resampling.
