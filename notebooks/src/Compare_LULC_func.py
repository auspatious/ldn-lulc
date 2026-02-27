# package for comparing LULC products
import numpy as np
import xarray as xr
from matplotlib.colors import ListedColormap, BoundaryNorm
from ldn.typology import colors as class_colors, classes_flipped as standard_legend


def get_standard_legend():
    return standard_legend


def get_class_colors():
    return class_colors


def get_class_ids():
    return list(standard_legend.keys())


def get_class_labels():
    return list(standard_legend.values())


def get_standard_cmap():
    ids = get_class_ids()
    return ListedColormap([class_colors[i] for i in ids])


def get_standard_norm():
    ids = get_class_ids()
    return BoundaryNorm(ids + [max(ids) + 1], len(ids))


# project the current land cover classes to UNCCD, based on the given mapping directory
def standardise_class(DataArray, mapping):
    # Create a copy to preserve original metadata
    remapped = xr.full_like(DataArray, fill_value=np.nan)

    for original, new in mapping.items():
        remapped = remapped.where(DataArray != original, new)

    return remapped


# Given the source and target data, generate the parameters for sankey diagrams
def load_sankey_params(s_data, t_data, mask, count_limit=10):
    pairs, counts = np.unique(
        np.vstack([s_data[mask], t_data[mask]]).T, axis=0, return_counts=True
    )

    keep = counts > count_limit
    pairs = pairs[keep]
    counts = counts[keep]

    uniq_source = np.unique(pairs[:, 0])
    uniq_target = np.unique(pairs[:, 1])

    labels = [standard_legend[cls] for cls in uniq_source] + [
        standard_legend[cls] for cls in uniq_target
    ]
    node_colors = [class_colors[cls] for cls in uniq_source] + [
        class_colors[cls] for cls in uniq_target
    ]

    source_map = {cls: i for i, cls in enumerate(uniq_source)}
    target_map = {cls: i + len(uniq_source) for i, cls in enumerate(uniq_target)}
    source_idx = [source_map[s] for s in pairs[:, 0]]
    target_idx = [target_map[t] for t in pairs[:, 1]]
    link_colors = [class_colors[i] for i in pairs[:, 0]]

    return labels, node_colors, source_idx, target_idx, counts, link_colors


# Define heterogeneity function
def heterogeneity_func(window):
    window = window[window > 0]  # remove nodata (value == 0)
    if len(window) == 0:
        return np.nan  # For example, all pixels are on ocean or nodata
    return len(
        np.unique(window)
    )  # number of unique classes, 1 = pure homogeneous, bigger means more landtypes within the window
