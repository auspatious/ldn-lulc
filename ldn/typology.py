import re
from importlib.resources import files

import yaml
from pystac.extensions.classification import Classification

_typology_path = files("ldn").joinpath("typology_mapping.yaml")
with _typology_path.open("r", encoding="utf-8") as f:
    typology_mapping = yaml.safe_load(f)
    lvl1 = typology_mapping["level1_classes"]
    classes = {v["label"]: v["value"] for v in lvl1.values()}
    classes_flipped = {v["value"]: v["label"] for v in lvl1.values()}
    colors = {v["value"]: v["color"] for v in lvl1.values()}

    world_cover_map = typology_mapping["world_cover_map"]
    cci_lc_map = typology_mapping["cci_lc_map"]
    io_map = typology_mapping["io_map"]


def stac_classes(nodata_value: int) -> list[Classification]:
    """Level 1 classes as STAC classification extension classes.

    Args:
        nodata_value: Value used for nodata in the output, replacing the no_data class value.

    Returns:
        One Classification per level 1 class, with colors as hex color hints.
    """
    return [
        Classification.create(
            value=nodata_value if name == "no_data" else c["value"],
            name=name,
            title=c["label"],
            color_hint="".join(f"{int(n):02X}" for n in re.findall(r"\d+", c["color"])),
            nodata=name == "no_data",
        )
        for name, c in lvl1.items()
    ]
