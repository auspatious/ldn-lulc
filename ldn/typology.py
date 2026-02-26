from importlib.resources import files
import yaml


_typology_path = files("ldn").joinpath("typology_mapping.yaml")
with _typology_path.open("r", encoding="utf-8") as f:
    typology_mapping = yaml.safe_load(f)
    lvl1 = typology_mapping["level1_classes"]
    classes = {v["label"]: v["value"] for v in lvl1.values()}
    classes_flipped = {v["value"]: v["label"] for v in lvl1.values()}
    colors = {v["value"]: v["color"] for v in lvl1.values()}
    
    world_cover_map = typology_mapping['world_cover_map']
    cci_lc_map = typology_mapping['cci_lc_map']
    io_map = typology_mapping['io_map']
