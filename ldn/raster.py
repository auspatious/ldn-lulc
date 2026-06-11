from dep_tools.namers import S3ItemPath
from dep_tools.utils import join_path_or_url


# This is needed to support Source.Coop prefix.
class PrefixedS3ItemPath(S3ItemPath):
    def __init__(self, key_prefix: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.key_prefix = key_prefix.strip("/") if key_prefix else None

    def path(self, item_id, asset_name=None, ext=".tif", absolute=False) -> str:
        relative_path = super().path(item_id, asset_name=asset_name, ext=ext, absolute=False)
        if self.key_prefix:
            relative_path = f"{self.key_prefix}/{relative_path}"
        return (
            join_path_or_url(self.full_path_prefix, relative_path)
            if absolute and self.full_path_prefix is not None
            else relative_path
        )
