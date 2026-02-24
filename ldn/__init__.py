from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ldn")
except PackageNotFoundError:
    __version__ = "unknown"

__version_tuple__ = tuple(int(p) for p in __version__.split(".") if p.isdigit())


def get_version() -> str:
    return __version__
