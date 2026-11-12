import pytest
from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES

from ldn.cli_grid import list_countries
from ldn.utils import ALL_COUNTRIES, NON_DEP_COUNTRIES


@pytest.mark.parametrize(
    "grids,expected_source",
    [
        ("all", ALL_COUNTRIES),
        ("non-pacific", NON_DEP_COUNTRIES),
        ("pacific", DEP_COUNTRIES_AND_CODES),
    ],
)
def test_list_countries_returns_sorted_source_dict(grids, expected_source):
    assert list_countries(grids) == dict(sorted(expected_source.items()))


def test_list_countries_default_is_all():
    assert list_countries() == dict(sorted(ALL_COUNTRIES.items()))
