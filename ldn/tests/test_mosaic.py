from unittest.mock import MagicMock, patch

import pytest
from cogeo_mosaic.mosaic import MosaicJSON
from typer.testing import CliRunner

from ldn.cli import _stac_self_link, _build_mosaic_for_year, app


runner = CliRunner()


# _stac_self_link


def test_stac_self_link_returns_self_href():
    feature = {
        "id": "item-123",
        "links": [
            {"rel": "root", "href": "https://example.com/root"},
            {"rel": "self", "href": "https://example.com/items/item-123"},
        ],
    }
    assert _stac_self_link(feature) == "https://example.com/items/item-123"


# _build_mosaic_for_year


def _make_stac_item(item_id: str, bbox: list[float]) -> MagicMock:
    """Helper to create a mock STAC item with a Polygon geometry from a bbox."""
    minx, miny, maxx, maxy = bbox
    item = MagicMock()
    item.to_dict.return_value = {
        "id": item_id,
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [minx, miny],
                    [maxx, miny],
                    [maxx, maxy],
                    [minx, maxy],
                    [minx, miny],
                ]
            ],
        },
        "links": [{"rel": "self", "href": f"https://example.com/items/{item_id}"}],
        "properties": {"datetime": "2020-06-01T00:00:00Z"},
        "assets": {},
    }
    return item


@patch("ldn.cli.search_sync")
@patch("ldn.cli.ItemCollection")
def test_build_mosaic_for_year_returns_mosaic(mock_item_collection, mock_search):
    items = [
        _make_stac_item("item-1", [103.6, 1.2, 104.0, 1.5]),
        _make_stac_item("item-2", [104.0, 1.2, 104.4, 1.5]),
        _make_stac_item("item-3", [103.6, 1.5, 104.0, 1.8]),
    ]
    mock_search.return_value = ["raw-item-1", "raw-item-2", "raw-item-3"]
    mock_item_collection.return_value = items

    mosaic = _build_mosaic_for_year("2020", "https://example.com/stac.parquet")

    mock_search.assert_called_once_with(
        "https://example.com/stac.parquet", datetime="2020"
    )
    assert isinstance(mosaic, MosaicJSON)
    assert mosaic.minzoom == 5
    assert mosaic.maxzoom == 14


@patch("ldn.cli.search_sync")
@patch("ldn.cli.ItemCollection")
def test_build_mosaic_for_year_raises_on_empty(mock_item_collection, mock_search):
    mock_search.return_value = []
    mock_item_collection.return_value = []

    with pytest.raises(ValueError, match="No STAC items found for year 2020"):
        _build_mosaic_for_year("2020", "https://example.com/stac.parquet")


@patch("ldn.cli.search_sync")
@patch("ldn.cli.ItemCollection")
def test_build_mosaic_for_year_converts_multipolygon_to_convex_hull(
    mock_item_collection, mock_search
):
    """Items with MultiPolygon geometries should be converted to convex hull."""
    item = MagicMock()
    item.to_dict.return_value = {
        "id": "multi-item",
        "type": "Feature",
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]],
            ],
        },
        "links": [
            {"rel": "self", "href": "https://example.com/items/multi-item"}
        ],
        "properties": {"datetime": "2020-06-01T00:00:00Z"},
        "assets": {},
    }
    mock_search.return_value = ["raw"]
    mock_item_collection.return_value = [item]

    mosaic = _build_mosaic_for_year("2020", "https://example.com/stac.parquet")

    assert isinstance(mosaic, MosaicJSON)


# make_mosaics CLI command


@patch("ldn.cli.MosaicBackend")
@patch("ldn.cli._build_mosaic_for_year")
def test_make_mosaics_geomad_single_year(mock_build, mock_backend):
    mock_mosaic = MagicMock(spec=MosaicJSON)
    mock_build.return_value = mock_mosaic

    # MosaicBackend is used as a context manager
    mock_backend_instance = MagicMock()
    mock_backend.return_value.__enter__ = MagicMock(return_value=mock_backend_instance)
    mock_backend.return_value.__exit__ = MagicMock(return_value=False)

    result = runner.invoke(app, ["make-mosaics", "--years", "2020", "--dataset", "geomad"])

    assert result.exit_code == 0, result.output
    mock_build.assert_called_once_with(
        "2020",
        "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_geomad/0-0-2/ausp_ls_geomad.parquet",
    )
    mock_backend.assert_called_once()
    # Check the output path contains the expected pattern
    out_path = mock_backend.call_args[0][0]
    assert "geomad_2020_mosaic.json" in out_path


@patch("ldn.cli.MosaicBackend")
@patch("ldn.cli._build_mosaic_for_year")
def test_make_mosaics_prediction_single_year(mock_build, mock_backend):
    mock_build.return_value = MagicMock(spec=MosaicJSON)
    mock_backend.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_backend.return_value.__exit__ = MagicMock(return_value=False)

    result = runner.invoke(app, ["make-mosaics", "--years", "2020", "--dataset", "prediction"])

    assert result.exit_code == 0, result.output
    mock_build.assert_called_once()
    out_path = mock_backend.call_args[0][0]
    assert "prediction_2020_mosaic.json" in out_path


@patch("ldn.cli.MosaicBackend")
@patch("ldn.cli._build_mosaic_for_year")
def test_make_mosaics_all_builds_both_datasets(mock_build, mock_backend):
    mock_build.return_value = MagicMock(spec=MosaicJSON)
    mock_backend.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_backend.return_value.__exit__ = MagicMock(return_value=False)

    result = runner.invoke(app, ["make-mosaics", "--years", "2020", "--dataset", "all"])

    assert result.exit_code == 0, result.output
    assert mock_build.call_count == 2

    called_urls = [c.args[1] for c in mock_build.call_args_list]
    assert any("prediction" in url for url in called_urls)
    assert any("geomad" in url for url in called_urls)


@patch("ldn.cli.MosaicBackend")
@patch("ldn.cli._build_mosaic_for_year")
def test_make_mosaics_multiple_years(mock_build, mock_backend):
    mock_build.return_value = MagicMock(spec=MosaicJSON)
    mock_backend.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_backend.return_value.__exit__ = MagicMock(return_value=False)

    result = runner.invoke(app, ["make-mosaics", "--years", "2020,2021", "--dataset", "geomad"])

    assert result.exit_code == 0, result.output
    assert mock_build.call_count == 2

    called_years = [c.args[0] for c in mock_build.call_args_list]
    assert called_years == ["2020", "2021"]

    out_paths = [c[0][0] for c in mock_backend.call_args_list]
    assert any("geomad_2020_mosaic.json" in p for p in out_paths)
    assert any("geomad_2021_mosaic.json" in p for p in out_paths)


@patch("ldn.cli.MosaicBackend")
@patch("ldn.cli._build_mosaic_for_year")
def test_make_mosaics_writes_with_overwrite(mock_build, mock_backend):
    mock_mosaic = MagicMock(spec=MosaicJSON)
    mock_build.return_value = mock_mosaic

    mock_writer = MagicMock()
    mock_backend.return_value.__enter__ = MagicMock(return_value=mock_writer)
    mock_backend.return_value.__exit__ = MagicMock(return_value=False)

    result = runner.invoke(app, ["make-mosaics", "--years", "2020", "--dataset", "geomad"])

    assert result.exit_code == 0, result.output
    mock_writer.write.assert_called_once_with(overwrite=True)
