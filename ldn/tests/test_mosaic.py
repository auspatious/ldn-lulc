from unittest.mock import MagicMock, patch

import pytest
from cogeo_mosaic.mosaic import MosaicJSON
from typer.testing import CliRunner

from ldn.cli import _build_mosaic_for_year, _stac_self_link, app
from ldn.utils import GEOMAD_VERSION, PREDICTION_VERSION, LdnError

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


def _make_feature(item_id: str, bbox: list[float], year: str = "2020") -> dict:
    """Helper to create a STAC feature dict."""
    minx, miny, maxx, maxy = bbox
    return {
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
        "properties": {"datetime": f"{year}-06-01T00:00:00Z"},
        "assets": {},
    }


def test_build_mosaic_for_year_returns_mosaic():
    features = [
        _make_feature("item-1", [103.6, 1.2, 104.0, 1.5]),
        _make_feature("item-2", [104.0, 1.2, 104.4, 1.5]),
        _make_feature("item-3", [103.6, 1.5, 104.0, 1.8]),
    ]

    mosaic = _build_mosaic_for_year(2020, features)

    assert isinstance(mosaic, MosaicJSON)
    assert mosaic.minzoom == 5
    assert mosaic.maxzoom == 12


def test_build_mosaic_for_year_raises_on_empty():
    with pytest.raises(LdnError, match="No STAC items found for year 2020"):
        _build_mosaic_for_year(2020, [])


def test_build_mosaic_for_year_converts_multipolygon_to_convex_hull():
    """Items with MultiPolygon geometries should be converted to convex hull."""
    feature = {
        "id": "multi-item",
        "type": "Feature",
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]],
            ],
        },
        "links": [{"rel": "self", "href": "https://example.com/items/multi-item"}],
        "properties": {"datetime": "2020-06-01T00:00:00Z"},
        "assets": {},
    }

    mosaic = _build_mosaic_for_year(2020, [feature])

    assert isinstance(mosaic, MosaicJSON)


# make_mosaics CLI command


@pytest.fixture
def mock_write_session():
    with patch("ldn.cli.get_write_session") as mock_get_session:
        mock_frozen = MagicMock()
        mock_frozen.access_key = "AKIAIOSFODNN7EXAMPLE"
        mock_frozen.secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        mock_frozen.token = None

        mock_creds = MagicMock()
        mock_creds.get_frozen_credentials.return_value = mock_frozen

        mock_session = MagicMock()
        mock_session.get_credentials.return_value = mock_creds
        mock_session.region_name = "us-west-2"
        mock_get_session.return_value = mock_session

        yield mock_get_session


@patch("ldn.cli._write_mosaic")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli._load_all_features")
@patch("ldn.cli.get_write_session")
def test_make_mosaics_geomad_single_year(
    mock_session, mock_load, mock_years, mock_build, mock_write, mock_write_session
):
    features = [_make_feature("item-1", [103.6, 1.2, 104.0, 1.5])]
    mock_load.return_value = features
    mock_years.return_value = [2020]
    mock_build.return_value = MagicMock()

    result = runner.invoke(
        app,
        [
            "make-mosaics",
            "--dataset",
            "geomad",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-prediction",
            PREDICTION_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    mock_build.assert_called_once()
    mock_write.assert_called_once()
    out_path = mock_write.call_args[0][1]
    assert "geomad_2020_mosaic.json" in out_path


@patch("ldn.cli._write_mosaic")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli._load_all_features")
@patch("ldn.cli.get_write_session")
def test_make_mosaics_prediction_single_year(
    mock_session, mock_load, mock_years, mock_build, mock_write, mock_write_session
):
    features = [_make_feature("item-1", [103.6, 1.2, 104.0, 1.5])]
    mock_load.return_value = features
    mock_years.return_value = [2020]
    mock_build.return_value = MagicMock()

    result = runner.invoke(
        app,
        [
            "make-mosaics",
            "--dataset",
            "prediction",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-prediction",
            PREDICTION_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once()
    out_path = mock_write.call_args[0][1]
    assert "prediction_2020_mosaic.json" in out_path


@patch("ldn.cli._write_mosaic")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli._load_all_features")
@patch("ldn.cli.get_write_session")
def test_make_mosaics_multiple_years(mock_session, mock_load, mock_years, mock_build, mock_write, mock_write_session):
    features = [
        _make_feature("item-1", [103.6, 1.2, 104.0, 1.5], "2020"),
        _make_feature("item-2", [104.0, 1.2, 104.4, 1.5], "2021"),
    ]
    mock_load.return_value = features
    mock_years.return_value = [2020, 2021]
    mock_build.return_value = MagicMock()

    result = runner.invoke(
        app,
        [
            "make-mosaics",
            "--dataset",
            "geomad",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-prediction",
            PREDICTION_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    assert mock_write.call_count == 2
    out_paths = [c[0][1] for c in mock_write.call_args_list]
    assert any("geomad_2020_mosaic.json" in p for p in out_paths)
    assert any("geomad_2021_mosaic.json" in p for p in out_paths)


@patch("ldn.cli._write_mosaic")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli._load_all_features")
@patch("ldn.cli.get_write_session")
def test_make_mosaics_passes_session_to_write(
    mock_session, mock_load, mock_years, mock_build, mock_write, mock_write_session
):
    features = [_make_feature("item-1", [103.6, 1.2, 104.0, 1.5])]
    mock_load.return_value = features
    mock_years.return_value = [2020]
    mock_build.return_value = MagicMock()
    fake_session = MagicMock()
    mock_session.return_value = fake_session

    result = runner.invoke(
        app,
        [
            "make-mosaics",
            "--dataset",
            "geomad",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-prediction",
            PREDICTION_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    assert mock_write.call_args[0][2] is fake_session
