from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from ldn.cli import app
from ldn.utils import GEOMAD_VERSION, LULC_VERSION

runner = CliRunner()


@pytest.fixture(autouse=True)
def mock_required_env(monkeypatch):
    """Set required CLI env vars so tests do not depend on shell state."""
    monkeypatch.setenv("BUCKET", "dep-public-staging")


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


# make_mosaics CLI command


@pytest.fixture
def mock_write_session():
    with patch("ldn.cli.boto3.Session") as mock_boto_session:
        mock_frozen = MagicMock()
        mock_frozen.access_key = "AKIAIOSFODNN7EXAMPLE"
        mock_frozen.secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        mock_frozen.token = None

        mock_creds = MagicMock()
        mock_creds.get_frozen_credentials.return_value = mock_frozen

        mock_session = MagicMock()
        mock_session.get_credentials.return_value = mock_creds
        mock_session.region_name = "us-west-2"
        mock_boto_session.return_value = mock_session

        yield mock_boto_session


@patch("ldn.cli._write_mosaic")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli._load_all_features")
def test_make_mosaics_geomad_single_year(mock_load, mock_years, mock_build, mock_write, mock_write_session):
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
            "--single-region",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    mock_build.assert_called_once()
    mock_write.assert_called_once()
    out_path = mock_write.call_args[0][2]
    assert "mosaics/2020/2020_mosaic.json" in out_path


@patch("ldn.cli._write_mosaic")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli._load_all_features")
def test_make_mosaics_prediction_single_year(mock_load, mock_years, mock_build, mock_write, mock_write_session):
    features = [_make_feature("item-1", [103.6, 1.2, 104.0, 1.5])]
    mock_load.return_value = features
    mock_years.return_value = [2020]
    mock_build.return_value = MagicMock()

    result = runner.invoke(
        app,
        [
            "make-mosaics",
            "--dataset",
            "lulc",
            "--single-region",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once()
    out_path = mock_write.call_args[0][2]
    assert "mosaics/2020/2020_mosaic.json" in out_path


@patch("ldn.cli._write_mosaic")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli._load_all_features")
def test_make_mosaics_multiple_years(mock_load, mock_years, mock_build, mock_write, mock_write_session):
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
            "--single-region",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    assert mock_write.call_count == 2
    out_paths = [c[0][2] for c in mock_write.call_args_list]
    assert any("mosaics/2020/2020_mosaic.json" in p for p in out_paths)
    assert any("mosaics/2021/2021_mosaic.json" in p for p in out_paths)


@patch("ldn.cli._write_mosaic")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli._load_all_features")
def test_make_mosaics_passes_bucket_to_write(mock_load, mock_years, mock_build, mock_write, mock_write_session):
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
            "--single-region",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    assert mock_write.call_args[0][1] == "dep-public-staging"
