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


@patch("ldn.cli.s3_client.put_object")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli.load_stac_geoparquet_features")
def test_make_mosaics_geomad_single_year(mock_load, mock_years, mock_build, mock_put):
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
            "--product-owner",
            "dep",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    mock_build.assert_called_once()
    mock_put.assert_called_once()
    out_path = mock_put.call_args.kwargs["Key"]
    assert "mosaics/2020/2020_mosaic.json" in out_path


@patch("ldn.cli.s3_client.put_object")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli.load_stac_geoparquet_features")
def test_make_mosaics_prediction_single_year(mock_load, mock_years, mock_build, mock_put):
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
            "--product-owner",
            "dep",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    mock_put.assert_called_once()
    out_path = mock_put.call_args.kwargs["Key"]
    assert "mosaics/2020/2020_mosaic.json" in out_path


@patch("ldn.cli.s3_client.put_object")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli.load_stac_geoparquet_features")
def test_make_mosaics_multiple_years(mock_load, mock_years, mock_build, mock_put):
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
            "--product-owner",
            "dep",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    assert mock_put.call_count == 2
    out_paths = [c.kwargs["Key"] for c in mock_put.call_args_list]
    assert any("mosaics/2020/2020_mosaic.json" in p for p in out_paths)
    assert any("mosaics/2021/2021_mosaic.json" in p for p in out_paths)


@patch("ldn.cli.s3_client.put_object")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli.load_stac_geoparquet_features")
def test_make_mosaics_passes_bucket_to_write(mock_load, mock_years, mock_build, mock_put):
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
            "--product-owner",
            "dep",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    assert mock_put.call_args.kwargs["Bucket"] == "dep-public-staging"


@patch("ldn.cli.s3_client.put_object")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli.load_stac_geoparquet_features")
def test_make_mosaics_writes_json_content_type(mock_load, mock_years, mock_build, mock_put):
    features = [_make_feature("item-1", [103.6, 1.2, 104.0, 1.5])]
    mock_load.return_value = features
    mock_years.return_value = [2020]
    mosaic = MagicMock()
    mosaic.model_dump_json.return_value = '{"tiles": []}'
    mock_build.return_value = mosaic

    result = runner.invoke(
        app,
        [
            "make-mosaics",
            "--dataset",
            "geomad",
            "--single-region",
            "--product-owner",
            "dep",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    call_kwargs = mock_put.call_args.kwargs
    assert call_kwargs["ContentType"] == "application/json"
    assert "mosaics/2020/2020_mosaic.json" in call_kwargs["Key"]


@patch("ldn.cli.s3_client.put_object")
@patch("ldn.cli._build_mosaic_for_year")
@patch("ldn.cli._extract_years")
@patch("ldn.cli.load_stac_geoparquet_features")
def test_make_mosaics_body_is_utf8_encoded_json(mock_load, mock_years, mock_build, mock_put):
    features = [_make_feature("item-1", [103.6, 1.2, 104.0, 1.5])]
    mock_load.return_value = features
    mock_years.return_value = [2020]
    mosaic = MagicMock()
    mosaic.model_dump_json.return_value = '{"minzoom": 5}'
    mock_build.return_value = mosaic

    result = runner.invoke(
        app,
        [
            "make-mosaics",
            "--dataset",
            "geomad",
            "--single-region",
            "--product-owner",
            "dep",
            "--version-geomad",
            GEOMAD_VERSION,
            "--version-lulc",
            LULC_VERSION,
        ],
    )

    assert result.exit_code == 0, result.output
    body = mock_put.call_args.kwargs["Body"]
    assert isinstance(body, bytes)
    assert body.decode("utf-8") == '{"minzoom": 5}'
