import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import geopandas as gpd
from shapely.geometry import Polygon

from fasm_pipeline import hms_smoke_plumes


def test_is_overnight_lull():
    overnight_dt = datetime(2026, 8, 10, 8, 0, tzinfo=timezone.utc)
    daytime_dt = datetime(2026, 8, 10, 18, 0, tzinfo=timezone.utc)

    assert hms_smoke_plumes.is_overnight_lull(overnight_dt) is True
    assert hms_smoke_plumes.is_overnight_lull(daytime_dt) is False


def test_write_status_to_s3_active():
    with patch("fasm_pipeline.hms_smoke_plumes.init_epa_s3") as mock_s3:
        mock_client = MagicMock()
        mock_s3.return_value = mock_client

        scan_dt = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
        hms_smoke_plumes.write_status_to_s3(
            last_scan_dt=scan_dt,
            is_fallback=False,
            display_date="2026-08-07",
            features=15,
            consecutive_empty_scans=0,
        )

        mock_client.put_object.assert_called_once()
        kwargs = mock_client.put_object.call_args[1]

        payload = json.loads(kwargs["Body"])

        assert payload["last_scan"] == "2026-08-07T14:00:00Z"
        assert payload["display_date"] == "2026-08-07"
        assert payload["is_fallback"] is False
        assert payload["features"] == 15
        assert payload["consecutive_empty_scans"] == 0
        assert "last_checked" in payload


def test_write_status_to_s3_fallback():
    with patch("fasm_pipeline.hms_smoke_plumes.init_epa_s3") as mock_s3:
        mock_client = MagicMock()
        mock_s3.return_value = mock_client

        scan_dt = datetime(2026, 8, 6, 20, 0, tzinfo=timezone.utc)
        hms_smoke_plumes.write_status_to_s3(
            last_scan_dt=scan_dt,
            is_fallback=True,
            display_date="2026-08-06",
            features=12,
            consecutive_empty_scans=1,
        )

        kwargs = mock_client.put_object.call_args[1]

        payload = json.loads(kwargs["Body"])

        assert payload["last_scan"] == "2026-08-06T20:00:00Z"
        assert payload["display_date"] == "2026-08-06"
        assert payload["is_fallback"] is True
        assert payload["features"] == 12
        assert payload["consecutive_empty_scans"] == 1


def test_run_with_features():
    sample_gdf = gpd.GeoDataFrame(
        {
            "satellite": ["GOES-EAST"],
            "density": [10],
            "start_utc": [datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc)],
            "end_utc": [datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)],
            "geom": [Polygon([(-120, 45), (-120, 46), (-119, 46), (-119, 45)])],
        }
    )

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[{"properties": {}}]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=sample_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.load", return_value="OK") as mock_load,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
    ):
        res = hms_smoke_plumes.run()

        mock_truncate.assert_called_once()
        mock_load.assert_called_once_with(sample_gdf)
        mock_write_status.assert_called_once_with(
            last_scan_dt=sample_gdf["end_utc"].max(),
            is_fallback=False,
            display_date="2026-08-07",
            features=1,
            consecutive_empty_scans=0,
        )
        assert res == "OK"


def test_run_zero_features_overnight_lull():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    last_valid_end = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
    overnight_now = datetime(2026, 8, 8, 8, 0, tzinfo=timezone.utc)

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, last_valid_end)),
        patch("fasm_pipeline.hms_smoke_plumes.is_overnight_lull", return_value=True),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
    ):
        res = hms_smoke_plumes.run(max_empty_scans=3)

        mock_truncate.assert_not_called()
        mock_write_status.assert_called_once_with(
            last_scan_dt=last_valid_end,
            is_fallback=True,
            display_date="2026-08-07",
            features=25,
            consecutive_empty_scans=0,
        )
        assert "overnight lull" in res
        assert "retained 25 existing HMS smoke plume features" in res


def test_run_zero_features_below_threshold():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    last_valid_end = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.is_overnight_lull", return_value=False),
        patch("fasm_pipeline.hms_smoke_plumes.read_status_from_s3", return_value={"consecutive_empty_scans": 0}),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, last_valid_end)),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
    ):
        res = hms_smoke_plumes.run(max_empty_scans=3)

        # Truncate should NOT be called because scan 1 < 3
        mock_truncate.assert_not_called()
        mock_write_status.assert_called_once_with(
            last_scan_dt=last_valid_end,
            is_fallback=True,
            display_date="2026-08-07",
            features=25,
            consecutive_empty_scans=1,
        )
        assert "scan 1/3" in res
        assert "retained 25 existing HMS smoke plume features" in res


def test_run_zero_features_at_threshold():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    last_valid_end = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.is_overnight_lull", return_value=False),
        patch("fasm_pipeline.hms_smoke_plumes.read_status_from_s3", return_value={"consecutive_empty_scans": 2}),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, last_valid_end)),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
    ):
        res = hms_smoke_plumes.run(max_empty_scans=3)

        # Truncate SHOULD be called because scan 3 >= 3
        mock_truncate.assert_called_once()
        mock_write_status.assert_called_once_with(
            last_scan_dt=None,
            is_fallback=False,
            display_date=None,
            features=0,
            consecutive_empty_scans=3,
        )
        assert "for 3 consecutive scan(s)" in res
        assert "truncated HMS smoke plume table" in res
