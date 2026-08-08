from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import geopandas as gpd
from shapely.geometry import Polygon

from fasm_pipeline import hms_smoke_plumes


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
        )

        mock_client.put_object.assert_called_once()
        kwargs = mock_client.put_object.call_args[1]
        import json

        payload = json.loads(kwargs["Body"])

        assert payload["last_scan"] == "2026-08-07T14:00:00Z"
        assert payload["display_date"] == "2026-08-07"
        assert payload["is_fallback"] is False
        assert payload["features"] == 15
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
        )

        kwargs = mock_client.put_object.call_args[1]
        import json

        payload = json.loads(kwargs["Body"])

        assert payload["last_scan"] == "2026-08-06T20:00:00Z"
        assert payload["display_date"] == "2026-08-06"
        assert payload["is_fallback"] is True
        assert payload["features"] == 12


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
        )
        assert res == "OK"


def test_run_zero_features_with_valid_fallback():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    last_valid_end = datetime.now(timezone.utc) - timedelta(hours=3)

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, last_valid_end)),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
    ):
        res = hms_smoke_plumes.run(max_fallback_hours=6)

        # Truncate should NOT be called because fallback features are within 6 hours
        mock_truncate.assert_not_called()
        mock_write_status.assert_called_once_with(
            last_scan_dt=last_valid_end,
            is_fallback=True,
            display_date=last_valid_end.strftime("%Y-%m-%d"),
            features=25,
        )
        assert "retained 25 fallback HMS smoke plume features" in res


def test_run_zero_features_expired_fallback():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    old_end = datetime.now(timezone.utc) - timedelta(hours=10)

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, old_end)),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
    ):
        res = hms_smoke_plumes.run()

        # Truncate SHOULD be called because default fallback expired (> 6h)
        mock_truncate.assert_called_once()
        mock_write_status.assert_called_once_with(
            last_scan_dt=None,
            is_fallback=False,
            display_date=None,
            features=0,
        )
        assert "cleared HMS smoke plume table" in res
