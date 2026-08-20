from datetime import datetime, timedelta, timezone
import json
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
            staleness_hours=0.0,
        )

        mock_client.put_object.assert_called_once()
        kwargs = mock_client.put_object.call_args[1]

        payload = json.loads(kwargs["Body"])

        assert payload["last_scan"] == "2026-08-07T14:00:00Z"
        assert payload["display_date"] == "2026-08-07"
        assert payload["is_fallback"] is False
        assert payload["features"] == 15
        assert payload["staleness_hours"] == 0.0
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
            staleness_hours=14.25,
        )

        kwargs = mock_client.put_object.call_args[1]

        payload = json.loads(kwargs["Body"])

        assert payload["last_scan"] == "2026-08-06T20:00:00Z"
        assert payload["display_date"] == "2026-08-06"
        assert payload["is_fallback"] is True
        assert payload["features"] == 12
        assert payload["staleness_hours"] == 14.25


def test_emit_emf_metrics(capsys):
    hms_smoke_plumes.emit_emf_metrics(
        feature_count=20,
        staleness_hours=12.5,
        is_truncated=False,
        environment="test",
    )
    captured = capsys.readouterr()
    lines = [line for line in captured.out.strip().split("\n") if line]
    assert len(lines) >= 1

    payload = json.loads(lines[-1])
    assert "_aws" in payload
    assert payload["Environment"] == "test"
    assert payload["Stream"] == "hms-smoke-plumes"
    assert payload["PlumeFeatureCount"] == 20
    assert payload["PlumeAgeHours"] == 12.5
    assert payload["PlumeTableTruncated"] == 0


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
        patch("fasm_pipeline.hms_smoke_plumes.emit_emf_metrics") as mock_emf,
    ):
        res = hms_smoke_plumes.run()

        mock_truncate.assert_called_once()
        mock_load.assert_called_once_with(sample_gdf)
        mock_write_status.assert_called_once_with(
            last_scan_dt=sample_gdf["end_utc"].max(),
            is_fallback=False,
            display_date="2026-08-07",
            features=1,
            staleness_hours=0.0,
        )
        mock_emf.assert_called_once_with(feature_count=1, staleness_hours=0.0, is_truncated=False)
        assert res == "OK"


def test_run_zero_features_within_staleness_window():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    now = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    # 10 hours old (well within 24h)
    last_valid_end = now - timedelta(hours=10)

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, last_valid_end)),
        patch("fasm_pipeline.hms_smoke_plumes.datetime") as mock_dt,
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
        patch("fasm_pipeline.hms_smoke_plumes.emit_emf_metrics") as mock_emf,
    ):
        mock_dt.now.return_value = now

        res = hms_smoke_plumes.run(staleness_threshold_hours=24.0)

        # Should NOT truncate
        mock_truncate.assert_not_called()
        mock_write_status.assert_called_once_with(
            last_scan_dt=last_valid_end,
            is_fallback=True,
            display_date=last_valid_end.strftime("%Y-%m-%d"),
            features=25,
            staleness_hours=10.0,
        )
        mock_emf.assert_called_once_with(feature_count=25, staleness_hours=10.0, is_truncated=False)
        assert "age 10.0h <= 24.0h" in res
        assert "retained 25 existing HMS smoke plume features" in res


def test_run_zero_features_exceeds_staleness_window():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    now = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    # 25 hours old (exceeds 24h threshold)
    last_valid_end = now - timedelta(hours=25)

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, last_valid_end)),
        patch("fasm_pipeline.hms_smoke_plumes.datetime") as mock_dt,
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
        patch("fasm_pipeline.hms_smoke_plumes.emit_emf_metrics") as mock_emf,
    ):
        mock_dt.now.return_value = now

        res = hms_smoke_plumes.run(staleness_threshold_hours=24.0)

        # SHOULD truncate
        mock_truncate.assert_called_once()
        mock_write_status.assert_called_once_with(
            last_scan_dt=None,
            is_fallback=False,
            display_date=None,
            features=0,
            staleness_hours=25.0,
        )
        mock_emf.assert_called_once_with(feature_count=0, staleness_hours=25.0, is_truncated=True)
        assert "for 25.0h (> 24.0h)" in res
        assert "truncated HMS smoke plume table" in res


def test_run_zero_features_empty_table():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=[]),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(0, None)),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
        patch("fasm_pipeline.hms_smoke_plumes.emit_emf_metrics") as mock_emf,
    ):
        res = hms_smoke_plumes.run(staleness_threshold_hours=24.0)

        mock_truncate.assert_not_called()
        mock_write_status.assert_called_once_with(
            last_scan_dt=None,
            is_fallback=False,
            display_date=None,
            features=0,
            staleness_hours=None,
        )
        mock_emf.assert_called_once_with(feature_count=0, staleness_hours=None, is_truncated=False)
        assert "plume table already empty" in res
