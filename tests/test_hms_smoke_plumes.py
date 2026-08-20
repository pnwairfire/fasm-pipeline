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


def test_compute_density_and_satellite_counts():
    gdf = gpd.GeoDataFrame(
        {
            "satellite": ["GOES-EAST", "GOES-WEST", "GOES-EAST"],
            "density": ["Light", "Medium", "Dense"],
            "geom": [
                Polygon([(-120, 45), (-120, 46), (-119, 46), (-119, 45)]),
                Polygon([(-118, 45), (-118, 46), (-117, 46), (-117, 45)]),
                Polygon([(-116, 45), (-116, 46), (-115, 46), (-115, 45)]),
            ],
        }
    )
    density_counts = hms_smoke_plumes.compute_density_counts(gdf)
    assert density_counts == {"Light": 1, "Medium": 1, "Dense": 1}

    satellite_counts = hms_smoke_plumes.compute_satellite_counts(gdf)
    assert satellite_counts == {"GOES-EAST": 2, "GOES-WEST": 1}


def test_extract_and_hashing():
    raw_payload = b'{"type": "FeatureCollection", "features": [{"type": "Feature"}]}'
    mock_body = MagicMock()
    mock_body.read.return_value = raw_payload

    with patch("fasm_pipeline.hms_smoke_plumes.init_s3") as mock_s3:
        mock_client = MagicMock()
        mock_client.get_object.return_value = {
            "Body": mock_body,
            "LastModified": datetime(2026, 8, 19, 20, 0, tzinfo=timezone.utc),
        }
        mock_s3.return_value = mock_client

        raw_bytes, features, meta = hms_smoke_plumes.extract()

        assert raw_bytes == raw_payload
        assert len(features) == 1
        assert "sha256_hash" in meta
        assert meta["source_last_modified"] == "2026-08-19T20:00:00Z"
        assert meta["content_length"] == len(raw_payload)


def test_archive_raw_geojson_to_s3():
    with patch("fasm_pipeline.hms_smoke_plumes.init_epa_s3") as mock_s3:
        mock_client = MagicMock()
        mock_s3.return_value = mock_client

        dt = datetime(2026, 8, 19, 21, 30, 0, tzinfo=timezone.utc)
        raw_bytes = b'{"test": "geojson"}'
        key = hms_smoke_plumes.archive_raw_geojson_to_s3(raw_bytes, now_dt=dt)

        assert key == "hms/archive/2026/08/19/smoke_20260819_213000Z.geojson"
        mock_client.put_object.assert_called_once()
        kwargs = mock_client.put_object.call_args[1]
        assert kwargs["Key"] == key
        assert kwargs["Body"] == raw_bytes


def test_archive_status_to_s3():
    with patch("fasm_pipeline.hms_smoke_plumes.init_epa_s3") as mock_s3:
        mock_client = MagicMock()
        mock_s3.return_value = mock_client

        dt = datetime(2026, 8, 19, 21, 30, 0, tzinfo=timezone.utc)
        status = {"status": "ok"}
        key = hms_smoke_plumes.archive_status_to_s3(status, now_dt=dt)

        assert key == "status/archive/2026/08/19/hms_status_20260819_213000Z.json"
        mock_client.put_object.assert_called_once()
        kwargs = mock_client.put_object.call_args[1]
        assert kwargs["Key"] == key
        assert json.loads(kwargs["Body"]) == status


def test_write_status_to_s3_active():
    with patch("fasm_pipeline.hms_smoke_plumes.init_epa_s3") as mock_s3:
        mock_client = MagicMock()
        mock_s3.return_value = mock_client

        scan_dt = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
        now_dt = datetime(2026, 8, 7, 14, 30, tzinfo=timezone.utc)
        hms_smoke_plumes.write_status_to_s3(
            last_scan_dt=scan_dt,
            is_fallback=False,
            display_date="2026-08-07",
            features=15,
            staleness_hours=0.0,
            sha256_hash="abc123sha",
            source_last_modified="2026-08-07T14:15:00Z",
            archived_geojson_key="hms/archive/2026/08/07/smoke_20260807_143000Z.geojson",
            counts_by_density={"Light": 5, "Medium": 5, "Dense": 5},
            counts_by_satellite={"GOES-EAST": 15},
            start_utc_min=datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc),
            start_utc_max=datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc),
            end_utc_min=datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc),
            end_utc_max=scan_dt,
            now_dt=now_dt,
        )

        # 2 calls: one to status/hms_status.json and one to status/archive/...
        assert mock_client.put_object.call_count == 2
        first_call = mock_client.put_object.call_args_list[0][1]
        payload = json.loads(first_call["Body"])

        assert payload["last_scan"] == "2026-08-07T14:00:00Z"
        assert payload["display_date"] == "2026-08-07"
        assert payload["is_fallback"] is False
        assert payload["features"] == 15
        assert payload["staleness_hours"] == 0.0
        assert payload["sha256_hash"] == "abc123sha"
        assert payload["source_last_modified"] == "2026-08-07T14:15:00Z"
        assert payload["counts_by_density"] == {"Light": 5, "Medium": 5, "Dense": 5}
        assert payload["counts_by_satellite"] == {"GOES-EAST": 15}
        assert payload["start_utc_min"] == "2026-08-07T10:00:00Z"
        assert payload["end_utc_max"] == "2026-08-07T14:00:00Z"
        assert payload["last_checked"] == "2026-08-07T14:30:00Z"


def test_write_status_to_s3_fallback():
    with patch("fasm_pipeline.hms_smoke_plumes.init_epa_s3") as mock_s3:
        mock_client = MagicMock()
        mock_s3.return_value = mock_client

        scan_dt = datetime(2026, 8, 6, 20, 0, tzinfo=timezone.utc)
        now_dt = datetime(2026, 8, 7, 10, 15, tzinfo=timezone.utc)
        hms_smoke_plumes.write_status_to_s3(
            last_scan_dt=scan_dt,
            is_fallback=True,
            display_date="2026-08-06",
            features=12,
            staleness_hours=14.25,
            now_dt=now_dt,
        )

        assert mock_client.put_object.call_count == 2
        first_call = mock_client.put_object.call_args_list[0][1]
        payload = json.loads(first_call["Body"])

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
        density_counts={"Light": 10, "Medium": 6, "Dense": 4},
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
    assert payload["PlumeLightCount"] == 10
    assert payload["PlumeMediumCount"] == 6
    assert payload["PlumeDenseCount"] == 4
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
    mock_extract = (
        b'{"features": []}',
        [{"properties": {}}],
        {"sha256_hash": "mocksha", "source_last_modified": "2026-08-07T14:00:00Z"},
    )

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=mock_extract),
        patch("fasm_pipeline.hms_smoke_plumes.archive_raw_geojson_to_s3", return_value="hms/archive/k.geojson"),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=sample_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.load", return_value="OK") as mock_load,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
        patch("fasm_pipeline.hms_smoke_plumes.emit_emf_metrics") as mock_emf,
    ):
        res = hms_smoke_plumes.run()

        mock_truncate.assert_called_once()
        mock_load.assert_called_once_with(sample_gdf)
        mock_write_status.assert_called_once()
        write_kwargs = mock_write_status.call_args[1]
        assert write_kwargs["last_scan_dt"] == sample_gdf["end_utc"].max()
        assert write_kwargs["is_fallback"] is False
        assert write_kwargs["display_date"] == "2026-08-07"
        assert write_kwargs["features"] == 1
        assert write_kwargs["sha256_hash"] == "mocksha"
        mock_emf.assert_called_once()
        assert res == "OK"


def test_run_zero_features_within_staleness_window():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    now = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    # 10 hours old (well within 24h)
    last_valid_end = now - timedelta(hours=10)
    mock_extract = (
        b'{"features": []}',
        [],
        {"sha256_hash": "mocksha_empty", "source_last_modified": "2026-08-08T12:00:00Z"},
    )

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=mock_extract),
        patch("fasm_pipeline.hms_smoke_plumes.archive_raw_geojson_to_s3", return_value="hms/archive/k.geojson"),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, last_valid_end)),
        patch("fasm_pipeline.hms_smoke_plumes.datetime") as mock_dt,
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
        patch("fasm_pipeline.hms_smoke_plumes.emit_emf_metrics") as mock_emf,
    ):
        mock_dt.now.return_value = now

        res = hms_smoke_plumes.run(staleness_threshold_hours=24.0)

        mock_truncate.assert_not_called()
        mock_write_status.assert_called_once()
        write_kwargs = mock_write_status.call_args[1]
        assert write_kwargs["last_scan_dt"] == last_valid_end
        assert write_kwargs["is_fallback"] is True
        assert write_kwargs["features"] == 25
        assert write_kwargs["staleness_hours"] == 10.0
        assert write_kwargs["sha256_hash"] == "mocksha_empty"
        mock_emf.assert_called_once()
        assert "age 10.0h <= 24.0h" in res
        assert "retained 25 existing HMS smoke plume features" in res


def test_run_zero_features_exceeds_staleness_window():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    now = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    # 25 hours old (exceeds 24h threshold)
    last_valid_end = now - timedelta(hours=25)
    mock_extract = (
        b'{"features": []}',
        [],
        {"sha256_hash": "mocksha_stale", "source_last_modified": "2026-08-07T11:00:00Z"},
    )

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=mock_extract),
        patch("fasm_pipeline.hms_smoke_plumes.archive_raw_geojson_to_s3", return_value="hms/archive/k.geojson"),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(25, last_valid_end)),
        patch("fasm_pipeline.hms_smoke_plumes.datetime") as mock_dt,
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
        patch("fasm_pipeline.hms_smoke_plumes.emit_emf_metrics") as mock_emf,
    ):
        mock_dt.now.return_value = now

        res = hms_smoke_plumes.run(staleness_threshold_hours=24.0)

        mock_truncate.assert_called_once()
        mock_write_status.assert_called_once()
        write_kwargs = mock_write_status.call_args[1]
        assert write_kwargs["last_scan_dt"] is None
        assert write_kwargs["is_fallback"] is False
        assert write_kwargs["features"] == 0
        assert write_kwargs["staleness_hours"] == 25.0
        assert write_kwargs["sha256_hash"] == "mocksha_stale"
        mock_emf.assert_called_once()
        assert "for 25.0h (> 24.0h)" in res
        assert "truncated HMS smoke plume table" in res


def test_run_zero_features_empty_table():
    empty_gdf = gpd.GeoDataFrame(columns=["satellite", "density", "start_utc", "end_utc", "geom"])
    mock_extract = (
        b'{"features": []}',
        [],
        {"sha256_hash": "mocksha_empty", "source_last_modified": None},
    )

    with (
        patch("fasm_pipeline.hms_smoke_plumes.extract", return_value=mock_extract),
        patch("fasm_pipeline.hms_smoke_plumes.archive_raw_geojson_to_s3", return_value="hms/archive/k.geojson"),
        patch("fasm_pipeline.hms_smoke_plumes.transform", return_value=empty_gdf),
        patch("fasm_pipeline.hms_smoke_plumes.get_existing_table_metadata", return_value=(0, None)),
        patch("fasm_pipeline.hms_smoke_plumes.truncate") as mock_truncate,
        patch("fasm_pipeline.hms_smoke_plumes.write_status_to_s3") as mock_write_status,
        patch("fasm_pipeline.hms_smoke_plumes.emit_emf_metrics") as mock_emf,
    ):
        res = hms_smoke_plumes.run(staleness_threshold_hours=24.0)

        mock_truncate.assert_not_called()
        mock_write_status.assert_called_once()
        write_kwargs = mock_write_status.call_args[1]
        assert write_kwargs["last_scan_dt"] is None
        assert write_kwargs["is_fallback"] is False
        assert write_kwargs["features"] == 0
        assert write_kwargs["staleness_hours"] is None
        assert write_kwargs["sha256_hash"] == "mocksha_empty"
        mock_emf.assert_called_once()
        assert "plume table already empty" in res
