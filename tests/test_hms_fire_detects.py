from datetime import datetime
from fasm_pipeline.hms_fire_detects import parse_hms_datetime


def test_parse_hms_datetime_int():
    res = parse_hms_datetime(2026193, 1200)
    assert res == datetime(2026, 7, 12, 12, 0)


def test_parse_hms_datetime_str():
    res = parse_hms_datetime("2026193", "1230")
    assert res == datetime(2026, 7, 12, 12, 30)


def test_parse_hms_datetime_float():
    res = parse_hms_datetime(2026193, 1230.0)
    assert res == datetime(2026, 7, 12, 12, 30)
