WITH source_data AS (
  SELECT 
    unit_id,
    ROUND(latitude, 5) AS latitude,
    ROUND(longitude, 5) AS longitude,
    utc_ts,
    local_ts,
    timezone,
    raw_pm25,
    nowcast,
    aqi,
    site_name,
    deployment_type,
    device_type,
    instrument,
    aqsid,
    full_aqsid,
    latency_mins,
    status,
    geom
  FROM pwfsl_map.airnow_monitors
  WHERE deployment_type != 'Temporary' AND device_type != 'Sensor'
),

changes AS (
  SELECT 
    h.unit_id,
    h.utc_ts,
    h.raw_pm25 AS old_raw_pm25,
    s.raw_pm25 AS new_raw_pm25,
    h.nowcast AS old_nowcast,
    s.nowcast AS new_nowcast,
    h.status AS old_status,
    s.status AS new_status
  FROM pwfsl_historical.permanent_monitors h
  INNER JOIN source_data s
    ON h.unit_id = s.unit_id AND h.utc_ts = s.utc_ts
  WHERE h.raw_pm25 IS DISTINCT FROM s.raw_pm25
     OR h.nowcast IS DISTINCT FROM s.nowcast
     OR h.status IS DISTINCT FROM s.status
),

log_changes AS (
  INSERT INTO pwfsl_historical.aq_changelog 
    (unit_id, utc_ts, old_raw_pm25, new_raw_pm25, old_nowcast, new_nowcast, old_status, new_status)
  SELECT unit_id, utc_ts, old_raw_pm25, new_raw_pm25, old_nowcast, new_nowcast, old_status, new_status
  FROM changes
  RETURNING *
)

INSERT INTO pwfsl_historical.permanent_monitors 
  (unit_id, latitude, longitude, utc_ts, local_ts, timezone, raw_pm25, nowcast, aqi,
   site_name, deployment_type, device_type, instrument, aqsid, full_aqsid, latency_mins, status, geom)
SELECT 
  unit_id, latitude, longitude, utc_ts, local_ts, timezone, raw_pm25, nowcast, aqi,
  site_name, deployment_type, device_type, instrument, aqsid, full_aqsid, latency_mins, status, geom
FROM source_data
ON CONFLICT (unit_id, utc_ts) 
DO UPDATE SET
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  local_ts = EXCLUDED.local_ts,
  timezone = EXCLUDED.timezone,
  raw_pm25 = EXCLUDED.raw_pm25,
  nowcast = EXCLUDED.nowcast,
  aqi = EXCLUDED.aqi,
  site_name = EXCLUDED.site_name,
  deployment_type = EXCLUDED.deployment_type,
  device_type = EXCLUDED.device_type,
  instrument = EXCLUDED.instrument,
  aqsid = EXCLUDED.aqsid,
  full_aqsid = EXCLUDED.full_aqsid,
  latency_mins = EXCLUDED.latency_mins,
  status = EXCLUDED.status,
  geom = EXCLUDED.geom,
  created_at = NOW()
WHERE pwfsl_historical.permanent_monitors.raw_pm25 IS DISTINCT FROM EXCLUDED.raw_pm25
   OR pwfsl_historical.permanent_monitors.nowcast IS DISTINCT FROM EXCLUDED.nowcast
   OR pwfsl_historical.permanent_monitors.status IS DISTINCT FROM EXCLUDED.status;