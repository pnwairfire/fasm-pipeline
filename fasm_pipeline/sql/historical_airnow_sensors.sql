WITH source_data AS (
  SELECT
    am.unit_id,
    ROUND(am.latitude, 5) AS latitude,
    ROUND(am.longitude, 5) AS longitude,
    am.utc_ts,
    am.local_ts,
    am.timezone,
    am.raw_pm25,
    am.nowcast,
    am.aqi,
    am.site_name,
    am.deployment_type,
    am.device_type,
    am.instrument,
    am.aqsid,
    am.full_aqsid,
    am.latency_mins,
    CASE
      WHEN eo.unit_id IS NOT NULL AND eo.unit_status != 0 THEN eo.unit_status
      ELSE am.status
    END AS status,
    'AirNow' as provider,
    am.geom
  FROM pwfsl_map.airnow_monitors am
  LEFT JOIN pwfsl_map.elwood_outliers eo
    ON am.unit_id::VARCHAR = eo.unit_id::VARCHAR
  WHERE am.deployment_type != 'Temporary' AND am.device_type = 'Sensor'
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
  FROM pwfsl_historical.airnow_sensors h
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

INSERT INTO pwfsl_historical.airnow_sensors 
  (unit_id, latitude, longitude, utc_ts, local_ts, timezone, raw_pm25, nowcast, aqi,
   site_name, deployment_type, device_type, instrument, aqsid, full_aqsid, latency_mins, status, provider, geom)
SELECT 
  unit_id, latitude, longitude, utc_ts, local_ts, timezone, raw_pm25, nowcast, aqi,
  site_name, deployment_type, device_type, instrument, aqsid, full_aqsid, latency_mins, status, provider, geom
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
  provider = EXCLUDED.provider,
  geom = EXCLUDED.geom,
  created_at = NOW()
WHERE pwfsl_historical.airnow_sensors.raw_pm25 IS DISTINCT FROM EXCLUDED.raw_pm25
   OR pwfsl_historical.airnow_sensors.nowcast IS DISTINCT FROM EXCLUDED.nowcast
   OR pwfsl_historical.airnow_sensors.status IS DISTINCT FROM EXCLUDED.status;