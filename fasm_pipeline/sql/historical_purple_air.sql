WITH source_data AS (
  SELECT 
    pa.unit_id,
    ROUND(latitude, 5) AS latitude,
    ROUND(longitude, 5) AS longitude,
    utc_ts,
    local_ts,
    timezone,
    corrected_pm25,
    nowcast,
    raw_pm25,
    aqi,
    latency_mins,
    CASE
      WHEN eo.unit_id IS NOT NULL AND eo.unit_status != 0 THEN eo.unit_status
      ELSE pa.status
    END AS status,
    location_name,
    geom
  FROM pwfsl_map.purple_air pa
  LEFT JOIN pwfsl_map.sensor_name_override sno
    ON pa.unit_id::VARCHAR = sno.unit_id
  LEFT JOIN pwfsl_map.elwood_outliers eo
    ON pa.unit_id::VARCHAR = eo.unit_id::VARCHAR
),

-- Capture changes before upserting
changes AS (
  SELECT 
    h.unit_id::VARCHAR AS unit_id,
    h.utc_ts,
    h.raw_pm25 AS old_raw_pm25,
    s.raw_pm25 AS new_raw_pm25,
    h.nowcast AS old_nowcast,
    s.nowcast AS new_nowcast,
    h.status AS old_status,
    s.status AS new_status
  FROM pwfsl_historical.purple_air h
  INNER JOIN source_data s
    ON h.unit_id = s.unit_id AND h.utc_ts = s.utc_ts
  WHERE h.raw_pm25 IS DISTINCT FROM s.raw_pm25
     OR h.nowcast IS DISTINCT FROM s.nowcast
     OR h.status IS DISTINCT FROM s.status
),

-- Insert changes into changelog
log_changes AS (
  INSERT INTO pwfsl_historical.aq_changelog 
    (unit_id, utc_ts, old_raw_pm25, new_raw_pm25, old_nowcast, new_nowcast, old_status, new_status)
  SELECT unit_id, utc_ts, old_raw_pm25, new_raw_pm25, old_nowcast, new_nowcast, old_status, new_status
  FROM changes
  RETURNING *
)

-- Upsert into historical
INSERT INTO pwfsl_historical.purple_air 
  (unit_id, latitude, longitude, utc_ts, local_ts, timezone, 
   corrected_pm25, nowcast, raw_pm25, aqi, latency_mins, status, location_name, geom)
SELECT 
  unit_id, latitude, longitude, utc_ts, local_ts, timezone,
  corrected_pm25, nowcast, raw_pm25, aqi, latency_mins, status, location_name, geom
FROM source_data
ON CONFLICT (unit_id, utc_ts) 
DO UPDATE SET
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  local_ts = EXCLUDED.local_ts,
  timezone = EXCLUDED.timezone,
  corrected_pm25 = EXCLUDED.corrected_pm25,
  nowcast = EXCLUDED.nowcast,
  raw_pm25 = EXCLUDED.raw_pm25,
  aqi = EXCLUDED.aqi,
  latency_mins = EXCLUDED.latency_mins,
  status = EXCLUDED.status,
  location_name = EXCLUDED.location_name,
  geom = EXCLUDED.geom,
  created_at = NOW()
WHERE pwfsl_historical.purple_air.raw_pm25 IS DISTINCT FROM EXCLUDED.raw_pm25
   OR pwfsl_historical.purple_air.nowcast IS DISTINCT FROM EXCLUDED.nowcast
   OR pwfsl_historical.purple_air.status IS DISTINCT FROM EXCLUDED.status;