"""PM2.5 -> AQI conversion.

Breakpoints per the 2024 EPA PM NAAQS fact sheet:
  https://www.epa.gov/system/files/documents/2024-02/pm-naaqs-air-quality-index-fact-sheet.pdf
  https://www.govinfo.gov/content/pkg/FR-2023-01-27/pdf/2023-00269.pdf (pg 161)
"""


def pm25_to_aqi(pm25):
    if pm25 is None:
        return None

    if pm25 <= 0:
        return 0

    pm_range_low = [0.0, 9.1, 35.5, 55.5, 125.5, 225.5, 225.5]
    pm_range_high = [9.0, 35.4, 55.4, 125.4, 225.4, 325.4, 325.4]
    aqi_range_low = [0, 51, 101, 151, 201, 301, 301]
    aqi_range_high = [50, 100, 150, 200, 300, 500, 500]

    idx_key = 0
    for idx, val in enumerate(pm_range_high):
        if pm25 > val:
            idx_key = idx + 1

    idx_key = min(idx_key, len(pm_range_high) - 1)

    pm_hi = pm_range_high[idx_key]
    pm_low = pm_range_low[idx_key]
    aqi_hi = aqi_range_high[idx_key]
    aqi_low = aqi_range_low[idx_key]

    aqi_pm25 = (aqi_hi - aqi_low) / (pm_hi - pm_low) * (pm25 - pm_low) + aqi_low
    return round(aqi_pm25)
