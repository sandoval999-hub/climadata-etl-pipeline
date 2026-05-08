"""
transform.py — Data transformation, validation, and enrichment.

Handles:
- Parsing raw API JSON into structured records (HourlyWeatherRecord, DailyWeatherRecord)
- Timestamp normalization (ISO 8601 → MySQL DATETIME)
- Heat Index calculation (Rothfusz regression equation)
- Alert flag computation (heat, rain, wind)
- Data validation with range checks
- Discarded record tracking for quality logging
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, date, time, timezone
from typing import Any, Optional

from src.models import (
    City,
    DailyWeatherRecord,
    DiscardedRecord,
    HourlyWeatherRecord,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Heat Index Calculation (Rothfusz Regression)
# =============================================================================

def calculate_heat_index(temperature_c: float, humidity: float) -> Optional[float]:
    """
    Calculate the Heat Index using the Rothfusz regression equation.

    This is the same formula used by the U.S. National Weather Service.
    The formula expects temperature in Fahrenheit, so we convert internally.

    The Heat Index is only meaningful when:
    - Temperature >= 26.7°C (80°F)
    - Relative Humidity >= 40%

    If conditions are not met, returns None (Heat Index not applicable).

    Reference: https://www.calculator.net/heat-index-calculator.html

    Args:
        temperature_c: Air temperature in Celsius.
        humidity: Relative humidity in percent (0-100).

    Returns:
        Heat Index in Celsius, or None if conditions are not met.
    """
    if temperature_c is None or humidity is None:
        return None

    # Heat Index is only defined for T >= 80°F (26.7°C) and RH >= 40%
    if temperature_c < 26.7 or humidity < 40:
        return None

    # Convert to Fahrenheit for the Rothfusz equation
    t_f = (temperature_c * 9.0 / 5.0) + 32.0

    # Rothfusz regression coefficients
    c1 = -42.379
    c2 = 2.04901523
    c3 = 10.14333127
    c4 = -0.22475541
    c5 = -6.83783e-3
    c6 = -5.481717e-2
    c7 = 1.22874e-3
    c8 = 8.5282e-4
    c9 = -1.99e-6

    hi_f = (
        c1
        + c2 * t_f
        + c3 * humidity
        + c4 * t_f * humidity
        + c5 * t_f ** 2
        + c6 * humidity ** 2
        + c7 * t_f ** 2 * humidity
        + c8 * t_f * humidity ** 2
        + c9 * t_f ** 2 * humidity ** 2
    )

    # Adjustment for low humidity
    if humidity < 13 and 80 <= t_f <= 112:
        adjustment = -((13 - humidity) / 4) * math.sqrt((17 - abs(t_f - 95)) / 17)
        hi_f += adjustment

    # Adjustment for high humidity
    if humidity > 85 and 80 <= t_f <= 87:
        adjustment = ((humidity - 85) / 10) * ((87 - t_f) / 5)
        hi_f += adjustment

    # Convert back to Celsius
    hi_c = (hi_f - 32.0) * 5.0 / 9.0
    return round(hi_c, 2)


# =============================================================================
# Timestamp Parsing
# =============================================================================

def _parse_timestamp(raw: str) -> datetime:
    """
    Parse an ISO 8601 timestamp string into a Python datetime.

    The API may return timestamps in several formats:
    - "2026-05-07T12:00"        (hourly, no seconds)
    - "2026-05-07T12:00:00"     (with seconds)
    - "2026-05-07"              (daily, date only)

    All are converted to a timezone-naive datetime in UTC.
    """
    raw = raw.strip()
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: '{raw}'")


def _parse_time_from_iso(raw: Optional[str]) -> Optional[time]:
    """
    Extract the time portion from an ISO datetime string.

    Used for sunrise/sunset fields which come as full datetime strings
    like "2026-05-07T06:15" but we only need the time part.
    """
    if not raw:
        return None
    try:
        dt = _parse_timestamp(raw)
        return dt.time()
    except (ValueError, AttributeError):
        return None


# =============================================================================
# Validation
# =============================================================================

def _validate_temperature(
    value: Optional[float],
    city_id: int,
    timestamp_str: str,
    variable_name: str,
    execution_id: int,
    discarded: list[DiscardedRecord],
    config_validation: dict[str, float],
) -> Optional[float]:
    """Validate temperature is within [-10, 55] °C."""
    if value is None:
        return None
    t_min = config_validation.get("temperature_min", -10.0)
    t_max = config_validation.get("temperature_max", 55.0)
    if value < t_min or value > t_max:
        discarded.append(DiscardedRecord(
            execution_id=execution_id,
            city_id=city_id,
            record_timestamp=timestamp_str,
            variable_name=variable_name,
            invalid_value=str(value),
            reason=f"Temperature {value}°C outside valid range [{t_min}, {t_max}]",
        ))
        logger.warning(
            "DISCARDED | city_id=%d | ts=%s | %s=%s | out of range [%s, %s]",
            city_id, timestamp_str, variable_name, value, t_min, t_max,
        )
        return None
    return value


def _validate_humidity(
    value: Optional[float],
    city_id: int,
    timestamp_str: str,
    execution_id: int,
    discarded: list[DiscardedRecord],
    config_validation: dict[str, float],
) -> Optional[float]:
    """Validate humidity is within [0, 100] %."""
    if value is None:
        return None
    h_min = config_validation.get("humidity_min", 0.0)
    h_max = config_validation.get("humidity_max", 100.0)
    if value < h_min or value > h_max:
        discarded.append(DiscardedRecord(
            execution_id=execution_id,
            city_id=city_id,
            record_timestamp=timestamp_str,
            variable_name="relative_humidity",
            invalid_value=str(value),
            reason=f"Humidity {value}% outside valid range [{h_min}, {h_max}]",
        ))
        logger.warning(
            "DISCARDED | city_id=%d | ts=%s | humidity=%s | out of range [%s, %s]",
            city_id, timestamp_str, value, h_min, h_max,
        )
        return None
    return value


def _validate_precipitation(
    value: Optional[float],
    city_id: int,
    date_str: str,
    execution_id: int,
    discarded: list[DiscardedRecord],
) -> Optional[float]:
    """Validate precipitation is not negative."""
    if value is None:
        return None
    if value < 0:
        discarded.append(DiscardedRecord(
            execution_id=execution_id,
            city_id=city_id,
            record_timestamp=date_str,
            variable_name="precipitation_sum",
            invalid_value=str(value),
            reason=f"Precipitation {value}mm is negative",
        ))
        logger.warning(
            "DISCARDED | city_id=%d | date=%s | precipitation=%s | negative value",
            city_id, date_str, value,
        )
        return None
    return value


# =============================================================================
# Transform Functions
# =============================================================================

def transform_hourly(
    raw_json: dict[str, Any],
    city: City,
    execution_id: int,
    config: dict[str, Any],
) -> tuple[list[HourlyWeatherRecord], list[DiscardedRecord]]:
    """
    Transform raw hourly API response into validated HourlyWeatherRecord objects.

    Steps:
    1. Parse timestamps from ISO 8601 to datetime
    2. Validate each measurement against configured ranges
    3. Calculate Heat Index (Rothfusz formula)
    4. Set alert flags
    5. Track discarded records

    Args:
        raw_json: Raw API response containing 'hourly' key.
        city: City object with id, name, country.
        execution_id: Current pipeline execution ID.
        config: Full pipeline config dict.

    Returns:
        Tuple of (valid_records, discarded_records).
    """
    records: list[HourlyWeatherRecord] = []
    discarded: list[DiscardedRecord] = []

    hourly = raw_json.get("hourly")
    if not hourly:
        logger.warning("No hourly data in response for %s", city.name)
        return records, discarded

    timestamps = hourly.get("time", [])
    temperatures = hourly.get("temperature_2m", [])
    humidities = hourly.get("relative_humidity_2m", [])
    wind_speeds = hourly.get("wind_speed_10m", [])
    weather_codes = hourly.get("weather_code", [])

    validation_cfg = config.get("validation", {})
    thresholds = config.get("thresholds", {})
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for i, ts_raw in enumerate(timestamps):
        try:
            ts = _parse_timestamp(ts_raw)
        except ValueError as exc:
            logger.error("Failed to parse timestamp '%s': %s", ts_raw, exc)
            continue

        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        # Safely get values (API arrays may have None for missing data)
        raw_temp = temperatures[i] if i < len(temperatures) else None
        raw_hum = humidities[i] if i < len(humidities) else None
        raw_wind = wind_speeds[i] if i < len(wind_speeds) else None
        raw_wcode = weather_codes[i] if i < len(weather_codes) else None

        # Validate
        temp = _validate_temperature(
            raw_temp, city.city_id, ts_str, "temperature_2m",
            execution_id, discarded, validation_cfg,
        )
        hum = _validate_humidity(
            raw_hum, city.city_id, ts_str, execution_id, discarded, validation_cfg,
        )
        wind = raw_wind  # Wind speed doesn't have validation range per requirements

        # Calculate Heat Index
        heat_idx = None
        if temp is not None and hum is not None:
            heat_idx = calculate_heat_index(temp, hum)

        # Alert flags
        hi_alert = (
            temp is not None
            and hum is not None
            and temp > thresholds.get("heat_index_temp_min", 27.0)
            and hum > thresholds.get("heat_index_humidity_min", 40.0)
        )
        wind_alert = (
            wind is not None
            and wind > thresholds.get("high_wind_kmh", 60.0)
        )

        records.append(HourlyWeatherRecord(
            city_id=city.city_id,
            timestamp=ts,
            temperature_2m=temp,
            relative_humidity=hum,
            wind_speed_10m=wind,
            weather_code=raw_wcode,
            heat_index=heat_idx,
            heat_index_alert=hi_alert,
            high_wind_alert=wind_alert,
            ingested_at=now,
        ))

    logger.info(
        "Transformed %d hourly records for %s (%d discarded)",
        len(records), city.name, len(discarded),
    )
    return records, discarded


def transform_daily(
    raw_json: dict[str, Any],
    city: City,
    execution_id: int,
    config: dict[str, Any],
) -> tuple[list[DailyWeatherRecord], list[DiscardedRecord]]:
    """
    Transform raw daily API response into validated DailyWeatherRecord objects.

    Steps:
    1. Parse dates from ISO 8601 to date objects
    2. Validate temperature and precipitation
    3. Parse sunrise/sunset times
    4. Set alert flags
    5. Track discarded records

    Args:
        raw_json: Raw API response containing 'daily' key.
        city: City object with id, name, country.
        execution_id: Current pipeline execution ID.
        config: Full pipeline config dict.

    Returns:
        Tuple of (valid_records, discarded_records).
    """
    records: list[DailyWeatherRecord] = []
    discarded: list[DiscardedRecord] = []

    daily = raw_json.get("daily")
    if not daily:
        logger.warning("No daily data in response for %s", city.name)
        return records, discarded

    dates = daily.get("time", [])
    temp_maxs = daily.get("temperature_2m_max", [])
    temp_mins = daily.get("temperature_2m_min", [])
    precips = daily.get("precipitation_sum", [])
    wind_maxs = daily.get("wind_speed_10m_max", [])
    sunrises = daily.get("sunrise", [])
    sunsets = daily.get("sunset", [])

    validation_cfg = config.get("validation", {})
    thresholds = config.get("thresholds", {})
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for i, date_raw in enumerate(dates):
        try:
            dt = _parse_timestamp(date_raw)
            record_date = dt.date() if isinstance(dt, datetime) else dt
        except ValueError as exc:
            logger.error("Failed to parse date '%s': %s", date_raw, exc)
            continue

        date_str = record_date.strftime("%Y-%m-%d")

        # Safely get values
        raw_tmax = temp_maxs[i] if i < len(temp_maxs) else None
        raw_tmin = temp_mins[i] if i < len(temp_mins) else None
        raw_precip = precips[i] if i < len(precips) else None
        raw_wind_max = wind_maxs[i] if i < len(wind_maxs) else None
        raw_sunrise = sunrises[i] if i < len(sunrises) else None
        raw_sunset = sunsets[i] if i < len(sunsets) else None

        # Validate
        tmax = _validate_temperature(
            raw_tmax, city.city_id, date_str, "temperature_max",
            execution_id, discarded, validation_cfg,
        )
        tmin = _validate_temperature(
            raw_tmin, city.city_id, date_str, "temperature_min",
            execution_id, discarded, validation_cfg,
        )
        precip = _validate_precipitation(
            raw_precip, city.city_id, date_str, execution_id, discarded,
        )

        # Parse sunrise/sunset
        sunrise_time = _parse_time_from_iso(raw_sunrise)
        sunset_time = _parse_time_from_iso(raw_sunset)

        # Alert flags
        rain_alert = (
            precip is not None
            and precip > thresholds.get("heavy_rain_mm", 50.0)
        )
        wind_alert = (
            raw_wind_max is not None
            and raw_wind_max > thresholds.get("high_wind_kmh", 60.0)
        )

        records.append(DailyWeatherRecord(
            city_id=city.city_id,
            date=record_date,
            temperature_max=tmax,
            temperature_min=tmin,
            precipitation_sum=precip,
            wind_speed_max=raw_wind_max,
            sunrise=sunrise_time,
            sunset=sunset_time,
            heavy_rain_alert=rain_alert,
            high_wind_alert=wind_alert,
            ingested_at=now,
        ))

    logger.info(
        "Transformed %d daily records for %s (%d discarded)",
        len(records), city.name, len(discarded),
    )
    return records, discarded
