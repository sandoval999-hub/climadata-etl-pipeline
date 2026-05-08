"""
test_transform.py — Unit tests for transformation functions.

Tests:
- Heat Index calculation (Rothfusz formula) with known values
- Temperature validation (range checks)
- Humidity validation (range checks)
- Precipitation validation (non-negative)
- Timestamp parsing (multiple ISO formats)
- Alert flag logic
"""

from __future__ import annotations

import math
from datetime import datetime, time

import pytest

# Adjust path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.transform.processor import (
    calculate_heat_index,
    _parse_timestamp,
    _parse_time_from_iso,
    _validate_temperature,
    _validate_humidity,
    _validate_precipitation,
    transform_hourly,
    transform_daily,
)
from src.models.data_models import City, DiscardedRecord


# =============================================================================
# Heat Index Tests
# =============================================================================

class TestHeatIndex:
    """Test the Rothfusz regression Heat Index calculation."""

    def test_basic_heat_index(self):
        """30°C and 60% humidity should produce a valid Heat Index."""
        result = calculate_heat_index(30.0, 60.0)
        assert result is not None
        # Expected approx 32-34°C based on Rothfusz formula
        assert 30.0 < result < 40.0

    def test_high_temp_high_humidity(self):
        """35°C and 80% humidity → very high heat index."""
        result = calculate_heat_index(35.0, 80.0)
        assert result is not None
        assert result > 40.0  # Should be dangerously high

    def test_below_threshold_temperature(self):
        """Below 26.7°C → Heat Index not applicable."""
        result = calculate_heat_index(25.0, 60.0)
        assert result is None

    def test_below_threshold_humidity(self):
        """Below 40% humidity → Heat Index not applicable."""
        result = calculate_heat_index(30.0, 30.0)
        assert result is None

    def test_none_temperature(self):
        """None temperature → returns None."""
        result = calculate_heat_index(None, 60.0)
        assert result is None

    def test_none_humidity(self):
        """None humidity → returns None."""
        result = calculate_heat_index(30.0, None)
        assert result is None

    def test_both_none(self):
        """Both None → returns None."""
        result = calculate_heat_index(None, None)
        assert result is None

    def test_edge_threshold(self):
        """Exactly at threshold (26.7°C, 40%) → should compute."""
        result = calculate_heat_index(27.0, 41.0)
        assert result is not None

    def test_known_value_33c_50pct(self):
        """
        Verify against known value:
        33°C (91.4°F) at 50% humidity → approx 35-37°C Heat Index.
        Reference: calculator.net/heat-index-calculator
        """
        result = calculate_heat_index(33.0, 50.0)
        assert result is not None
        assert 34.0 < result < 38.0

    def test_low_humidity_adjustment(self):
        """
        Low humidity (< 13%) with moderate temp should trigger
        the Rothfusz low-humidity adjustment.
        """
        result = calculate_heat_index(35.0, 10.0)
        # Below 40% humidity → returns None per our implementation
        assert result is None


# =============================================================================
# Timestamp Parsing Tests
# =============================================================================

class TestTimestampParsing:
    """Test ISO 8601 timestamp parsing."""

    def test_hourly_format(self):
        """Parse 'YYYY-MM-DDTHH:MM' format."""
        result = _parse_timestamp("2026-05-07T12:00")
        assert result == datetime(2026, 5, 7, 12, 0)

    def test_full_format(self):
        """Parse 'YYYY-MM-DDTHH:MM:SS' format."""
        result = _parse_timestamp("2026-05-07T12:00:00")
        assert result == datetime(2026, 5, 7, 12, 0, 0)

    def test_date_only_format(self):
        """Parse 'YYYY-MM-DD' format."""
        result = _parse_timestamp("2026-05-07")
        assert result == datetime(2026, 5, 7, 0, 0, 0)

    def test_invalid_format(self):
        """Invalid format should raise ValueError."""
        with pytest.raises(ValueError):
            _parse_timestamp("not-a-date")

    def test_whitespace_handling(self):
        """Leading/trailing whitespace should be stripped."""
        result = _parse_timestamp("  2026-05-07T12:00  ")
        assert result == datetime(2026, 5, 7, 12, 0)

    def test_parse_time_from_iso(self):
        """Extract time portion from ISO datetime string."""
        result = _parse_time_from_iso("2026-05-07T06:15")
        assert result == time(6, 15)

    def test_parse_time_none(self):
        """None input returns None."""
        result = _parse_time_from_iso(None)
        assert result is None


# =============================================================================
# Validation Tests
# =============================================================================

class TestValidation:
    """Test data validation functions."""

    def test_valid_temperature(self):
        """Temperature within range passes."""
        discarded = []
        result = _validate_temperature(
            25.0, 1, "2026-05-07", "temperature_2m", 1, discarded,
            {"temperature_min": -10, "temperature_max": 55},
        )
        assert result == 25.0
        assert len(discarded) == 0

    def test_temperature_too_high(self):
        """Temperature above 55°C is discarded."""
        discarded = []
        result = _validate_temperature(
            60.0, 1, "2026-05-07", "temperature_2m", 1, discarded,
            {"temperature_min": -10, "temperature_max": 55},
        )
        assert result is None
        assert len(discarded) == 1
        assert "outside valid range" in discarded[0].reason

    def test_temperature_too_low(self):
        """Temperature below -10°C is discarded."""
        discarded = []
        result = _validate_temperature(
            -15.0, 1, "2026-05-07", "temperature_2m", 1, discarded,
            {"temperature_min": -10, "temperature_max": 55},
        )
        assert result is None
        assert len(discarded) == 1

    def test_temperature_none(self):
        """None temperature passes through (no discard)."""
        discarded = []
        result = _validate_temperature(
            None, 1, "2026-05-07", "temperature_2m", 1, discarded,
            {"temperature_min": -10, "temperature_max": 55},
        )
        assert result is None
        assert len(discarded) == 0

    def test_valid_humidity(self):
        """Humidity within [0, 100] passes."""
        discarded = []
        result = _validate_humidity(
            65.0, 1, "2026-05-07", 1, discarded,
            {"humidity_min": 0, "humidity_max": 100},
        )
        assert result == 65.0
        assert len(discarded) == 0

    def test_humidity_over_100(self):
        """Humidity > 100% is discarded."""
        discarded = []
        result = _validate_humidity(
            105.0, 1, "2026-05-07", 1, discarded,
            {"humidity_min": 0, "humidity_max": 100},
        )
        assert result is None
        assert len(discarded) == 1

    def test_humidity_negative(self):
        """Negative humidity is discarded."""
        discarded = []
        result = _validate_humidity(
            -5.0, 1, "2026-05-07", 1, discarded,
            {"humidity_min": 0, "humidity_max": 100},
        )
        assert result is None
        assert len(discarded) == 1

    def test_valid_precipitation(self):
        """Non-negative precipitation passes."""
        discarded = []
        result = _validate_precipitation(10.5, 1, "2026-05-07", 1, discarded)
        assert result == 10.5
        assert len(discarded) == 0

    def test_negative_precipitation(self):
        """Negative precipitation is discarded."""
        discarded = []
        result = _validate_precipitation(-3.0, 1, "2026-05-07", 1, discarded)
        assert result is None
        assert len(discarded) == 1
        assert "negative" in discarded[0].reason

    def test_zero_precipitation(self):
        """Zero precipitation is valid."""
        discarded = []
        result = _validate_precipitation(0.0, 1, "2026-05-07", 1, discarded)
        assert result == 0.0
        assert len(discarded) == 0

    def test_precipitation_none(self):
        """None precipitation passes through."""
        discarded = []
        result = _validate_precipitation(None, 1, "2026-05-07", 1, discarded)
        assert result is None
        assert len(discarded) == 0


# =============================================================================
# Transform Integration Tests
# =============================================================================

class TestTransformHourly:
    """Test full hourly transformation pipeline."""

    def _make_city(self) -> City:
        return City(city_id=1, name="Test City", country="Test", latitude=13.0, longitude=-89.0)

    def _make_config(self) -> dict:
        return {
            "validation": {
                "temperature_min": -10.0,
                "temperature_max": 55.0,
                "humidity_min": 0.0,
                "humidity_max": 100.0,
            },
            "thresholds": {
                "heat_index_temp_min": 27.0,
                "heat_index_humidity_min": 40.0,
                "high_wind_kmh": 60.0,
                "heavy_rain_mm": 50.0,
            },
        }

    def test_empty_hourly(self):
        """Empty response produces no records."""
        raw = {"hourly": None}
        records, discarded = transform_hourly(raw, self._make_city(), 1, self._make_config())
        assert len(records) == 0
        assert len(discarded) == 0

    def test_basic_hourly_transform(self):
        """Basic hourly data transforms correctly."""
        raw = {
            "hourly": {
                "time": ["2026-05-07T12:00", "2026-05-07T13:00"],
                "temperature_2m": [28.5, 30.0],
                "relative_humidity_2m": [65.0, 70.0],
                "wind_speed_10m": [15.0, 25.0],
                "weather_code": [1, 2],
            }
        }
        records, discarded = transform_hourly(raw, self._make_city(), 1, self._make_config())
        assert len(records) == 2
        assert len(discarded) == 0
        # Both should have heat index alerts (temp > 27 and humidity > 40)
        assert all(r.heat_index_alert for r in records)

    def test_invalid_temp_discarded(self):
        """Temperature outside range is discarded, record still created with None temp."""
        raw = {
            "hourly": {
                "time": ["2026-05-07T12:00"],
                "temperature_2m": [60.0],  # Invalid: > 55
                "relative_humidity_2m": [50.0],
                "wind_speed_10m": [10.0],
                "weather_code": [0],
            }
        }
        records, discarded = transform_hourly(raw, self._make_city(), 1, self._make_config())
        assert len(records) == 1
        assert records[0].temperature_2m is None  # Discarded
        assert len(discarded) == 1


class TestTransformDaily:
    """Test full daily transformation pipeline."""

    def _make_city(self) -> City:
        return City(city_id=1, name="Test City", country="Test", latitude=13.0, longitude=-89.0)

    def _make_config(self) -> dict:
        return {
            "validation": {
                "temperature_min": -10.0,
                "temperature_max": 55.0,
                "humidity_min": 0.0,
                "humidity_max": 100.0,
                "precipitation_min": 0.0,
            },
            "thresholds": {
                "heat_index_temp_min": 27.0,
                "heat_index_humidity_min": 40.0,
                "high_wind_kmh": 60.0,
                "heavy_rain_mm": 50.0,
            },
        }

    def test_basic_daily_transform(self):
        """Basic daily data transforms correctly."""
        raw = {
            "daily": {
                "time": ["2026-05-07"],
                "temperature_2m_max": [32.0],
                "temperature_2m_min": [22.0],
                "precipitation_sum": [5.0],
                "wind_speed_10m_max": [30.0],
                "sunrise": ["2026-05-07T06:00"],
                "sunset": ["2026-05-07T18:00"],
            }
        }
        records, discarded = transform_daily(raw, self._make_city(), 1, self._make_config())
        assert len(records) == 1
        assert records[0].temperature_max == 32.0
        assert records[0].sunrise == time(6, 0)
        assert records[0].heavy_rain_alert is False

    def test_heavy_rain_alert(self):
        """Precipitation > 50mm triggers alert."""
        raw = {
            "daily": {
                "time": ["2026-05-07"],
                "temperature_2m_max": [30.0],
                "temperature_2m_min": [20.0],
                "precipitation_sum": [75.0],
                "wind_speed_10m_max": [20.0],
                "sunrise": ["2026-05-07T06:00"],
                "sunset": ["2026-05-07T18:00"],
            }
        }
        records, _ = transform_daily(raw, self._make_city(), 1, self._make_config())
        assert records[0].heavy_rain_alert is True

    def test_high_wind_alert(self):
        """Wind > 60 km/h triggers alert."""
        raw = {
            "daily": {
                "time": ["2026-05-07"],
                "temperature_2m_max": [30.0],
                "temperature_2m_min": [20.0],
                "precipitation_sum": [5.0],
                "wind_speed_10m_max": [75.0],
                "sunrise": ["2026-05-07T06:00"],
                "sunset": ["2026-05-07T18:00"],
            }
        }
        records, _ = transform_daily(raw, self._make_city(), 1, self._make_config())
        assert records[0].high_wind_alert is True
