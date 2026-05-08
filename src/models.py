"""
models.py — Data classes and type definitions for the ETL pipeline.

Provides structured representations for cities, weather records,
and execution metadata. All fields use type hints for clarity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date, time, timezone
from typing import Optional


@dataclass
class City:
    """Represents a target city loaded from config or database."""
    city_id: int
    name: str
    country: str
    latitude: float
    longitude: float


@dataclass
class HourlyWeatherRecord:
    """A single hourly weather observation for one city."""
    city_id: int
    timestamp: datetime
    temperature_2m: Optional[float] = None
    relative_humidity: Optional[float] = None
    wind_speed_10m: Optional[float] = None
    weather_code: Optional[int] = None
    heat_index: Optional[float] = None
    heat_index_alert: bool = False
    high_wind_alert: bool = False
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

    def to_tuple(self) -> tuple:
        """Convert to a tuple matching the INSERT column order."""
        return (
            self.city_id,
            self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            self.temperature_2m,
            self.relative_humidity,
            self.wind_speed_10m,
            self.weather_code,
            self.heat_index,
            int(self.heat_index_alert),
            int(self.high_wind_alert),
            self.ingested_at.strftime("%Y-%m-%d %H:%M:%S"),
        )


@dataclass
class DailyWeatherRecord:
    """A single daily weather aggregate for one city."""
    city_id: int
    date: date
    temperature_max: Optional[float] = None
    temperature_min: Optional[float] = None
    precipitation_sum: Optional[float] = None
    wind_speed_max: Optional[float] = None
    sunrise: Optional[time] = None
    sunset: Optional[time] = None
    heavy_rain_alert: bool = False
    high_wind_alert: bool = False
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

    def to_tuple(self) -> tuple:
        """Convert to a tuple matching the INSERT column order."""
        sunrise_str = self.sunrise.strftime("%H:%M:%S") if self.sunrise else None
        sunset_str = self.sunset.strftime("%H:%M:%S") if self.sunset else None
        return (
            self.city_id,
            self.date.strftime("%Y-%m-%d"),
            self.temperature_max,
            self.temperature_min,
            self.precipitation_sum,
            self.wind_speed_max,
            sunrise_str,
            sunset_str,
            int(self.heavy_rain_alert),
            int(self.high_wind_alert),
            self.ingested_at.strftime("%Y-%m-%d %H:%M:%S"),
        )


@dataclass
class DiscardedRecord:
    """A record that failed validation and was discarded."""
    execution_id: int
    city_id: Optional[int]
    record_timestamp: Optional[str]
    variable_name: str
    invalid_value: Optional[str]
    reason: str
    discarded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

    def to_tuple(self) -> tuple:
        """Convert to a tuple matching the INSERT column order."""
        return (
            self.execution_id,
            self.city_id,
            self.record_timestamp,
            self.variable_name,
            str(self.invalid_value) if self.invalid_value is not None else None,
            self.reason,
            self.discarded_at.strftime("%Y-%m-%d %H:%M:%S"),
        )


@dataclass
class ExecutionLog:
    """Tracks a single pipeline execution."""
    execution_id: Optional[int] = None
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    mode: str = "all"
    cities_success: int = 0
    cities_failed: int = 0
    rows_inserted: int = 0
    errors_count: int = 0
    status: str = "running"
