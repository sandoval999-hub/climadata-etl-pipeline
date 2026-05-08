"""
test_load.py — Unit tests for loading functions.

Uses unittest.mock to simulate MySQL connections without a real database.

Tests:
- DatabaseConnection configuration parsing
- UPSERT SQL generation (parameterized, not concatenated)
- Batch processing logic
- Execution log lifecycle (start → finish)
- Discarded data insertion
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from datetime import datetime, date, time, timezone

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.load.mysql import DatabaseConnection
from src.models.data_models import (
    HourlyWeatherRecord,
    DailyWeatherRecord,
    DiscardedRecord,
    ExecutionLog,
)


class TestDatabaseConnectionConfig:
    """Test database connection configuration."""

    def test_config_parsing(self):
        """Environment variables are correctly parsed into config."""
        env = {
            "MYSQL_HOST": "localhost",
            "MYSQL_PORT": "3306",
            "MYSQL_USER": "testuser",
            "MYSQL_PASSWORD": "testpass",
            "MYSQL_DATABASE": "testdb",
        }
        db = DatabaseConnection(env)
        assert db._config["host"] == "localhost"
        assert db._config["port"] == 3306
        assert db._config["user"] == "testuser"
        assert db._config["database"] == "testdb"
        assert db._config["charset"] == "utf8mb4"
        assert db._config["autocommit"] is False

    def test_missing_env_var_raises(self):
        """Missing required env var raises KeyError."""
        env = {"MYSQL_HOST": "localhost"}
        with pytest.raises(KeyError):
            DatabaseConnection(env)


class TestUpsertHourlyBatch:
    """Test hourly UPSERT batch logic."""

    def _make_records(self, count: int) -> list[HourlyWeatherRecord]:
        return [
            HourlyWeatherRecord(
                city_id=1,
                timestamp=datetime(2026, 5, 7 + (i // 24), i % 24, 0),
                temperature_2m=25.0 + (i % 10),
                relative_humidity=60.0,
                wind_speed_10m=15.0,
                weather_code=1,
                heat_index=None,
                heat_index_alert=False,
                high_wind_alert=False,
            )
            for i in range(count)
        ]

    @patch("src.load.mysql.mysql.connector.connect")
    def test_empty_records_returns_zero(self, mock_connect):
        """Empty record list returns 0 without any DB call."""
        env = {
            "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
            "MYSQL_USER": "root", "MYSQL_PASSWORD": "", "MYSQL_DATABASE": "test",
        }
        db = DatabaseConnection(env)
        db._conn = MagicMock()
        result = db.upsert_hourly_batch([])
        assert result == 0

    @patch("src.load.mysql.mysql.connector.connect")
    def test_batch_splitting(self, mock_connect):
        """250 records should produce 3 batches with batch_size=100."""
        env = {
            "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
            "MYSQL_USER": "root", "MYSQL_PASSWORD": "", "MYSQL_DATABASE": "test",
        }
        db = DatabaseConnection(env)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        db._conn = mock_conn

        records = self._make_records(250)
        db.upsert_hourly_batch(records, batch_size=100)

        # executemany should be called 3 times (100 + 100 + 50)
        assert mock_cursor.executemany.call_count == 3

    @patch("src.load.mysql.mysql.connector.connect")
    def test_upsert_sql_is_parameterized(self, mock_connect):
        """SQL uses %s placeholders, never string concatenation."""
        env = {
            "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
            "MYSQL_USER": "root", "MYSQL_PASSWORD": "", "MYSQL_DATABASE": "test",
        }
        db = DatabaseConnection(env)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        db._conn = mock_conn

        records = self._make_records(1)
        db.upsert_hourly_batch(records)

        sql_used = mock_cursor.executemany.call_args[0][0]
        assert "%s" in sql_used
        assert "ON DUPLICATE KEY UPDATE" in sql_used
        # Verify no f-string or format usage (no actual values in SQL)
        assert "25.0" not in sql_used
        assert "2026" not in sql_used


class TestUpsertDailyBatch:
    """Test daily UPSERT batch logic."""

    def _make_records(self, count: int) -> list[DailyWeatherRecord]:
        return [
            DailyWeatherRecord(
                city_id=1,
                date=date(2026, 5, i + 1),
                temperature_max=32.0,
                temperature_min=22.0,
                precipitation_sum=5.0,
                wind_speed_max=30.0,
                sunrise=time(6, 0),
                sunset=time(18, 0),
                heavy_rain_alert=False,
                high_wind_alert=False,
            )
            for i in range(count)
        ]

    @patch("src.load.mysql.mysql.connector.connect")
    def test_daily_upsert_uses_correct_sql(self, mock_connect):
        """Daily UPSERT SQL contains correct table and ON DUPLICATE KEY."""
        env = {
            "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
            "MYSQL_USER": "root", "MYSQL_PASSWORD": "", "MYSQL_DATABASE": "test",
        }
        db = DatabaseConnection(env)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        db._conn = mock_conn

        records = self._make_records(1)
        db.upsert_daily_batch(records)

        sql_used = mock_cursor.executemany.call_args[0][0]
        assert "fact_daily_weather" in sql_used
        assert "ON DUPLICATE KEY UPDATE" in sql_used
        assert "%s" in sql_used


class TestExecutionLog:
    """Test execution logging lifecycle."""

    @patch("src.load.mysql.mysql.connector.connect")
    def test_start_execution_returns_id(self, mock_connect):
        """start_execution inserts a log and returns the execution_id."""
        env = {
            "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
            "MYSQL_USER": "root", "MYSQL_PASSWORD": "", "MYSQL_DATABASE": "test",
        }
        db = DatabaseConnection(env)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 42
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        db._conn = mock_conn

        execution_id = db.start_execution("forecast")

        assert execution_id == 42
        sql_used = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO log_executions" in sql_used
        assert "%s" in sql_used

    @patch("src.load.mysql.mysql.connector.connect")
    def test_finish_execution_updates_record(self, mock_connect):
        """finish_execution updates the execution log with results."""
        env = {
            "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
            "MYSQL_USER": "root", "MYSQL_PASSWORD": "", "MYSQL_DATABASE": "test",
        }
        db = DatabaseConnection(env)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        db._conn = mock_conn

        execution = ExecutionLog(
            execution_id=42,
            mode="forecast",
            start_time=datetime.now(timezone.utc),
            cities_success=10,
            cities_failed=0,
            rows_inserted=1750,
            errors_count=0,
            status="completed",
        )
        db.finish_execution(execution)

        sql_used = mock_cursor.execute.call_args[0][0]
        assert "UPDATE log_executions" in sql_used


class TestDiscardedBatch:
    """Test discarded data logging."""

    @patch("src.load.mysql.mysql.connector.connect")
    def test_insert_discarded_records(self, mock_connect):
        """Discarded records are inserted with correct SQL."""
        env = {
            "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
            "MYSQL_USER": "root", "MYSQL_PASSWORD": "", "MYSQL_DATABASE": "test",
        }
        db = DatabaseConnection(env)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_connected.return_value = True
        db._conn = mock_conn

        records = [
            DiscardedRecord(
                execution_id=1,
                city_id=1,
                record_timestamp="2026-05-07",
                variable_name="temperature_2m",
                invalid_value="60.0",
                reason="Temperature 60.0°C outside valid range",
            )
        ]
        db.insert_discarded_batch(records)

        sql_used = mock_cursor.executemany.call_args[0][0]
        assert "log_discarded_data" in sql_used
        assert "%s" in sql_used

    @patch("src.load.mysql.mysql.connector.connect")
    def test_empty_discarded_skips_insert(self, mock_connect):
        """Empty discarded list does nothing."""
        env = {
            "MYSQL_HOST": "localhost", "MYSQL_PORT": "3306",
            "MYSQL_USER": "root", "MYSQL_PASSWORD": "", "MYSQL_DATABASE": "test",
        }
        db = DatabaseConnection(env)
        db._conn = MagicMock()
        db.insert_discarded_batch([])
        # No cursor should be created
