"""
load.py — Data loading into MySQL database.

Handles:
- Database connection management
- Batch UPSERT (INSERT ... ON DUPLICATE KEY UPDATE)
- Execution log tracking
- Discarded data logging
- All queries use parameterized statements (never string concatenation)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional

import mysql.connector
from mysql.connector import Error as MySQLError

from src.models import (
    City,
    DailyWeatherRecord,
    DiscardedRecord,
    ExecutionLog,
    HourlyWeatherRecord,
)

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Context manager for MySQL database connections.

    Usage:
        with DatabaseConnection(env_vars) as db:
            db.insert_hourly_batch(records)
    """

    def __init__(self, env_vars: dict[str, str]) -> None:
        self._config = {
            "host": env_vars["MYSQL_HOST"],
            "port": int(env_vars["MYSQL_PORT"]),
            "user": env_vars["MYSQL_USER"],
            "password": env_vars["MYSQL_PASSWORD"],
            "database": env_vars["MYSQL_DATABASE"],
            "charset": "utf8mb4",
            "autocommit": False,
        }
        self._conn: Optional[mysql.connector.MySQLConnection] = None

    def __enter__(self) -> "DatabaseConnection":
        try:
            self._conn = mysql.connector.connect(**self._config)
            logger.info(
                "MySQL connected — %s@%s:%s/%s",
                self._config["user"],
                self._config["host"],
                self._config["port"],
                self._config["database"],
            )
        except MySQLError as exc:
            logger.error("MySQL connection failed: %s", exc)
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._conn and self._conn.is_connected():
            if exc_type:
                self._conn.rollback()
                logger.warning("Transaction rolled back due to exception")
            self._conn.close()
            logger.info("MySQL connection closed")

    @property
    def connection(self) -> mysql.connector.MySQLConnection:
        if not self._conn or not self._conn.is_connected():
            raise RuntimeError("Database not connected")
        return self._conn

    # ── City Operations ──────────────────────────────────────────────────

    def get_cities(self) -> list[City]:
        """Fetch all cities from dim_cities."""
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute("SELECT city_id, name, country, latitude, longitude FROM dim_cities")
        rows = cursor.fetchall()
        cursor.close()
        return [
            City(
                city_id=row["city_id"],
                name=row["name"],
                country=row["country"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
            )
            for row in rows
        ]

    # ── Execution Log Operations ─────────────────────────────────────────

    def start_execution(self, mode: str) -> int:
        """Insert a new execution log and return its ID."""
        cursor = self.connection.cursor()
        cursor.execute(
            "INSERT INTO log_executions (start_time, mode, status) VALUES (%s, %s, %s)",
            (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), mode, "running"),
        )
        self.connection.commit()
        execution_id = cursor.lastrowid
        cursor.close()
        logger.info("Execution started — id=%d, mode=%s", execution_id, mode)
        return execution_id

    def finish_execution(self, execution: ExecutionLog) -> None:
        """Update an execution log with final results."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            UPDATE log_executions
            SET end_time = %s, cities_success = %s, cities_failed = %s,
                rows_inserted = %s, errors_count = %s, status = %s
            WHERE execution_id = %s
            """,
            (
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                execution.cities_success,
                execution.cities_failed,
                execution.rows_inserted,
                execution.errors_count,
                execution.status,
                execution.execution_id,
            ),
        )
        self.connection.commit()
        cursor.close()
        logger.info(
            "Execution finished — id=%d, status=%s, inserted=%d, errors=%d",
            execution.execution_id, execution.status,
            execution.rows_inserted, execution.errors_count,
        )

    # ── Hourly Weather UPSERT ────────────────────────────────────────────

    def upsert_hourly_batch(
        self, records: list[HourlyWeatherRecord], batch_size: int = 100
    ) -> int:
        """
        Insert hourly weather records using UPSERT in batches.

        Uses INSERT ... ON DUPLICATE KEY UPDATE for idempotency.
        Processes records in chunks of `batch_size` for efficiency.

        Returns the total number of rows affected.
        """
        if not records:
            return 0

        sql = """
            INSERT INTO fact_hourly_weather
                (city_id, timestamp, temperature_2m, relative_humidity,
                 wind_speed_10m, weather_code, heat_index,
                 heat_index_alert, high_wind_alert, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                temperature_2m = VALUES(temperature_2m),
                relative_humidity = VALUES(relative_humidity),
                wind_speed_10m = VALUES(wind_speed_10m),
                weather_code = VALUES(weather_code),
                heat_index = VALUES(heat_index),
                heat_index_alert = VALUES(heat_index_alert),
                high_wind_alert = VALUES(high_wind_alert),
                ingested_at = VALUES(ingested_at)
        """

        total_affected = 0
        cursor = self.connection.cursor()

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            data = [r.to_tuple() for r in batch]
            try:
                cursor.executemany(sql, data)
                self.connection.commit()
                total_affected += cursor.rowcount
            except MySQLError as exc:
                self.connection.rollback()
                logger.error(
                    "Failed to insert hourly batch [%d:%d]: %s",
                    i, i + len(batch), exc,
                )
                raise

        cursor.close()
        logger.info("Upserted %d hourly records (%d affected)", len(records), total_affected)
        return total_affected

    # ── Daily Weather UPSERT ─────────────────────────────────────────────

    def upsert_daily_batch(
        self, records: list[DailyWeatherRecord], batch_size: int = 100
    ) -> int:
        """
        Insert daily weather records using UPSERT in batches.

        Returns the total number of rows affected.
        """
        if not records:
            return 0

        sql = """
            INSERT INTO fact_daily_weather
                (city_id, date, temperature_max, temperature_min,
                 precipitation_sum, wind_speed_max, sunrise, sunset,
                 heavy_rain_alert, high_wind_alert, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                temperature_max = VALUES(temperature_max),
                temperature_min = VALUES(temperature_min),
                precipitation_sum = VALUES(precipitation_sum),
                wind_speed_max = VALUES(wind_speed_max),
                sunrise = VALUES(sunrise),
                sunset = VALUES(sunset),
                heavy_rain_alert = VALUES(heavy_rain_alert),
                high_wind_alert = VALUES(high_wind_alert),
                ingested_at = VALUES(ingested_at)
        """

        total_affected = 0
        cursor = self.connection.cursor()

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            data = [r.to_tuple() for r in batch]
            try:
                cursor.executemany(sql, data)
                self.connection.commit()
                total_affected += cursor.rowcount
            except MySQLError as exc:
                self.connection.rollback()
                logger.error(
                    "Failed to insert daily batch [%d:%d]: %s",
                    i, i + len(batch), exc,
                )
                raise

        cursor.close()
        logger.info("Upserted %d daily records (%d affected)", len(records), total_affected)
        return total_affected

    # ── Discarded Data Logging ───────────────────────────────────────────

    def insert_discarded_batch(
        self, records: list[DiscardedRecord], batch_size: int = 100
    ) -> None:
        """Insert discarded data records for quality tracking."""
        if not records:
            return

        sql = """
            INSERT INTO log_discarded_data
                (execution_id, city_id, record_timestamp, variable_name,
                 invalid_value, reason, discarded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor = self.connection.cursor()

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            data = [r.to_tuple() for r in batch]
            try:
                cursor.executemany(sql, data)
                self.connection.commit()
            except MySQLError as exc:
                self.connection.rollback()
                logger.error("Failed to insert discarded batch: %s", exc)

        cursor.close()
        logger.info("Logged %d discarded records", len(records))

    # ── Chart Data Query ─────────────────────────────────────────────────

    def get_recent_daily_temps(self, days: int = 7) -> dict[str, list[tuple[str, float]]]:
        """
        Fetch recent daily max temperatures for chart generation.

        Returns dict: city_name -> [(date_str, temp_max), ...]
        """
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT c.name, d.date, d.temperature_max
            FROM fact_daily_weather d
            JOIN dim_cities c ON d.city_id = c.city_id
            WHERE d.date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
              AND d.temperature_max IS NOT NULL
            ORDER BY c.name, d.date
            """,
            (days,),
        )
        rows = cursor.fetchall()
        cursor.close()

        result: dict[str, list[tuple[str, float]]] = {}
        for row in rows:
            city_name = row["name"]
            date_str = row["date"].strftime("%Y-%m-%d") if isinstance(row["date"], date) else str(row["date"])
            temp = float(row["temperature_max"])
            if city_name not in result:
                result[city_name] = []
            result[city_name].append((date_str, temp))

        return result
