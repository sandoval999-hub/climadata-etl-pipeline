"""
main.py — CLI entry point for the ClimaData ETL Pipeline.

Usage:
    python main.py --mode all         # Full pipeline (forecast + historical)
    python main.py --mode forecast    # Forecast only (daily quick run)
    python main.py --mode historical  # Historical only (backfill past data)
    python main.py --mode forecast --dry-run  # Extract & transform, no DB load
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from src.extract import extract_forecast, extract_historical, APIError
from src.load import DatabaseConnection
from src.models import City, ExecutionLog
from src.transform import transform_daily, transform_hourly
from src.utils import (
    generate_report,
    generate_temperature_chart,
    load_config,
    load_env,
    setup_logging,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="climadata-etl",
        description="ClimaData Solutions — ETL Pipeline for weather data ingestion",
    )
    parser.add_argument(
        "--mode",
        choices=["forecast", "historical", "all"],
        default="all",
        help="Pipeline mode: 'forecast' (7-day), 'historical' (90-day backfill), or 'all' (default)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform data but do NOT load into MySQL",
    )
    return parser.parse_args()


def process_city_forecast(
    city: City,
    config: dict[str, Any],
    execution_id: int,
    db: DatabaseConnection | None,
    dry_run: bool,
) -> dict[str, Any]:
    """
    Run the forecast pipeline for a single city.

    Returns a dict with counts: hourly_records, daily_records, discarded, alerts.
    """
    api_config = config["api"]
    result = {
        "hourly_records": 0,
        "daily_records": 0,
        "discarded": 0,
        "alerts": {"heat_index_alert": 0, "heavy_rain_alert": 0, "high_wind_alert": 0},
    }

    # ── Extract ──────────────────────────────────────────────────────
    raw = extract_forecast(city.latitude, city.longitude, city.name, api_config)

    # ── Transform hourly ─────────────────────────────────────────────
    hourly_records, hourly_discarded = transform_hourly(raw, city, execution_id, config)
    result["hourly_records"] = len(hourly_records)
    result["discarded"] += len(hourly_discarded)

    # Count alerts
    for r in hourly_records:
        if r.heat_index_alert:
            result["alerts"]["heat_index_alert"] += 1
        if r.high_wind_alert:
            result["alerts"]["high_wind_alert"] += 1

    # ── Transform daily ──────────────────────────────────────────────
    daily_records, daily_discarded = transform_daily(raw, city, execution_id, config)
    result["daily_records"] = len(daily_records)
    result["discarded"] += len(daily_discarded)

    for r in daily_records:
        if r.heavy_rain_alert:
            result["alerts"]["heavy_rain_alert"] += 1
        if r.high_wind_alert:
            result["alerts"]["high_wind_alert"] += 1

    # ── Load ─────────────────────────────────────────────────────────
    if not dry_run and db:
        db.upsert_hourly_batch(
            hourly_records, config.get("pipeline", {}).get("batch_size", 100)
        )
        db.upsert_daily_batch(
            daily_records, config.get("pipeline", {}).get("batch_size", 100)
        )
        all_discarded = hourly_discarded + daily_discarded
        if all_discarded:
            db.insert_discarded_batch(all_discarded)

    return result


def process_city_historical(
    city: City,
    config: dict[str, Any],
    execution_id: int,
    db: DatabaseConnection | None,
    dry_run: bool,
) -> dict[str, Any]:
    """
    Run the historical pipeline for a single city.

    Returns a dict with counts: daily_records, discarded, alerts.
    """
    api_config = config["api"]
    historical_days = config.get("pipeline", {}).get("historical_days", 90)
    result = {
        "hourly_records": 0,
        "daily_records": 0,
        "discarded": 0,
        "alerts": {"heat_index_alert": 0, "heavy_rain_alert": 0, "high_wind_alert": 0},
    }

    # ── Extract ──────────────────────────────────────────────────────
    raw = extract_historical(
        city.latitude, city.longitude, city.name, api_config, historical_days
    )

    # ── Transform daily ──────────────────────────────────────────────
    daily_records, daily_discarded = transform_daily(raw, city, execution_id, config)
    result["daily_records"] = len(daily_records)
    result["discarded"] += len(daily_discarded)

    for r in daily_records:
        if r.heavy_rain_alert:
            result["alerts"]["heavy_rain_alert"] += 1
        if r.high_wind_alert:
            result["alerts"]["high_wind_alert"] += 1

    # ── Load ─────────────────────────────────────────────────────────
    if not dry_run and db:
        db.upsert_daily_batch(
            daily_records, config.get("pipeline", {}).get("batch_size", 100)
        )
        if daily_discarded:
            db.insert_discarded_batch(daily_discarded)

    return result


def run_pipeline(args: argparse.Namespace) -> None:
    """
    Main pipeline orchestration.

    - Loads configuration and environment
    - Connects to MySQL (unless dry-run)
    - Iterates over all cities with per-city error handling
    - Generates execution report and temperature chart
    """
    start_time = datetime.now(timezone.utc)
    config = load_config()
    env_vars = load_env()

    # Counters
    execution = ExecutionLog(mode=args.mode, start_time=start_time)
    total_alerts: dict[str, int] = {
        "heat_index_alert": 0,
        "heavy_rain_alert": 0,
        "high_wind_alert": 0,
    }
    failed_cities: list[str] = []

    # ── Database Connection ──────────────────────────────────────────
    db: DatabaseConnection | None = None
    if not args.dry_run:
        db = DatabaseConnection(env_vars)
        db.__enter__()

    try:
        # Get cities from database if connected, otherwise from config
        if db:
            cities = db.get_cities()
            execution_id = db.start_execution(args.mode)
            execution.execution_id = execution_id
        else:
            # Dry-run mode: build cities from config (no DB)
            cities = [
                City(
                    city_id=i + 1,
                    name=c["name"],
                    country=c["country"],
                    latitude=c["latitude"],
                    longitude=c["longitude"],
                )
                for i, c in enumerate(config["cities"])
            ]
            execution_id = 0

        logger.info(
            "Pipeline starting — mode=%s, dry_run=%s, cities=%d",
            args.mode, args.dry_run, len(cities),
        )

        # ── Process Each City ────────────────────────────────────────
        for city in cities:
            logger.info("=" * 60)
            logger.info("Processing city: %s (%s)", city.name, city.country)
            try:
                city_rows = 0

                if args.mode in ("forecast", "all"):
                    result = process_city_forecast(
                        city, config, execution_id, db, args.dry_run
                    )
                    city_rows += result["hourly_records"] + result["daily_records"]
                    execution.rows_inserted += result["hourly_records"] + result["daily_records"]
                    execution.errors_count += result["discarded"]
                    for k, v in result["alerts"].items():
                        total_alerts[k] += v

                if args.mode in ("historical", "all"):
                    result = process_city_historical(
                        city, config, execution_id, db, args.dry_run
                    )
                    city_rows += result["daily_records"]
                    execution.rows_inserted += result["daily_records"]
                    execution.errors_count += result["discarded"]
                    for k, v in result["alerts"].items():
                        total_alerts[k] += v

                execution.cities_success += 1
                logger.info(
                    "✓ %s completed — %d records processed", city.name, city_rows
                )

            except APIError as exc:
                execution.cities_failed += 1
                failed_cities.append(city.name)
                logger.error("✗ %s FAILED (API): %s", city.name, exc)

            except Exception as exc:
                execution.cities_failed += 1
                failed_cities.append(city.name)
                logger.error(
                    "✗ %s FAILED (unexpected): %s", city.name, exc, exc_info=True
                )

        # ── Finalize ─────────────────────────────────────────────────
        execution.status = "completed" if execution.cities_failed == 0 else "completed_with_errors"
        end_time = datetime.now(timezone.utc)
        execution.end_time = end_time

        if db:
            db.finish_execution(execution)

        # ── Summary ──────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("  Mode:            %s", args.mode)
        logger.info("  Dry Run:         %s", args.dry_run)
        logger.info("  Cities OK:       %d", execution.cities_success)
        logger.info("  Cities Failed:   %d", execution.cities_failed)
        logger.info("  Rows Inserted:   %d", execution.rows_inserted)
        logger.info("  Data Discarded:  %d", execution.errors_count)
        logger.info("  Duration:        %s", end_time - start_time)
        if failed_cities:
            logger.info("  Failed Cities:   %s", ", ".join(failed_cities))
        logger.info("=" * 60)

        # ── Generate Report ──────────────────────────────────────────
        report_path = generate_report(
            mode=args.mode,
            start_time=start_time,
            end_time=end_time,
            cities_success=execution.cities_success,
            cities_failed=execution.cities_failed,
            rows_inserted=execution.rows_inserted,
            errors_count=execution.errors_count,
            alerts_detected=total_alerts,
            failed_cities=failed_cities,
        )
        logger.info("Report: %s", report_path)

        # ── Generate Chart ───────────────────────────────────────────
        if db and not args.dry_run:
            try:
                chart_data = db.get_recent_daily_temps(days=7)
                if chart_data:
                    chart_path = generate_temperature_chart(chart_data)
                    if chart_path:
                        logger.info("Chart: %s", chart_path)
            except Exception as exc:
                logger.warning("Chart generation failed: %s", exc)

    finally:
        if db:
            db.__exit__(None, None, None)


def main() -> None:
    """Entry point."""
    setup_logging()
    args = parse_args()

    logger.info("ClimaData ETL Pipeline v1.0")
    logger.info("Arguments: mode=%s, dry_run=%s", args.mode, args.dry_run)

    try:
        run_pipeline(args)
    except Exception as exc:
        logger.critical("Pipeline crashed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
