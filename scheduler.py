"""
scheduler.py — Automated pipeline scheduler.

Runs the forecast pipeline every 6 hours using the `schedule` library.

Usage:
    python scheduler.py
    (Runs indefinitely, executing forecast every 6 hours)
"""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import schedule

logger = logging.getLogger("scheduler")

PIPELINE_SCRIPT = str(Path(__file__).resolve().parent / "main.py")


def run_forecast_job() -> None:
    """Execute the forecast pipeline as a subprocess."""
    logger.info("=" * 50)
    logger.info("Scheduled job triggered at %s", datetime.utcnow().isoformat())
    logger.info("Running: python main.py --mode forecast")

    try:
        result = subprocess.run(
            [sys.executable, PIPELINE_SCRIPT, "--mode", "forecast"],
            capture_output=True,
            text=True,
            timeout=600,  # 10-minute timeout
        )

        if result.returncode == 0:
            logger.info("Job completed successfully")
        else:
            logger.error("Job failed with exit code %d", result.returncode)
            if result.stderr:
                logger.error("stderr: %s", result.stderr[-500:])

    except subprocess.TimeoutExpired:
        logger.error("Job timed out after 600 seconds")
    except Exception as exc:
        logger.error("Job failed with exception: %s", exc, exc_info=True)


def main() -> None:
    """Set up and run the scheduler."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("ClimaData ETL Scheduler started")
    logger.info("Schedule: forecast pipeline every 6 hours")

    # Schedule the job every 6 hours
    schedule.every(6).hours.do(run_forecast_job)

    # Run once immediately on startup
    logger.info("Running initial execution...")
    run_forecast_job()

    # Keep running
    logger.info("Scheduler running. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")


if __name__ == "__main__":
    main()
