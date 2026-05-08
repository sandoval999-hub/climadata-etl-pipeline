"""
utils.py — Utility functions for logging, configuration, and reporting.

Handles:
- Dual logging setup (console + file)
- YAML config loading
- Environment variable loading
- Markdown report generation
- Chart generation (matplotlib)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


# ── Project Paths ────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
LOG_DIR = PROJECT_ROOT / "logs"
REPORT_DIR = PROJECT_ROOT / "data" / "reports"


def setup_logging(log_level: int = logging.INFO) -> logging.Logger:
    """
    Configure dual logging: console (INFO+) and rotating file.

    Returns the root logger. All modules should use logging.getLogger(__name__).
    Logs go to both stdout and logs/pipeline_YYYYMMDD_HHMMSS.log.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"pipeline_{timestamp}.log"

    # Clear any existing handlers on the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    # Formatter
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(fmt)
    root_logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(fmt)
    root_logger.addHandler(file_handler)

    root_logger.info("Logging initialized → %s", log_file)
    return root_logger


def load_config() -> dict[str, Any]:
    """
    Load pipeline configuration from config/config.yaml.

    Returns the parsed YAML as a dictionary.
    Raises FileNotFoundError if the config file is missing.
    """
    config_path = CONFIG_DIR / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    logging.getLogger(__name__).info("Configuration loaded from %s", config_path)
    return config


def load_env() -> dict[str, str]:
    """
    Load MySQL credentials from .env file.

    Returns a dict with keys: MYSQL_HOST, MYSQL_PORT, MYSQL_USER,
    MYSQL_PASSWORD, MYSQL_DATABASE.
    Raises EnvironmentError if required variables are missing.
    """
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=env_path)

    required_keys = ["MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_DATABASE"]
    env_vars: dict[str, str] = {}

    for key in required_keys:
        value = os.getenv(key)
        if value is None:
            raise EnvironmentError(
                f"Missing required environment variable: {key}. "
                f"Check your .env file at {env_path}"
            )
        env_vars[key] = value

    # Password can be empty (e.g., XAMPP default)
    env_vars["MYSQL_PASSWORD"] = os.getenv("MYSQL_PASSWORD", "")

    logging.getLogger(__name__).info(
        "Environment loaded — host=%s, port=%s, database=%s",
        env_vars["MYSQL_HOST"],
        env_vars["MYSQL_PORT"],
        env_vars["MYSQL_DATABASE"],
    )
    return env_vars


def generate_report(
    mode: str,
    start_time: datetime,
    end_time: datetime,
    cities_success: int,
    cities_failed: int,
    rows_inserted: int,
    errors_count: int,
    alerts_detected: dict[str, int],
    failed_cities: list[str],
) -> str:
    """
    Generate a Markdown execution report and save it to data/reports/.

    Returns the path to the generated report file.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    report_path = REPORT_DIR / f"report_{timestamp}.md"

    duration = end_time - start_time
    total_cities = cities_success + cities_failed

    lines = [
        f"# ETL Pipeline Execution Report",
        f"",
        f"**Date:** {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"**Mode:** `{mode}`",
        f"**Duration:** {duration}",
        f"",
        f"## Summary",
        f"",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Cities Processed | {cities_success}/{total_cities} |",
        f"| Cities Failed | {cities_failed} |",
        f"| Total Rows Inserted | {rows_inserted:,} |",
        f"| Total Errors | {errors_count} |",
        f"",
    ]

    if failed_cities:
        lines.append("## Failed Cities")
        lines.append("")
        for city in failed_cities:
            lines.append(f"- {city}")
        lines.append("")

    if alerts_detected:
        lines.append("## Alerts Detected")
        lines.append("")
        lines.append("| Alert Type | Count |")
        lines.append("|---|---|")
        for alert_type, count in alerts_detected.items():
            lines.append(f"| {alert_type} | {count} |")
        lines.append("")

    lines.append(f"---")
    lines.append(f"*Generated by ClimaData ETL Pipeline*")

    content = "\n".join(lines)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)

    logging.getLogger(__name__).info("Report saved to %s", report_path)
    return str(report_path)


def generate_temperature_chart(
    city_data: dict[str, list[tuple[str, float]]],
) -> str:
    """
    Generate a line chart of max temperature over the last 7 days
    for all cities. Saves as PNG.

    Args:
        city_data: dict mapping city_name -> list of (date_str, temp_max)

    Returns the path to the saved chart image.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")  # Non-interactive backend
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        logging.getLogger(__name__).warning(
            "matplotlib not installed — skipping chart generation"
        )
        return ""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(14, 7))

    for city_name, data_points in city_data.items():
        if not data_points:
            continue
        dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in data_points]
        temps = [t for _, t in data_points]
        ax.plot(dates, temps, marker="o", markersize=4, linewidth=1.5, label=city_name)

    ax.set_title("Maximum Temperature — Last 7 Days", fontsize=16, fontweight="bold")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Temperature (°C)", fontsize=12)
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    fig.autofmt_xdate()
    plt.tight_layout()

    chart_path = REPORT_DIR / f"temperature_chart_{datetime.now(timezone.utc).strftime('%Y%m%d')}.png"
    fig.savefig(str(chart_path), dpi=150)
    plt.close(fig)

    logging.getLogger(__name__).info("Temperature chart saved to %s", chart_path)
    return str(chart_path)
