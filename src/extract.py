"""
extract.py — Data extraction from the Open-Meteo API.

Handles:
- Forecast API calls (7-day hourly + daily data)
- Historical API calls (last N days, daily only)
- Rate limiting (~600 req/min → 0.15s pause between requests)
- Exponential backoff retry (1s, 2s, 4s — max 3 attempts)
- Per-request logging (city, endpoint, status, response time)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Raised when the API returns a non-recoverable error."""
    pass


def _request_with_retry(
    url: str,
    params: dict[str, Any],
    max_retries: int = 3,
    backoff_base: float = 1.0,
    rate_limit_pause: float = 0.15,
    city_name: str = "",
    endpoint_label: str = "",
) -> dict[str, Any]:
    """
    Execute an HTTP GET with retry and exponential backoff.

    Args:
        url: Base API URL.
        params: Query parameters.
        max_retries: Maximum number of retry attempts.
        backoff_base: Base delay in seconds (doubled on each retry).
        rate_limit_pause: Pause between requests for rate limiting.
        city_name: City name for logging.
        endpoint_label: Endpoint label for logging (forecast/historical).

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        APIError: If all retries are exhausted.
    """
    # Rate limiting: pause before each request
    time.sleep(rate_limit_pause)

    last_exception: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        start_ts = time.monotonic()
        try:
            response = requests.get(url, params=params, timeout=30)
            elapsed = round(time.monotonic() - start_ts, 3)

            logger.info(
                "REQUEST | city=%s | endpoint=%s | status=%d | time=%.3fs | attempt=%d/%d",
                city_name, endpoint_label, response.status_code, elapsed, attempt, max_retries,
            )

            if response.status_code == 200:
                return response.json()

            # Client errors (4xx) are not retryable
            if 400 <= response.status_code < 500:
                error_msg = response.text[:200]
                logger.error(
                    "CLIENT ERROR | city=%s | status=%d | body=%s",
                    city_name, response.status_code, error_msg,
                )
                raise APIError(
                    f"Client error {response.status_code} for {city_name}: {error_msg}"
                )

            # Server errors (5xx) are retryable
            last_exception = APIError(
                f"Server error {response.status_code} for {city_name}"
            )

        except requests.exceptions.Timeout as exc:
            elapsed = round(time.monotonic() - start_ts, 3)
            logger.warning(
                "TIMEOUT | city=%s | endpoint=%s | time=%.3fs | attempt=%d/%d",
                city_name, endpoint_label, elapsed, attempt, max_retries,
            )
            last_exception = exc

        except requests.exceptions.ConnectionError as exc:
            elapsed = round(time.monotonic() - start_ts, 3)
            logger.warning(
                "CONNECTION ERROR | city=%s | endpoint=%s | time=%.3fs | attempt=%d/%d",
                city_name, endpoint_label, elapsed, attempt, max_retries,
            )
            last_exception = exc

        # Exponential backoff: 1s, 2s, 4s
        if attempt < max_retries:
            wait = backoff_base * (2 ** (attempt - 1))
            logger.info("Retrying in %.1fs...", wait)
            time.sleep(wait)

    # All retries exhausted
    logger.error(
        "ALL RETRIES EXHAUSTED | city=%s | endpoint=%s | attempts=%d",
        city_name, endpoint_label, max_retries,
    )
    raise APIError(
        f"Failed to fetch data for {city_name} ({endpoint_label}) "
        f"after {max_retries} attempts: {last_exception}"
    )


def extract_forecast(
    latitude: float,
    longitude: float,
    city_name: str,
    api_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Extract 7-day forecast data (hourly + daily) for a single city.

    Args:
        latitude: City latitude.
        longitude: City longitude.
        city_name: City name for logging.
        api_config: API configuration from config.yaml.

    Returns:
        Raw JSON response from the Forecast API.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": api_config["hourly_variables"],
        "daily": api_config["daily_variables"],
        "timezone": api_config["timezone"],
    }

    logger.info("Extracting forecast for %s (%.4f, %.4f)", city_name, latitude, longitude)

    return _request_with_retry(
        url=api_config["forecast_url"],
        params=params,
        max_retries=api_config.get("max_retries", 3),
        backoff_base=api_config.get("backoff_base_seconds", 1.0),
        rate_limit_pause=api_config.get("rate_limit_pause_seconds", 0.15),
        city_name=city_name,
        endpoint_label="forecast",
    )


def extract_historical(
    latitude: float,
    longitude: float,
    city_name: str,
    api_config: dict[str, Any],
    historical_days: int = 90,
) -> dict[str, Any]:
    """
    Extract historical daily data for the last N days for a single city.

    The Historical API only returns daily data (no hourly for free tier).

    Args:
        latitude: City latitude.
        longitude: City longitude.
        city_name: City name for logging.
        api_config: API configuration from config.yaml.
        historical_days: Number of past days to fetch.

    Returns:
        Raw JSON response from the Historical API.
    """
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=historical_days)

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "daily": api_config["daily_variables"],
        "timezone": api_config["timezone"],
    }

    logger.info(
        "Extracting historical for %s (%.4f, %.4f) | range=%s to %s",
        city_name, latitude, longitude, start_date, end_date,
    )

    return _request_with_retry(
        url=api_config["historical_url"],
        params=params,
        max_retries=api_config.get("max_retries", 3),
        backoff_base=api_config.get("backoff_base_seconds", 1.0),
        rate_limit_pause=api_config.get("rate_limit_pause_seconds", 0.15),
        city_name=city_name,
        endpoint_label="historical",
    )
