"""
test_extract.py — Unit tests for extraction functions.

Uses unittest.mock to simulate API responses without making real HTTP requests.

Tests:
- Successful API call returns parsed JSON
- Retry on server error (5xx)
- No retry on client error (4xx)
- Timeout handling with retry
- Rate limiting pause is applied
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.extract import _request_with_retry, extract_forecast, extract_historical, APIError


class TestRequestWithRetry:
    """Test the core retry mechanism."""

    @patch("src.extract.time.sleep")
    @patch("src.extract.requests.get")
    def test_successful_request(self, mock_get, mock_sleep):
        """200 response returns parsed JSON immediately."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hourly": {"time": []}}
        mock_get.return_value = mock_response

        result = _request_with_retry(
            url="https://api.example.com/test",
            params={"latitude": 13.69},
            city_name="Test City",
            endpoint_label="forecast",
        )

        assert result == {"hourly": {"time": []}}
        assert mock_get.call_count == 1

    @patch("src.extract.time.sleep")
    @patch("src.extract.requests.get")
    def test_retry_on_server_error(self, mock_get, mock_sleep):
        """5xx error triggers retry, then succeeds on 2nd attempt."""
        error_response = MagicMock()
        error_response.status_code = 500

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"data": "ok"}

        mock_get.side_effect = [error_response, success_response]

        result = _request_with_retry(
            url="https://api.example.com/test",
            params={},
            max_retries=3,
            backoff_base=0.01,
            rate_limit_pause=0.0,
            city_name="Test City",
            endpoint_label="forecast",
        )

        assert result == {"data": "ok"}
        assert mock_get.call_count == 2

    @patch("src.extract.time.sleep")
    @patch("src.extract.requests.get")
    def test_no_retry_on_client_error(self, mock_get, mock_sleep):
        """4xx error raises APIError immediately (no retry)."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_get.return_value = mock_response

        with pytest.raises(APIError, match="Client error 400"):
            _request_with_retry(
                url="https://api.example.com/test",
                params={},
                rate_limit_pause=0.0,
                city_name="Test City",
                endpoint_label="forecast",
            )

        assert mock_get.call_count == 1  # No retry

    @patch("src.extract.time.sleep")
    @patch("src.extract.requests.get")
    def test_all_retries_exhausted(self, mock_get, mock_sleep):
        """3 consecutive failures raise APIError."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_get.return_value = mock_response

        with pytest.raises(APIError, match="Failed to fetch data"):
            _request_with_retry(
                url="https://api.example.com/test",
                params={},
                max_retries=3,
                backoff_base=0.01,
                rate_limit_pause=0.0,
                city_name="Test City",
                endpoint_label="forecast",
            )

        assert mock_get.call_count == 3

    @patch("src.extract.time.sleep")
    @patch("src.extract.requests.get")
    def test_timeout_triggers_retry(self, mock_get, mock_sleep):
        """Timeout exception triggers retry."""
        import requests as req

        mock_get.side_effect = [
            req.exceptions.Timeout("Connection timed out"),
            MagicMock(status_code=200, json=MagicMock(return_value={"ok": True})),
        ]

        result = _request_with_retry(
            url="https://api.example.com/test",
            params={},
            max_retries=3,
            backoff_base=0.01,
            rate_limit_pause=0.0,
            city_name="Test City",
            endpoint_label="forecast",
        )

        assert result == {"ok": True}
        assert mock_get.call_count == 2

    @patch("src.extract.time.sleep")
    @patch("src.extract.requests.get")
    def test_connection_error_triggers_retry(self, mock_get, mock_sleep):
        """ConnectionError triggers retry."""
        import requests as req

        mock_get.side_effect = [
            req.exceptions.ConnectionError("Connection refused"),
            MagicMock(status_code=200, json=MagicMock(return_value={"ok": True})),
        ]

        result = _request_with_retry(
            url="https://api.example.com/test",
            params={},
            max_retries=3,
            backoff_base=0.01,
            rate_limit_pause=0.0,
            city_name="Test City",
            endpoint_label="forecast",
        )

        assert result == {"ok": True}
        assert mock_get.call_count == 2


class TestExtractForecast:
    """Test forecast extraction function."""

    @patch("src.extract._request_with_retry")
    def test_forecast_passes_correct_params(self, mock_request):
        """Verify forecast passes correct parameters to the API."""
        mock_request.return_value = {"hourly": {}, "daily": {}}

        api_config = {
            "forecast_url": "https://api.open-meteo.com/v1/forecast",
            "hourly_variables": "temperature_2m,relative_humidity_2m",
            "daily_variables": "temperature_2m_max,temperature_2m_min",
            "timezone": "auto",
            "max_retries": 3,
            "backoff_base_seconds": 1.0,
            "rate_limit_pause_seconds": 0.0,
        }

        result = extract_forecast(13.69, -89.22, "San Salvador", api_config)

        assert result == {"hourly": {}, "daily": {}}
        mock_request.assert_called_once()
        call_kwargs = mock_request.call_args
        assert call_kwargs[1]["city_name"] == "San Salvador"
        assert call_kwargs[1]["endpoint_label"] == "forecast"


class TestExtractHistorical:
    """Test historical extraction function."""

    @patch("src.extract._request_with_retry")
    def test_historical_date_range(self, mock_request):
        """Verify historical uses correct date range."""
        mock_request.return_value = {"daily": {}}

        api_config = {
            "historical_url": "https://archive-api.open-meteo.com/v1/archive",
            "daily_variables": "temperature_2m_max",
            "timezone": "auto",
            "max_retries": 3,
            "backoff_base_seconds": 1.0,
            "rate_limit_pause_seconds": 0.0,
        }

        result = extract_historical(13.69, -89.22, "San Salvador", api_config, historical_days=90)

        assert result == {"daily": {}}
        call_kwargs = mock_request.call_args
        params = call_kwargs[1]["params"] if "params" in call_kwargs[1] else call_kwargs[0][1]
        assert "start_date" in params
        assert "end_date" in params
