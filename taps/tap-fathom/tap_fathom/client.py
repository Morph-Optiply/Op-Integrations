"""Fathom base stream with cursor pagination and defensive rate limiting."""

from __future__ import annotations

import json
import logging
import threading
import time
import typing as t
from datetime import datetime, timezone
from http.client import RemoteDisconnected

import backoff
import requests
import singer
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath
from hotglue_singer_sdk.streams import RESTStream
from hotglue_singer_sdk.tap_base import InvalidCredentialsError
from requests.exceptions import ChunkedEncodingError, JSONDecodeError
from singer import StateMessage

if t.TYPE_CHECKING:
    from hotglue_singer_sdk.helpers.types import Context

logger = logging.getLogger(__name__)


class FathomStream(RESTStream):
    """Base stream for Fathom External API endpoints."""

    records_jsonpath = "$.items[*]"
    extra_retry_statuses = [408, 429, 500, 502, 503, 504]
    timeout = 120

    _rate_limit_lock = threading.Lock()
    _last_request_at = 0.0

    @property
    def url_base(self) -> str:
        """Return the configured Fathom API base URL."""
        return self.config.get("api_url", "https://api.fathom.ai/external/v1").rstrip("/")

    @property
    def authenticator(self):
        """Fathom uses an X-Api-Key header."""
        return None

    @property
    def http_headers(self) -> dict:
        """Return request headers."""
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Api-Key": self.config["api_key"],
        }

    def request_decorator(self, func):
        """Wrap requests with client-side throttling and retry backoff."""
        decorated = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                RemoteDisconnected,
                ChunkedEncodingError,
            ),
            max_tries=8,
            factor=2,
            jitter=backoff.full_jitter,
        )(func)

        def throttled(prepared_request, context):
            self._throttle()
            return decorated(prepared_request, context)

        return throttled

    def _throttle(self) -> None:
        """Sleep before each call so the tap stays below Fathom's global cap."""
        configured_limit = self.config.get("rate_limit_per_minute", 50) or 50
        try:
            limit = max(1, min(int(configured_limit), 60))
        except (TypeError, ValueError):
            limit = 50

        min_interval = 60.0 / limit
        with self._rate_limit_lock:
            now = time.monotonic()
            elapsed = now - FathomStream._last_request_at
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            FathomStream._last_request_at = time.monotonic()

    def validate_response(self, response: requests.Response) -> None:
        """Handle auth, rate limit, and transient API errors."""
        if response.status_code == 429:
            wait = self._rate_limit_wait_seconds(response, default=60.0)
            logger.warning("Fathom rate limit hit. Sleeping %.1fs.", wait)
            time.sleep(wait)
            raise RetriableAPIError(f"Fathom rate limited (429). Slept {wait:.1f}s.")

        if response.status_code == 401:
            raise InvalidCredentialsError("Fathom auth failed (401). Check api_key.")

        if response.status_code == 403:
            raise FatalAPIError(
                f"Fathom 403 Forbidden for {self.path}. Check API permissions."
            )

        if response.status_code in (404, 204):
            return

        if response.status_code in self.extra_retry_statuses or response.status_code >= 500:
            raise RetriableAPIError(
                f"Fathom {response.status_code}: {response.text[:300]}"
            )

        if 400 <= response.status_code < 500:
            raise FatalAPIError(f"Fathom {response.status_code}: {response.text[:300]}")

        self._respect_rate_limit_headers(response)

    def _respect_rate_limit_headers(self, response: requests.Response) -> None:
        """Pause if Fathom says the minute window is exhausted."""
        remaining = response.headers.get("RateLimit-Remaining")
        if remaining is None:
            return

        try:
            remaining_int = int(float(remaining))
        except ValueError:
            return

        if remaining_int > 1:
            return

        wait = self._rate_limit_wait_seconds(response, default=2.0)
        if wait > 0:
            logger.info(
                "Fathom RateLimit-Remaining=%s. Sleeping %.1fs before continuing.",
                remaining,
                wait,
            )
            time.sleep(wait)

    def _rate_limit_wait_seconds(
        self,
        response: requests.Response,
        default: float,
    ) -> float:
        """Determine wait time from Retry-After or RateLimit-Reset headers."""
        for header_name in ("Retry-After", "RateLimit-Reset"):
            value = response.headers.get(header_name)
            if not value:
                continue
            try:
                parsed = float(value)
            except ValueError:
                continue

            if header_name == "RateLimit-Reset" and parsed > time.time():
                parsed = parsed - time.time()
            return max(parsed, 1.0)

        return default

    def get_next_page_token(self, response, previous_token):
        """Fathom uses a next_cursor value in the JSON response."""
        if response.status_code in (404, 204):
            return None
        try:
            return response.json().get("next_cursor")
        except (json.JSONDecodeError, JSONDecodeError):
            return None

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Return shared cursor pagination params."""
        params: dict[str, t.Any] = {
            "limit": self._page_size(),
        }
        if next_page_token:
            params["cursor"] = next_page_token
        return params

    def _page_size(self) -> int:
        """Return a conservative requested page size."""
        try:
            return max(1, min(int(self.config.get("page_size", 100)), 100))
        except (TypeError, ValueError):
            return 100

    def parse_response(self, response: requests.Response):
        """Parse JSON records from the configured jsonpath."""
        if response.status_code in (404, 204):
            return

        try:
            payload = response.json()
        except (json.JSONDecodeError, JSONDecodeError):
            logger.error("Failed to decode Fathom response: %s", response.text[:500])
            return

        yield from extract_jsonpath(self.records_jsonpath, input=payload)

    def apply_catalog(self, catalog) -> None:
        """Apply selection while keeping tap-owned replication keys current."""
        configured_replication_key = type(self).__dict__.get("replication_key")
        super().apply_catalog(catalog)

        if configured_replication_key:
            self.replication_key = configured_replication_key
            if self.forced_replication_method == "FULL_TABLE":
                self.forced_replication_method = None
            return

        if getattr(self, "replication_key", None):
            self.logger.warning(
                "Ignoring catalog replication key '%s' for non-incremental stream '%s'.",
                self.replication_key,
                self.name,
            )
            self.replication_key = None
            if self.forced_replication_method == "INCREMENTAL":
                self.forced_replication_method = None

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Filter already-bookmarked incremental records."""
        if self._record_is_after_bookmark(row, context):
            return row
        return None

    def _record_is_after_bookmark(
        self,
        row: dict,
        context: Context | None = None,
    ) -> bool:
        """Return False when an API page repeats the bookmarked boundary record."""
        replication_key = getattr(self, "replication_key", None)
        if not replication_key:
            return True

        start = self._starting_datetime(context)
        if start is None:
            return True

        value = row.get(replication_key)
        if value in (None, ""):
            self.logger.warning(
                "Keeping %s record without replication key value '%s'.",
                self.name,
                replication_key,
            )
            return True

        record_time = self._parse_datetime(value)
        start_time = self._parse_datetime(start)
        if record_time is None or start_time is None:
            return str(value) > self._format_datetime(start)

        return record_time > start_time

    def _starting_datetime(self, context: Context | None = None) -> datetime | None:
        """Return the SDK starting marker, falling back to loaded Singer state."""
        start_value = self.get_starting_replication_key_value(context)
        if start_value is None:
            state = self.get_context_state(context)
            if state.get("replication_key") == getattr(self, "replication_key", None):
                start_value = state.get("replication_key_value")

        if start_value is None:
            return None

        return self._parse_datetime(start_value)

    def _parse_datetime(self, value) -> datetime | None:
        """Parse Fathom/Singer datetime values into timezone-aware datetimes."""
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            normalized = value.strip()
            if normalized.endswith("Z"):
                normalized = f"{normalized[:-1]}+00:00"
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                return None
        else:
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _config_start_date(self) -> str:
        """Return config start date as an ISO timestamp string."""
        configured = self.config.get("start_date") or "2000-01-01T00:00:00Z"
        if isinstance(configured, str):
            return configured
        return self._format_datetime(configured)

    def _format_datetime(self, value) -> str:
        """Format datetime-like values for Fathom query params."""
        if isinstance(value, str):
            return value
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if hasattr(value, "isoformat"):
            return value.isoformat().replace("+00:00", "Z")
        return str(value)

    def _csv_values(self, config_key: str) -> list[str]:
        """Parse comma-separated config values."""
        value = self.config.get(config_key)
        if not value:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [item.strip() for item in str(value).split(",") if item.strip()]

    def _write_state_message(self):
        """Write HotGlue-compatible state without partition noise."""
        tap_state = self.tap_state
        if tap_state and tap_state.get("bookmarks"):
            for stream_state in tap_state["bookmarks"].values():
                if isinstance(stream_state, dict):
                    stream_state.pop("partitions", None)
        singer.write_message(StateMessage(value=tap_state))
