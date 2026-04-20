"""TapColleqtive -- Singer tap for the Colleqtive public API."""

from __future__ import annotations

from typing import Any, List

from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th

from tap_colleqtive.streams import (
    BuyOrdersStream,
    OrdersStream,
    ProductsStream,
    StocksStream,
)


class TapColleqtive(Tap):
    """Singer tap for the Colleqtive public API.

    Streams:
      - products    FULL_TABLE  GET /api/v2/public/products
      - stocks      INCREMENTAL GET /api/v2/public/products/stock/storeproductlogs
      - orders      INCREMENTAL GET /api/v2/public/products/stock/storeproductlogs?reason_code=100
      - buy_orders  INCREMENTAL GET /api/v2/public/orders/deliveries
    """

    name = "tap-colleqtive"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            default="https://bbq-test.colleqtive.net",
            description="Base URL for the Colleqtive public API",
        ),
        th.Property(
            "token_url",
            th.StringType,
            required=False,
            default=(
                "https://login.microsoftonline.com/"
                "ca47d553-3e2b-42f0-a655-7ec6f6b466e4/oauth2/v2.0/token"
            ),
            description="OAuth2 client credentials token endpoint",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="OAuth2 client ID",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="OAuth2 client secret",
        ),
        th.Property(
            "scope",
            th.StringType,
            required=True,
            description="OAuth2 scope for the Colleqtive API",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="Earliest last_modified_date to sync for incremental streams",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            required=False,
            description=(
                "Latest last_modified_date to sync. Used as to_modified_date where "
                "the API supports it."
            ),
        ),
        th.Property(
            "store_number",
            th.StringType,
            required=False,
            description="Optional store_number filter for stock, order, and buy-order streams",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            required=False,
            default=200,
            description="Page_Size/Page_Size value. Default 200.",
        ),
        th.Property(
            "requests_per_second",
            th.NumberType,
            required=False,
            default=4,
            description="Client-side request throttle for Colleqtive API calls",
        ),
        th.Property(
            "request_timeout_seconds",
            th.NumberType,
            required=False,
            default=120,
            description="Per-request timeout in seconds",
        ),
    ).to_dict()

    def load_state(self, state: dict[str, Any]) -> None:
        """Normalize legacy scalar bookmarks before delegating to the SDK."""
        super().load_state(self._normalize_legacy_bookmarks(state))

    def _normalize_legacy_bookmarks(self, state: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(state, dict):
            return {}

        bookmarks = state.get("bookmarks")
        if not isinstance(bookmarks, dict):
            return state

        normalized_state = dict(state)
        normalized_bookmarks: dict[str, dict[str, Any]] = {}

        for stream_name, stream_state in bookmarks.items():
            if isinstance(stream_state, dict):
                normalized_bookmarks[stream_name] = stream_state
                continue

            stream = self.streams.get(stream_name)
            replication_key = getattr(stream, "replication_key", None) if stream else None

            if replication_key and stream_state not in (None, ""):
                self.logger.warning(
                    "Coercing legacy scalar bookmark for stream '%s' into Singer state.",
                    stream_name,
                )
                normalized_bookmarks[stream_name] = {
                    "replication_key": replication_key,
                    "replication_key_value": stream_state,
                }
                continue

            self.logger.warning(
                "Ignoring malformed bookmark for stream '%s': expected an object, got %s.",
                stream_name,
                type(stream_state).__name__,
            )
            normalized_bookmarks[stream_name] = {}

        normalized_state["bookmarks"] = normalized_bookmarks
        return normalized_state

    def discover_streams(self) -> List:
        """Return stream instances."""
        return [
            ProductsStream(tap=self),
            StocksStream(tap=self),
            OrdersStream(tap=self),
            BuyOrdersStream(tap=self),
        ]


if __name__ == "__main__":
    TapColleqtive.cli()
