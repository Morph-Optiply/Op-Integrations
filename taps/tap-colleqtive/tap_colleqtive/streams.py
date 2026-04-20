"""Stream definitions for tap-colleqtive."""

from __future__ import annotations

import json
import logging
import time
from email.utils import parsedate_to_datetime
from typing import Any, Iterable, Optional

import backoff
import requests

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import Stream

try:
    from hotglue_singer_sdk.exceptions import InvalidCredentialsError
except ImportError:
    class InvalidCredentialsError(Exception):
        """Raised when API credentials are invalid."""


logger = logging.getLogger(__name__)


class _RetryableError(Exception):
    """Raised for errors that should trigger backoff retry."""


def _json_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _field_schema(fields: dict[str, th.JSONTypeHelper]) -> dict:
    return th.PropertiesList(
        *[th.Property(name, field_type) for name, field_type in fields.items()]
    ).to_dict()


PRODUCT_FIELDS = {
    "product_number": th.StringType,
    "product_name": th.StringType,
    "barcode": th.StringType,
    "price": th.NumberType,
    "purchase_price": th.NumberType,
    "is_active": th.BooleanType,
    "moq": th.NumberType,
    "brand_code": th.StringType,
    "brand_description": th.StringType,
    "image_url": th.StringType,
    "product_url": th.StringType,
    "volume": th.StringType,
    "base_unit_name": th.StringType,
    "allow_decimal": th.BooleanType,
    "supplier_id": th.StringType,
    "supplier_name": th.StringType,
    "category_code": th.StringType,
    "promotion": th.StringType,
    "old_price": th.NumberType,
    "is_order_manually": th.BooleanType,
    "is_edit_replenishment": th.BooleanType,
    "is_edit_min_max": th.BooleanType,
    "is_counted": th.BooleanType,
    "is_expiration": th.BooleanType,
    "expiration_min_range": th.IntegerType,
    "deprecation_date": th.DateTimeType,
    "product_details": th.StringType,
    "margin_category": th.StringType,
    "is_stock": th.BooleanType,
    "attention_code": th.StringType,
    "referral_product_id": th.StringType,
    "reset_store_stock": th.BooleanType,
    "country_code": th.StringType,
    "promo_stores_allowed": th.StringType,
    "promo_stores_not_allowed": th.StringType,
    "units": th.StringType,
    "stores_allowed": th.StringType,
    "stores_not_allowed": th.StringType,
    "countries_allowed": th.StringType,
    "countries_not_allowed": th.StringType,
    "smallest_product_number": th.StringType,
    "smallest_quantity": th.NumberType,
    "price_lines": th.StringType,
    "is_promotion": th.BooleanType,
    "promotion_week": th.StringType,
    "barcodes_array": th.StringType,
    "purchase_unit_barcode": th.StringType,
    "purchase_unit_quantity": th.NumberType,
    "purchase_unit_name": th.StringType,
    "is_set": th.BooleanType,
    "set_product": th.StringType,
    "carrier_item_quantity": th.NumberType,
    "product_type": th.StringType,
    "average_weight": th.NumberType,
    "variant_factor": th.NumberType,
    "is_store_product": th.BooleanType,
    "color": th.StringType,
    "color_code": th.StringType,
    "size": th.StringType,
    "size_code": th.StringType,
    "variant_code": th.StringType,
    "variant_family": th.StringType,
    "general_ledger": th.StringType,
    "is_template": th.BooleanType,
    "shelf_label_layout": th.IntegerType,
    "shelf_label": th.BooleanType,
    "shelf_label_price": th.BooleanType,
    "shelf_label_quantity": th.IntegerType,
    "shelf_label_barcode": th.BooleanType,
    "price_compare_multiplier": th.NumberType,
    "price_compare_unit": th.StringType,
    "special_price": th.NumberType,
    "free_fields": th.StringType,
    "purchase_unit_product_number": th.StringType,
    "supplier_product_number": th.StringType,
    "edit_min_max_percentage": th.IntegerType,
}

STOCK_LOG_FIELDS = {
    "id": th.IntegerType,
    "last_modified_date": th.DateTimeType,
    "store_number": th.StringType,
    "product_number": th.StringType,
    "barcode": th.StringType,
    "quantity": th.NumberType,
    "location": th.IntegerType,
    "source": th.StringType,
    "reference": th.StringType,
    "reason_code": th.IntegerType,
    "reason_code_description": th.StringType,
    "stock_pool_1": th.NumberType,
    "stock_pool_2": th.NumberType,
    "stock_pool_3": th.NumberType,
    "stock_pool_4": th.NumberType,
    "stock_pool_5": th.NumberType,
    "stock_pool_6": th.NumberType,
    "stock_pool_7": th.NumberType,
    "stock_pool_8": th.NumberType,
    "stock_pool_9": th.NumberType,
    "stock_pool_10": th.NumberType,
    "counted_quantity": th.NumberType,
    "expected_quantity": th.NumberType,
    "app_user_name": th.StringType,
    "updated_on": th.DateTimeType,
    "created_on": th.DateTimeType,
    "price": th.NumberType,
    "cost_center": th.StringType,
    "purchase_unit_quantity": th.NumberType,
    "general_ledger_debit": th.StringType,
    "general_ledger_credit": th.StringType,
    "purchase_price": th.NumberType,
    "store_products_price": th.NumberType,
    "reference_id": th.IntegerType,
    "location_code": th.StringType,
    "customer_order_shipment_number": th.StringType,
    "customer_order_shipment_number_line_number": th.IntegerType,
    "customer_order_number": th.StringType,
    "customer_order_line_number": th.IntegerType,
    "moved_on": th.DateTimeType,
    "updated_by": th.StringType,
    "remarks": th.StringType,
    "orderno": th.StringType,
    "exception_image": th.StringType,
    "review_status": th.StringType,
    "counting_assignment_id": th.IntegerType,
    "counting_lines_remaining": th.IntegerType,
    "stock_pool": th.StringType,
    "location_tag": th.StringType,
    "counting_lines_remaining_stockpool": th.StringType,
}

BUY_ORDER_FIELDS = {
    "order_number": th.StringType,
    "store_number": th.StringType,
    "last_modified_date": th.DateTimeType,
    "order_type": th.StringType,
    "import_id": th.IntegerType,
    "datetime_created": th.DateTimeType,
    "datetime_expected": th.DateTimeType,
    "tracing_url": th.StringType,
    "supplier_code": th.StringType,
    "supplier_name": th.StringType,
    "no_of_containers": th.IntegerType,
    "updated_on": th.DateTimeType,
    "status": th.StringType,
    "store_gln": th.StringType,
    "is_downloaded": th.BooleanType,
    "external_reference_number": th.StringType,
    "store_origin": th.StringType,
    "order_lines": th.StringType,
}


class ColleqtiveStream(Stream):
    """Base class for Colleqtive public API streams."""

    _session: Optional[requests.Session] = None
    _access_token: Optional[str] = None
    _token_expires_at = 0.0
    _next_request_at = 0.0

    path = ""
    primary_keys: list[str] = []
    replication_key: Optional[str] = None
    replication_method = "FULL_TABLE"
    page_start_param = "Page_Start"
    page_size_param = "Page_Size"
    records_key = "list"
    schema_fields: dict[str, th.JSONTypeHelper] = {}
    json_string_fields: set[str] = set()

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "Accept": "application/json",
                "Content-Type": "application/json",
            })
        return self._session

    @property
    def base_url(self) -> str:
        return self.config.get("api_url", "https://bbq-test.colleqtive.net").rstrip("/")

    @property
    def page_size(self) -> int:
        raw_value = self.config.get("page_size", 200)
        try:
            page_size = int(raw_value)
        except (TypeError, ValueError):
            logger.warning("Invalid page_size=%r. Falling back to 200.", raw_value)
            page_size = 200
        return max(page_size, 1)

    @property
    def requests_per_second(self) -> float:
        raw_value = self.config.get("requests_per_second", 4)
        try:
            requests_per_second = float(raw_value)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid requests_per_second=%r. Falling back to 4 requests/second.",
                raw_value,
            )
            requests_per_second = 4.0
        return max(requests_per_second, 0.1)

    @property
    def request_timeout(self) -> float:
        raw = self.config.get("request_timeout_seconds", 120)
        try:
            return max(float(raw), 10.0)
        except (TypeError, ValueError):
            return 120.0

    def _apply_client_throttle(self) -> None:
        min_interval = 1.0 / self.requests_per_second
        now = time.monotonic()
        sleep_for = ColleqtiveStream._next_request_at - now
        if sleep_for > 0:
            time.sleep(sleep_for)
            now = time.monotonic()
        ColleqtiveStream._next_request_at = max(ColleqtiveStream._next_request_at, now) + min_interval

    def _delay_from_retry_after(self, retry_after: Optional[str]) -> Optional[float]:
        if not retry_after:
            return None
        try:
            return max(float(retry_after), 0.0)
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return None
            return max(retry_at.timestamp() - time.time(), 0.0)

    def _token_is_valid(self) -> bool:
        return bool(self._access_token) and time.time() < self._token_expires_at - 60

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout, _RetryableError),
        max_tries=5,
        factor=2,
        jitter=backoff.full_jitter,
    )
    def _fetch_access_token(self) -> str:
        response = requests.post(
            self.config.get(
                "token_url",
                "https://login.microsoftonline.com/"
                "ca47d553-3e2b-42f0-a655-7ec6f6b466e4/oauth2/v2.0/token",
            ),
            data={
                "grant_type": "client_credentials",
                "client_id": self.config["client_id"],
                "client_secret": self.config["client_secret"],
                "scope": self.config["scope"],
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.request_timeout,
        )

        if response.status_code in (429, 500, 502, 503, 504):
            if response.status_code == 429:
                delay = self._delay_from_retry_after(response.headers.get("Retry-After")) or 30.0
                logger.warning("Token endpoint rate limited (429). Sleeping %.1fs.", delay)
                time.sleep(delay)
            raise _RetryableError(f"Token endpoint error ({response.status_code})")
        if response.status_code == 401:
            raise InvalidCredentialsError(
                f"Authentication failed (401): {response.text[:300]}"
            )

        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise InvalidCredentialsError("Authentication failed: token response has no access_token")

        expires_in = payload.get("expires_in", 3600)
        try:
            expires_in_seconds = int(expires_in)
        except (TypeError, ValueError):
            expires_in_seconds = 3600

        ColleqtiveStream._access_token = token
        ColleqtiveStream._token_expires_at = time.time() + expires_in_seconds
        return token

    def _get_access_token(self) -> str:
        if self._token_is_valid():
            return str(self._access_token)
        return self._fetch_access_token()

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout, _RetryableError),
        max_tries=8,
        factor=2,
        jitter=backoff.full_jitter,
    )
    def _request(self, path: str, params: Optional[dict] = None, *, retry_auth: bool = True) -> requests.Response:
        self._apply_client_throttle()
        response = self.session.get(
            f"{self.base_url}{path}",
            params=params,
            headers={"Authorization": f"Bearer {self._get_access_token()}"},
            timeout=self.request_timeout,
        )

        if response.status_code == 401 and retry_auth:
            ColleqtiveStream._access_token = None
            return self._request(path, params=params, retry_auth=False)
        if response.status_code == 401:
            raise InvalidCredentialsError(
                f"Authentication failed (401): {response.text[:300]}"
            )
        if response.status_code == 429:
            delay = self._delay_from_retry_after(response.headers.get("Retry-After")) or 30.0
            logger.warning("Rate limited (429). Sleeping %.1fs.", delay)
            time.sleep(delay)
            raise _RetryableError("Rate limited (429)")
        if response.status_code >= 500:
            raise _RetryableError(
                f"Server error ({response.status_code}): {response.text[:300]}"
            )
        if response.status_code >= 400:
            logger.error(
                "GET %s -> %d: %s",
                response.url,
                response.status_code,
                (response.text or "")[:500],
            )
        response.raise_for_status()
        return response

    def _page_params(self, page_start: int) -> dict[str, Any]:
        return {
            self.page_size_param: self.page_size,
            self.page_start_param: page_start,
        }

    def _incremental_filter(self, context: Optional[dict]) -> Optional[str]:
        start_replication = self.get_starting_replication_key_value(context)
        if start_replication:
            return str(start_replication)
        if self.config.get("start_date"):
            return str(self.config["start_date"])
        return None

    def _base_params(self, context: Optional[dict], page_start: int) -> dict[str, Any]:
        params = self._page_params(page_start)

        if self.replication_key:
            last_modified_date = self._incremental_filter(context)
            if last_modified_date:
                params["last_modified_date"] = last_modified_date

        store_number = self.config.get("store_number")
        if store_number:
            params["store_number"] = store_number

        return params

    def _items_from_payload(self, payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if not isinstance(payload, dict):
            return []

        if isinstance(payload.get("data"), dict):
            return self._items_from_payload(payload["data"])
        if isinstance(payload.get("data"), list):
            return self._items_from_payload(payload["data"])

        for key in (self.records_key, "records", "list", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        return []

    def _normalize_record(self, row: dict) -> dict:
        record = {field: row.get(field) for field in self.schema_fields}
        for field in self.json_string_fields:
            record[field] = _json_string(row.get(field))

        if self.replication_key and not record.get(self.replication_key):
            record[self.replication_key] = (
                row.get("last_modified_date")
                or row.get("updated_on")
                or row.get("datetime_created")
            )

        return record

    def _request_params(self, context: Optional[dict], page_start: int) -> dict[str, Any]:
        return self._base_params(context, page_start)

    def _write_state_message(self) -> None:
        """Clean partitions from state to avoid bloat."""
        try:
            tap_state = getattr(getattr(self, "_tap", None), "state", {}) or {}
            for stream_state in tap_state.get("bookmarks", {}).values():
                stream_state.pop("partitions", None)
            super()._write_state_message()
        except Exception as exc:
            self.logger.warning("Error writing state message: %s", exc)

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        page_start = 1

        while True:
            params = self._request_params(context, page_start)
            payload = self._request(self.path, params=params).json()
            items = self._items_from_payload(payload)

            for row in items:
                yield self._normalize_record(row)

            if len(items) < self.page_size:
                break

            page_start += 1


class ProductsStream(ColleqtiveStream):
    """Colleqtive products.

    FULL_TABLE because this endpoint does not expose last_modified_date in the
    request or response shape returned by the v2 public API.
    """

    name = "products"
    path = "/api/v2/public/products"
    primary_keys = ["product_number"]
    replication_method = "FULL_TABLE"
    records_key = "records"
    schema_fields = PRODUCT_FIELDS
    json_string_fields = {
        "promo_stores_allowed",
        "promo_stores_not_allowed",
        "units",
        "stores_allowed",
        "stores_not_allowed",
        "countries_allowed",
        "countries_not_allowed",
        "set_product",
        "free_fields",
    }
    schema = _field_schema(schema_fields)


class StocksStream(ColleqtiveStream):
    """Colleqtive stock changes."""

    name = "stocks"
    path = "/api/v2/public/products/stock/storeproductlogs"
    primary_keys = ["id"]
    replication_key = "last_modified_date"
    replication_method = "INCREMENTAL"
    records_key = "list"
    schema_fields = STOCK_LOG_FIELDS
    json_string_fields = {"counting_lines_remaining_stockpool"}
    schema = _field_schema(schema_fields)

    def _request_params(self, context: Optional[dict], page_start: int) -> dict[str, Any]:
        params = super()._request_params(context, page_start)
        if self.config.get("end_date"):
            params["to_modified_date"] = str(self.config["end_date"])
        return params


class OrdersStream(StocksStream):
    """Colleqtive sell-order transactions from stock logs."""

    name = "orders"

    def _request_params(self, context: Optional[dict], page_start: int) -> dict[str, Any]:
        params = super()._request_params(context, page_start)
        params["reason_code"] = 100
        return params


class BuyOrdersStream(ColleqtiveStream):
    """Colleqtive delivery buy orders."""

    name = "buy_orders"
    path = "/api/v2/public/orders/deliveries"
    primary_keys = ["order_number", "store_number"]
    replication_key = "last_modified_date"
    replication_method = "INCREMENTAL"
    records_key = "list"
    schema_fields = BUY_ORDER_FIELDS
    json_string_fields = {"order_lines"}
    schema = _field_schema(schema_fields)
