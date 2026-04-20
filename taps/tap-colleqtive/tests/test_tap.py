"""Regression tests for tap-colleqtive."""

import unittest

from tap_colleqtive.streams import BuyOrdersStream, OrdersStream, ProductsStream, StocksStream
from tap_colleqtive.tap import TapColleqtive


def minimal_config():
    return {
        "api_url": "https://example.com",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "scope": "api://client-id/.default",
    }


class TapColleqtiveTests(unittest.TestCase):
    def test_discover_streams(self):
        tap = TapColleqtive(config=minimal_config())

        self.assertEqual(
            [stream.name for stream in tap.discover_streams()],
            ["products", "stocks", "orders", "buy_orders"],
        )

    def test_load_state_coerces_legacy_scalar_bookmarks(self):
        tap = TapColleqtive(
            config=minimal_config(),
            state={"bookmarks": {"stocks": "2026-04-01T00:00:00Z"}},
        )

        self.assertEqual(
            tap.state["bookmarks"]["stocks"],
            {
                "replication_key": "last_modified_date",
                "replication_key_value": "2026-04-01T00:00:00Z",
            },
        )

    def test_products_unwraps_data_records(self):
        tap = TapColleqtive(config={**minimal_config(), "page_size": 200})
        stream = ProductsStream(tap=tap)

        items = stream._items_from_payload({
            "status": "success",
            "data": {"records": [{"product_number": "P1"}]},
        })

        self.assertEqual(items, [{"product_number": "P1"}])

    def test_stocks_uses_page_and_incremental_params(self):
        tap = TapColleqtive(config={**minimal_config(), "page_size": 200})
        stream = StocksStream(tap=tap)

        params = stream._request_params(None, 1)

        self.assertEqual(params["Page_Size"], 200)
        self.assertEqual(params["Page_Start"], 1)
        self.assertNotIn("reason_code", params)

    def test_orders_adds_reason_code_filter(self):
        tap = TapColleqtive(config={**minimal_config(), "page_size": 200})
        stream = OrdersStream(tap=tap)

        params = stream._request_params(None, 3)

        self.assertEqual(params["Page_Size"], 200)
        self.assertEqual(params["Page_Start"], 3)
        self.assertEqual(params["reason_code"], 100)

    def test_incremental_records_alias_updated_on_to_last_modified_date(self):
        tap = TapColleqtive(config=minimal_config())
        stream = StocksStream(tap=tap)

        record = stream._normalize_record({
            "id": 1,
            "updated_on": "2026-04-01T12:00:00Z",
            "counting_lines_remaining_stockpool": {"pool": 1},
        })

        self.assertEqual(record["last_modified_date"], "2026-04-01T12:00:00Z")
        self.assertEqual(record["counting_lines_remaining_stockpool"], "{\"pool\": 1}")

    def test_buy_orders_stringifies_order_lines(self):
        tap = TapColleqtive(config=minimal_config())
        stream = BuyOrdersStream(tap=tap)

        record = stream._normalize_record({
            "order_number": "BO1",
            "store_number": "S1",
            "updated_on": "2026-04-01T12:00:00Z",
            "order_lines": [{"product_number": "P1"}],
        })

        self.assertEqual(record["last_modified_date"], "2026-04-01T12:00:00Z")
        self.assertEqual(record["order_lines"], "[{\"product_number\": \"P1\"}]")


if __name__ == "__main__":
    unittest.main()
