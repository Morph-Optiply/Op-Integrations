"""Regression tests for tap-extend state loading."""

import unittest

from tap_extend.tap import TapExtend


def minimal_config():
    return {
        "api_url": "https://example.com",
        "client": "TEST",
        "username": "user",
        "password": "pass",
    }


class TapStateLoadingTests(unittest.TestCase):
    def test_load_state_coerces_legacy_scalar_bookmarks(self):
        tap = TapExtend(
            config=minimal_config(),
            state={"bookmarks": {"products": "2026-03-31T00:00:00Z"}},
        )

        self.assertEqual(
            tap.state["bookmarks"]["products"],
            {
                "replication_key": "createDate",
                "replication_key_value": "2026-03-31T00:00:00Z",
            },
        )

    def test_load_state_ignores_non_singer_payloads(self):
        tap = TapExtend(config=minimal_config(), state={"force_patch_products": True})

        self.assertEqual(tap.state, {})


if __name__ == "__main__":
    unittest.main()
