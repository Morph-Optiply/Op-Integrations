"""Regression tests for Fathom tap state and replication behavior."""

import unittest
from unittest.mock import patch

from tap_fathom.tap import TapFathom


def minimal_config():
    return {
        "api_url": "https://example.com",
        "api_key": "test-key",
        "start_date": "2000-01-01T00:00:00Z",
    }


class TapFathomReplicationTests(unittest.TestCase):
    def test_main_streams_are_incremental_on_created_at(self):
        tap = TapFathom(config=minimal_config())

        self.assertEqual(tap.streams["meetings"].replication_key, "created_at")
        self.assertEqual(tap.streams["teams"].replication_key, "created_at")
        self.assertEqual(tap.streams["team_members"].replication_key, "created_at")

    def test_load_state_coerces_legacy_scalar_bookmarks(self):
        tap = TapFathom(
            config=minimal_config(),
            state={"bookmarks": {"meetings": "2026-04-20T08:00:00Z"}},
        )

        self.assertEqual(
            tap.state["bookmarks"]["meetings"],
            {
                "replication_key": "created_at",
                "replication_key_value": "2026-04-20T08:00:00Z",
            },
        )

    def test_state_writer_preserves_bookmarks_when_removing_partitions(self):
        tap = TapFathom(
            config=minimal_config(),
            state={
                "bookmarks": {
                    "meetings": {
                        "replication_key": "created_at",
                        "replication_key_value": "2026-04-20T08:00:00Z",
                        "partitions": [{"context": "noise"}],
                    }
                }
            },
        )

        with patch("tap_fathom.client.singer.write_message") as write_message:
            tap.streams["meetings"]._write_state_message()

        state_message = write_message.call_args.args[0]
        self.assertEqual(
            state_message.value["bookmarks"]["meetings"],
            {
                "replication_key": "created_at",
                "replication_key_value": "2026-04-20T08:00:00Z",
            },
        )

    def test_post_process_filters_bookmarked_boundary_records(self):
        tap = TapFathom(
            config=minimal_config(),
            state={
                "bookmarks": {
                    "meetings": {
                        "replication_key": "created_at",
                        "replication_key_value": "2026-04-20T08:00:00Z",
                    }
                }
            },
        )
        stream = tap.streams["meetings"]

        self.assertIsNone(
            stream.post_process(
                {"recording_id": 1, "created_at": "2026-04-20T08:00:00Z"}
            )
        )
        self.assertEqual(
            stream.post_process(
                {"recording_id": 2, "created_at": "2026-04-20T08:00:01Z"}
            ),
            {"recording_id": 2, "created_at": "2026-04-20T08:00:01Z"},
        )

    def test_team_members_filter_uses_created_at_bookmark(self):
        tap = TapFathom(
            config=minimal_config(),
            state={
                "bookmarks": {
                    "team_members": {
                        "replication_key": "created_at",
                        "replication_key_value": "2026-04-20T08:00:00Z",
                    }
                }
            },
        )
        stream = tap.streams["team_members"]

        self.assertIsNone(
            stream.post_process(
                {"email": "old@example.com", "created_at": "2026-04-20T07:59:59Z"}
            )
        )
        self.assertEqual(
            stream.post_process(
                {"email": "new@example.com", "created_at": "2026-04-20T08:00:01Z"}
            ),
            {"email": "new@example.com", "created_at": "2026-04-20T08:00:01Z"},
        )


if __name__ == "__main__":
    unittest.main()
