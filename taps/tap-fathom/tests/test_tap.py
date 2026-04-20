"""Regression tests for Fathom tap state and replication behavior."""

import copy
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

    def test_stale_catalog_cannot_remove_main_stream_replication_keys(self):
        discovered = TapFathom(config=minimal_config()).catalog_dict
        stale_catalog = copy.deepcopy(discovered)

        for stream in stale_catalog["streams"]:
            if stream["tap_stream_id"] in {"meetings", "teams", "team_members"}:
                stream.pop("replication_key", None)
                stream["replication_method"] = "FULL_TABLE"

        tap = TapFathom(config=minimal_config(), catalog=stale_catalog)

        self.assertEqual(tap.streams["meetings"].replication_key, "created_at")
        self.assertEqual(tap.streams["meetings"].replication_method, "INCREMENTAL")
        self.assertEqual(tap.streams["teams"].replication_key, "created_at")
        self.assertEqual(tap.streams["teams"].replication_method, "INCREMENTAL")
        self.assertEqual(tap.streams["team_members"].replication_key, "created_at")
        self.assertEqual(tap.streams["team_members"].replication_method, "INCREMENTAL")

    def test_stale_catalog_cannot_make_child_streams_incremental(self):
        config = {
            **minimal_config(),
            "sync_recording_summaries": True,
            "sync_recording_transcripts": True,
        }
        discovered = TapFathom(config=config).catalog_dict
        stale_catalog = copy.deepcopy(discovered)

        for stream in stale_catalog["streams"]:
            if stream["tap_stream_id"] in {
                "recording_summaries",
                "recording_transcripts",
            }:
                stream["replication_key"] = "created_at"
                stream["replication_method"] = "INCREMENTAL"

        tap = TapFathom(config=config, catalog=stale_catalog)

        self.assertIsNone(tap.streams["recording_summaries"].replication_key)
        self.assertEqual(
            tap.streams["recording_summaries"].replication_method,
            "FULL_TABLE",
        )
        self.assertIsNone(tap.streams["recording_transcripts"].replication_key)
        self.assertEqual(
            tap.streams["recording_transcripts"].replication_method,
            "FULL_TABLE",
        )

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
