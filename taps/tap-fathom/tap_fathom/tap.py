"""Fathom tap using hotglue_singer_sdk."""

from typing import Any

from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_fathom.streams import (
    MeetingsStream,
    RecordingSummariesStream,
    RecordingTranscriptsStream,
    TeamMembersStream,
    TeamsStream,
)

STREAM_TYPES = [
    MeetingsStream,
    TeamsStream,
    TeamMembersStream,
]

OPTIONAL_CHILD_STREAMS = [
    ("sync_recording_summaries", RecordingSummariesStream),
    ("sync_recording_transcripts", RecordingTranscriptsStream),
]


class TapFathom(Tap):
    """Singer tap for the Fathom External API."""

    name = "tap-fathom"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.fathom.ai/external/v1",
            description="Fathom External API base URL.",
        ),
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="Fathom API key sent as X-Api-Key.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2000-01-01T00:00:00Z",
            description="Earliest meeting created_at timestamp to sync.",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            default=100,
            description="Requested page size for cursor-paginated endpoints.",
        ),
        th.Property(
            "rate_limit_per_minute",
            th.IntegerType,
            default=50,
            description=(
                "Client-side request cap. Fathom documents 60 requests per "
                "60 seconds; default leaves headroom."
            ),
        ),
        th.Property(
            "calendar_invitees_domains",
            th.StringType,
            description=(
                "Optional comma-separated domains for meetings filtering. "
                "Sent as repeated calendar_invitees_domains[] params."
            ),
        ),
        th.Property(
            "calendar_invitees_domains_type",
            th.StringType,
            default="all",
            description="Meeting invitee domain filter: all, only_internal, or one_or_more_external.",
        ),
        th.Property(
            "recorded_by",
            th.StringType,
            description="Optional comma-separated recorder emails for meetings filtering.",
        ),
        th.Property(
            "teams",
            th.StringType,
            description="Optional comma-separated team names for meetings filtering.",
        ),
        th.Property(
            "team_member_team",
            th.StringType,
            description="Optional team name filter for the team_members stream.",
        ),
        th.Property(
            "include_action_items",
            th.BooleanType,
            default=False,
            description="Ask Fathom to include meeting action items in meetings payloads.",
        ),
        th.Property(
            "include_crm_matches",
            th.BooleanType,
            default=False,
            description="Ask Fathom to include CRM matches in meetings payloads.",
        ),
        th.Property(
            "include_summary",
            th.BooleanType,
            default=False,
            description="Ask Fathom to include default summaries in meetings payloads.",
        ),
        th.Property(
            "include_transcript",
            th.BooleanType,
            default=False,
            description="Ask Fathom to include transcripts in meetings payloads.",
        ),
        th.Property(
            "sync_recording_summaries",
            th.BooleanType,
            default=False,
            description="Enable per-recording summary endpoint calls. Disabled by default for rate safety.",
        ),
        th.Property(
            "sync_recording_transcripts",
            th.BooleanType,
            default=False,
            description="Enable per-recording transcript endpoint calls. Disabled by default for rate safety.",
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

    def discover_streams(self):
        """Return discovered streams."""
        stream_types = list(STREAM_TYPES)
        for config_key, stream_type in OPTIONAL_CHILD_STREAMS:
            if self.config.get(config_key):
                stream_types.append(stream_type)
        return [stream_type(tap=self) for stream_type in stream_types]


if __name__ == "__main__":
    TapFathom.cli()
