"""Fathom stream definitions."""

from __future__ import annotations

import json
import typing as t

from tap_fathom.client import FathomStream

if t.TYPE_CHECKING:
    from hotglue_singer_sdk.helpers.types import Context


def nullable(type_name: str) -> list[str]:
    """Return a nullable JSON schema type."""
    return [type_name, "null"]


def array_of(item_schema: dict) -> dict:
    """Return a nullable array schema."""
    return {
        "type": nullable("array"),
        "items": item_schema,
    }


person_schema = {
    "type": nullable("object"),
    "additionalProperties": True,
    "properties": {
        "name": {"type": nullable("string")},
        "email": {"type": nullable("string")},
        "email_domain": {"type": nullable("string")},
        "team": {"type": nullable("string")},
    },
}

speaker_schema = {
    "type": nullable("object"),
    "additionalProperties": True,
    "properties": {
        "display_name": {"type": nullable("string")},
        "matched_calendar_invitee_email": {"type": nullable("string")},
    },
}

transcript_line_schema = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "speaker": speaker_schema,
        "text": {"type": nullable("string")},
        "timestamp": {"type": nullable("string")},
    },
}

summary_schema = {
    "type": nullable("object"),
    "additionalProperties": True,
    "properties": {
        "template_name": {"type": nullable("string")},
        "markdown_formatted": {"type": nullable("string")},
    },
}


class MeetingsStream(FathomStream):
    """Fathom meetings."""

    name = "meetings"
    path = "/meetings"
    primary_keys = ["recording_id"]
    replication_key = "created_at"
    records_jsonpath = "$.items[*]"

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "title": {"type": nullable("string")},
            "meeting_title": {"type": nullable("string")},
            "recording_id": {"type": "integer"},
            "url": {"type": nullable("string")},
            "share_url": {"type": nullable("string")},
            "created_at": {"type": nullable("string"), "format": "date-time"},
            "scheduled_start_time": {"type": nullable("string"), "format": "date-time"},
            "scheduled_end_time": {"type": nullable("string"), "format": "date-time"},
            "recording_start_time": {"type": nullable("string"), "format": "date-time"},
            "recording_end_time": {"type": nullable("string"), "format": "date-time"},
            "calendar_invitees_domains_type": {"type": nullable("string")},
            "transcript_language": {"type": nullable("string")},
            "calendar_invitees": array_of({
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "name": {"type": nullable("string")},
                    "email": {"type": nullable("string")},
                    "email_domain": {"type": nullable("string")},
                    "is_external": {"type": nullable("boolean")},
                    "matched_speaker_display_name": {"type": nullable("string")},
                },
            }),
            "recorded_by": person_schema,
            "transcript": array_of(transcript_line_schema),
            "default_summary": summary_schema,
            "action_items": array_of({
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "description": {"type": nullable("string")},
                    "user_generated": {"type": nullable("boolean")},
                    "completed": {"type": nullable("boolean")},
                    "recording_timestamp": {"type": nullable("string")},
                    "recording_playback_url": {"type": nullable("string")},
                    "assignee": person_schema,
                },
            }),
            "crm_matches": {
                "type": nullable("object"),
                "additionalProperties": True,
                "properties": {
                    "contacts": array_of({
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "name": {"type": nullable("string")},
                            "email": {"type": nullable("string")},
                            "record_url": {"type": nullable("string")},
                        },
                    }),
                    "companies": array_of({
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "name": {"type": nullable("string")},
                            "record_url": {"type": nullable("string")},
                        },
                    }),
                    "deals": array_of({
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "name": {"type": nullable("string")},
                            "amount": {"type": nullable("number")},
                            "record_url": {"type": nullable("string")},
                        },
                    }),
                    "error": {"type": nullable("string")},
                },
            },
        },
        "required": ["recording_id"],
    }

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Return meeting list filters and cursor params."""
        params = super().get_url_params(context, next_page_token)

        start = self._starting_datetime(context)
        params["created_after"] = (
            self._format_datetime(start) if start else self._config_start_date()
        )

        domain_type = self.config.get("calendar_invitees_domains_type") or "all"
        params["calendar_invitees_domains_type"] = domain_type

        for config_key, param_key in (
            ("calendar_invitees_domains", "calendar_invitees_domains[]"),
            ("recorded_by", "recorded_by[]"),
            ("teams", "teams[]"),
        ):
            values = self._csv_values(config_key)
            if values:
                params[param_key] = values

        for flag in (
            "include_action_items",
            "include_crm_matches",
            "include_summary",
            "include_transcript",
        ):
            if self.config.get(flag):
                params[flag] = "true"

        return params

    def get_child_context(self, record: dict, context: Context | None) -> dict | None:
        """Pass recording_id to recording child streams."""
        recording_id = record.get("recording_id")
        if recording_id is None:
            return {}
        return {"recording_id": recording_id}

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Normalize primary key type when Fathom returns it as a string."""
        recording_id = row.get("recording_id")
        if recording_id is None:
            self.logger.warning("Skipping meeting without recording_id.")
            return None
        if isinstance(recording_id, str) and recording_id.isdigit():
            row["recording_id"] = int(recording_id)
        return super().post_process(row, context)


class RecordingSummariesStream(FathomStream):
    """Fathom recording summaries fetched per meeting recording."""

    name = "recording_summaries"
    path = "/recordings/{recording_id}/summary"
    parent_stream_type = MeetingsStream
    primary_keys = ["recording_id"]
    records_jsonpath = "$.summary"

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "recording_id": {"type": nullable("integer")},
            "template_name": {"type": nullable("string")},
            "markdown_formatted": {"type": nullable("string")},
            "summary_json": {"type": nullable("string")},
        },
    }

    def request_records(self, context: Context | None):
        """Skip expensive child calls unless explicitly enabled."""
        if not self.config.get("sync_recording_summaries"):
            self.logger.info(
                "Skipping recording_summaries; set sync_recording_summaries=true to enable."
            )
            return
        yield from super().request_records(context)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Summary endpoint is not paginated."""
        return {}

    def parse_response(self, response):
        """Parse a summary response into one Singer record."""
        if response.status_code in (404, 204):
            return
        payload = response.json()
        summary = payload.get("summary")
        if not isinstance(summary, dict):
            return
        yield {
            "template_name": summary.get("template_name"),
            "markdown_formatted": summary.get("markdown_formatted"),
            "summary_json": json.dumps(summary, sort_keys=True),
        }

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Attach parent recording_id."""
        if context:
            row["recording_id"] = context.get("recording_id")
        return row


class RecordingTranscriptsStream(FathomStream):
    """Fathom recording transcripts fetched per meeting recording."""

    name = "recording_transcripts"
    path = "/recordings/{recording_id}/transcript"
    parent_stream_type = MeetingsStream
    primary_keys = ["recording_id"]
    records_jsonpath = "$"

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "recording_id": {"type": nullable("integer")},
            "transcript_line_count": {"type": nullable("integer")},
            "transcript_text": {"type": nullable("string")},
            "transcript_json": {"type": nullable("string")},
        },
    }

    def request_records(self, context: Context | None):
        """Skip expensive child calls unless explicitly enabled."""
        if not self.config.get("sync_recording_transcripts"):
            self.logger.info(
                "Skipping recording_transcripts; set sync_recording_transcripts=true to enable."
            )
            return
        yield from super().request_records(context)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Transcript endpoint is not paginated."""
        return {}

    def parse_response(self, response):
        """Parse a transcript response into one Singer record per recording."""
        if response.status_code in (404, 204):
            return
        payload = response.json()
        transcript = payload.get("transcript")
        if not isinstance(transcript, list):
            return

        lines = [line for line in transcript if isinstance(line, dict)]
        yield {
            "transcript_line_count": len(lines),
            "transcript_text": self._transcript_text(lines),
            "transcript_json": json.dumps(lines, sort_keys=True, ensure_ascii=False),
        }

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Attach parent recording_id."""
        recording_id = context.get("recording_id") if context else None
        row["recording_id"] = recording_id
        return row

    @staticmethod
    def _transcript_text(lines: list[dict]) -> str:
        """Flatten transcript lines into CSV-safe readable text for mapping."""
        rendered: list[str] = []
        for line in lines:
            text = line.get("text")
            if not text:
                continue

            speaker = line.get("speaker")
            speaker_name = None
            if isinstance(speaker, dict):
                speaker_name = speaker.get("display_name")

            timestamp = line.get("timestamp")
            prefix_parts = [part for part in (timestamp, speaker_name) if part]
            prefix = " ".join(str(part) for part in prefix_parts)
            clean_text = RecordingTranscriptsStream._csv_safe_text(str(text))
            rendered.append(f"{prefix}: {clean_text}" if prefix else clean_text)
        return " | ".join(rendered)

    @staticmethod
    def _csv_safe_text(value: str) -> str:
        """Remove physical line breaks and control characters from text fields."""
        return " ".join(value.split())


class TeamsStream(FathomStream):
    """Fathom teams."""

    name = "teams"
    path = "/teams"
    primary_keys = ["name"]
    replication_key = "created_at"
    records_jsonpath = "$.items[*]"

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "name": {"type": nullable("string")},
            "created_at": {"type": nullable("string"), "format": "date-time"},
        },
    }


class TeamMembersStream(FathomStream):
    """Fathom team members."""

    name = "team_members"
    path = "/team_members"
    primary_keys = ["email"]
    replication_key = "created_at"
    records_jsonpath = "$.items[*]"

    schema = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "name": {"type": nullable("string")},
            "email": {"type": nullable("string")},
            "created_at": {"type": nullable("string"), "format": "date-time"},
            "team": {"type": nullable("string")},
        },
    }

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Return team member filters and cursor params."""
        params = super().get_url_params(context, next_page_token)
        team = self.config.get("team_member_team")
        if team:
            params["team"] = team
        return params
