# tap-fathom

Fathom External API Singer tap for Optiply, built with `hotglue_singer_sdk`.

## Streams

- `meetings`: cursor-paginated, incremental on `created_at` via `created_after`.
- `recording_summaries`: child stream from `meetings`, disabled unless `sync_recording_summaries=true`.
- `recording_transcripts`: child stream from `meetings`, disabled unless `sync_recording_transcripts=true`; emits one CSV-safe record per recording with `transcript_text`, `transcript_json`, and `transcript_line_count`.
- `teams`: cursor-paginated full table.
- `team_members`: cursor-paginated full table, optionally filtered by `team_member_team`.

## Rate Limits

Fathom documents a 60 requests per 60 seconds global account limit. The tap defaults
to `rate_limit_per_minute=50` and throttles before every request. It also sleeps on
`Retry-After` or `RateLimit-Reset` when Fathom returns a 429 or exhausted
`RateLimit-Remaining`.

Summary and transcript child streams are opt-in because they make one extra API call
per meeting recording.

## Local Smoke

```bash
python3.10 -m venv /tmp/tap-fathom-venv
/tmp/tap-fathom-venv/bin/python -m pip install -e .
/tmp/tap-fathom-venv/bin/tap-fathom --config config.json --discover
```
