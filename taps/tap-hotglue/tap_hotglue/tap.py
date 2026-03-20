"""HotGlue tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_hotglue.streams import (
    FlowsStream,
    JobsStream,
    LinkedFlowsStream,
    LinkedSourcesStream,
    SourceStateStream,
    TenantConfigStream,
    TenantMappingStream,
    TenantsStream,
)

STREAM_TYPES = [
    TenantsStream,
    FlowsStream,
    TenantConfigStream,
    TenantMappingStream,
    LinkedFlowsStream,
    LinkedSourcesStream,
    SourceStateStream,
    JobsStream,
]


class TapHotglue(Tap):
    """HotGlue platform tap — extracts tenant, flow, job, and config data."""

    name = "tap-hotglue"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="HotGlue API key (x-api-key header)",
        ),
        th.Property(
            "env_id",
            th.StringType,
            required=True,
            description="HotGlue environment ID (e.g. dev.hotglue.optiply.nl)",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapHotglue.cli()
