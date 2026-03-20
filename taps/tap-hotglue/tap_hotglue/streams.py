"""Stream type classes for tap-hotglue."""

from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hotglue.client import HotglueStream


# ---------------------------------------------------------------------------
# Standalone streams
# ---------------------------------------------------------------------------


class TenantsStream(HotglueStream):
    """All tenants in the environment."""

    name = "tenants"
    primary_keys = ["tenant_id"]
    replication_key = None
    records_jsonpath = "$[*]"

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("tenant_name", th.StringType),
        th.Property("tap", th.StringType),
        th.Property("target", th.StringType),
        th.Property("status", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("last_sync", th.DateTimeType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/tenants/{self.env_id}"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Pass tenant_id to child streams."""
        return {"tenant_id": record["tenant_id"]}


class FlowsStream(HotglueStream):
    """All supported flows (connectors) in the environment."""

    name = "flows"
    primary_keys = ["flow_id"]
    replication_key = None
    records_jsonpath = "$[*]"

    schema = th.PropertiesList(
        th.Property("flow_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("tap", th.StringType),
        th.Property("target", th.StringType),
        th.Property("default_tap", th.StringType),
        th.Property("default_target", th.StringType),
        th.Property("domain", th.StringType),
        th.Property("schedule", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/flows/supported"


# ---------------------------------------------------------------------------
# Per-tenant streams (children of TenantsStream)
# ---------------------------------------------------------------------------


class TenantConfigStream(HotglueStream):
    """Tenant configuration (credentials redacted by API harness)."""

    name = "tenant_config"
    primary_keys = ["tenant_id"]
    replication_key = None
    records_jsonpath = "$"
    parent_stream_type = TenantsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("config", th.CustomType({"type": ["object", "string"]})),
        th.Property("api_credentials", th.CustomType({"type": ["object", "string"]})),
        th.Property("account_id", th.StringType),
        th.Property("coupling_id", th.StringType),
        th.Property("webshop_handle", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/tenant/{self.env_id}/{{tenant_id}}/config"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """Inject tenant_id from context."""
        if context:
            row["tenant_id"] = context["tenant_id"]
        return row


class TenantMappingStream(HotglueStream):
    """Field mapping configuration per tenant."""

    name = "tenant_mapping"
    primary_keys = ["tenant_id"]
    replication_key = None
    records_jsonpath = "$"
    parent_stream_type = TenantsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("mapping", th.CustomType({"type": ["object", "array", "string"]})),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/tenant/{self.env_id}/{{tenant_id}}/mapping"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Wrap the mapping response into a record."""
        if response.status_code == 404:
            return

        try:
            data = response.json()
        except Exception:
            return

        yield {"mapping": data}

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
        return row


class LinkedFlowsStream(HotglueStream):
    """Flows linked to each tenant — bridges tenants to flow-level data."""

    name = "linked_flows"
    primary_keys = ["tenant_id", "flow_id"]
    replication_key = None
    records_jsonpath = "$[*]"
    parent_stream_type = TenantsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("tap", th.StringType),
        th.Property("target", th.StringType),
        th.Property("type", th.StringType),
        th.Property("schedule", th.StringType),
        th.Property("status", th.StringType),
        th.Property("last_sync", th.DateTimeType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/flows/linked"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = {}
        if context:
            params["tenant_id"] = context["tenant_id"]
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
        return row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Pass tenant_id + flow_id to grandchild streams."""
        return {
            "tenant_id": record.get("tenant_id") or (context or {}).get("tenant_id"),
            "flow_id": record.get("flow_id"),
        }


# ---------------------------------------------------------------------------
# Per-tenant-flow streams (children of LinkedFlowsStream)
# ---------------------------------------------------------------------------


class LinkedSourcesStream(HotglueStream):
    """Source configuration per tenant/flow (auth credentials redacted)."""

    name = "linked_sources"
    primary_keys = ["tenant_id", "flow_id", "tap"]
    replication_key = None
    records_jsonpath = "$[*]"
    parent_stream_type = LinkedFlowsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("tap", th.StringType),
        th.Property("config", th.CustomType({"type": ["object", "string"]})),
        th.Property("field_map", th.CustomType({"type": ["object", "array", "string"]})),
        th.Property("status", th.StringType),
        th.Property("incremental", th.BooleanType),
        th.Property("schedule", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/{{flow_id}}/{{tenant_id}}/linkedSources"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {"config": "true"}

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
            row["flow_id"] = context["flow_id"]
        return row


class SourceStateStream(HotglueStream):
    """Source bookmark/state for incremental sync tracking per tenant/flow."""

    name = "source_state"
    primary_keys = ["tenant_id", "flow_id"]
    replication_key = None
    records_jsonpath = "$"
    parent_stream_type = LinkedFlowsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("state", th.CustomType({"type": ["object", "array", "string"]})),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/{{flow_id}}/{{tenant_id}}/linkedSources/state"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if response.status_code == 404:
            return

        try:
            data = response.json()
        except Exception:
            return

        yield {"state": data}

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
            row["flow_id"] = context["flow_id"]
        return row


class JobsStream(HotglueStream):
    """Job history per tenant/flow."""

    name = "jobs"
    primary_keys = ["tenant_id", "flow_id", "job_id"]
    replication_key = "start_time"
    records_jsonpath = "$[*]"
    parent_stream_type = LinkedFlowsStream

    schema = th.PropertiesList(
        th.Property("tenant_id", th.StringType),
        th.Property("flow_id", th.StringType),
        th.Property("job_id", th.StringType),
        th.Property("job_name", th.StringType),
        th.Property("s3_root", th.StringType),
        th.Property("status", th.StringType),
        th.Property("tap", th.StringType),
        th.Property("target", th.StringType),
        th.Property("start_time", th.DateTimeType),
        th.Property("stop_time", th.DateTimeType),
        th.Property("task_count", th.IntegerType),
        th.Property("num_records", th.IntegerType),
        th.Property("error_count", th.IntegerType),
        th.Property("error_message", th.StringType),
        th.Property("scheduled", th.BooleanType),
        th.Property("env_id", th.StringType),
    ).to_dict()

    @property
    def path(self) -> str:
        return f"/{self.env_id}/{{flow_id}}/{{tenant_id}}/jobs"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        if context:
            row["tenant_id"] = context["tenant_id"]
            row["flow_id"] = context["flow_id"]
        return row
