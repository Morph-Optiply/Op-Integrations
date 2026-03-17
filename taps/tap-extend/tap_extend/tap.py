"""TapExtend -- Singer tap for the Extend Commerce (Lxir) REST API."""

from __future__ import annotations

from typing import List

from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_extend.streams import (
    CustomerOrdersStream,
    ProductSupplierAgreementsStream,
    ProductsStream,
    PurchaseOrdersStream,
    SupplierAgreementsStream,
    SuppliersStream,
)


class TapExtend(Tap):
    """Singer tap for Extend Commerce (Lxir) REST API.

    Streams:
      - suppliers                     FULL_TABLE  GET /Supplier
      - supplier_agreements           FULL_TABLE  GET /SupplierAgreement
      - product_supplier_agreements   FULL_TABLE  GET /ProductSupplierAgreements
      - products                      INCREMENTAL GET /Products (modifiedDateFrom)
      - customer_orders               INCREMENTAL GET /CustomerOrders (modifiedDateFrom)
      - purchase_orders               INCREMENTAL GET /PurchaseOrders (createDateFrom)
    """

    name = "tap-extend"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            default="https://s05.extend.se/RESTAPI",
            description="Base URL for the Extend Commerce REST API",
        ),
        th.Property(
            "client",
            th.StringType,
            required=True,
            description="Client shortname (e.g. MAXGAMING)",
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="Username for Extend API authentication",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Password for Extend API authentication",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="Earliest record date to sync (ISO 8601)",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            required=False,
            description="Latest record date to sync (ISO 8601). Used as upper bound on incremental streams.",
        ),
        th.Property(
            "warehouse_codes",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "Optional list of warehouse codes to include. "
                "If omitted, all warehouses are synced. "
                "Recommended: exclude MAXRMA (RMA/returns warehouse)."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List:
        """Return stream instances."""
        return [
            SuppliersStream(tap=self),
            SupplierAgreementsStream(tap=self),
            ProductSupplierAgreementsStream(tap=self),
            ProductsStream(tap=self),
            CustomerOrdersStream(tap=self),
            PurchaseOrdersStream(tap=self),
        ]


if __name__ == "__main__":
    TapExtend.cli()
