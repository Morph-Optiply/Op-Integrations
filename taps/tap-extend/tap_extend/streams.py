"""Stream definitions for tap-extend.

Streams:
  - SuppliersStream:          GET /Supplier          (FULL_TABLE)
  - SupplierAgreementsStream: GET /SupplierAgreement (FULL_TABLE)
  - ProductsStream:           GET /Products          (INCREMENTAL, modifiedDateFrom)
  - CustomerOrdersStream:     GET /CustomerOrders    (INCREMENTAL, modifiedDateFrom)
  - PurchaseOrdersStream:     GET /PurchaseOrders    (INCREMENTAL, createDateFrom)

All streams share ExtendStream base class for auth/HTTP/state handling.

Pagination:
  - Products, CustomerOrders: pageCount + pageOffset
  - Supplier, SupplierAgreement, PurchaseOrders: pageNumber (1-based)
"""

from __future__ import annotations

import base64
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import backoff
import requests

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import Stream

try:
    from hotglue_singer_sdk.exceptions import InvalidCredentialsError
except ImportError:
    class InvalidCredentialsError(Exception):
        """Raised when API credentials are invalid."""


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared base class
# ---------------------------------------------------------------------------


class ExtendStream(Stream):
    """Base class for all Extend Commerce streams."""

    _session: Optional[requests.Session] = None

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            credentials = base64.b64encode(
                f"{self.config['username']}:{self.config['password']}".encode("utf-8")
            ).decode("utf-8")
            self._session.headers.update({
                "ExtendBasicAuthorization": f"Basic {credentials}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            })
        return self._session

    @property
    def base_url(self) -> str:
        api_url = self.config.get("api_url", "https://s05.extend.se/RESTAPI").rstrip("/")
        return f"{api_url}/v1_0/{self.config['client']}"

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, ConnectionError, TimeoutError),
        max_tries=5,
        factor=2,
    )
    def _request(self, url: str, params: Optional[dict] = None) -> requests.Response:
        """GET with retry/backoff."""
        response = self.session.get(url, params=params, timeout=120)

        if response.status_code == 401:
            raise InvalidCredentialsError(
                f"Authentication failed (401): {response.text[:300]}"
            )
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            logger.warning("Rate limited (429). Sleeping %ds.", retry_after)
            time.sleep(retry_after)
            raise requests.exceptions.RequestException("Rate limited (429)")
        if response.status_code >= 500:
            raise requests.exceptions.RequestException(
                f"Server error ({response.status_code}): {response.text[:300]}"
            )

        response.raise_for_status()
        return response

    def _write_state_message(self) -> None:
        """Clean partitions from state to avoid bloat."""
        try:
            tap_state = getattr(getattr(self, "_tap", None), "state", {}) or {}
            for stream_state in tap_state.get("bookmarks", {}).values():
                stream_state.pop("partitions", None)
            super()._write_state_message()
        except Exception as exc:
            self.logger.warning("Error writing state message: %s", exc)


# ---------------------------------------------------------------------------
# SuppliersStream  —  GET /Supplier
# ---------------------------------------------------------------------------


class SuppliersStream(ExtendStream):
    """Extend Commerce Suppliers (company-level).

    Schema from SupplierListItem definition.
    FULL_TABLE — no date filter available on this endpoint.
    Pagination: pageNumber (1-based).
    Response wrapper key: SupplierList.
    """

    name = "suppliers"
    primary_keys = ["supplierNumber"]
    replication_method = "FULL_TABLE"

    schema = th.PropertiesList(
        th.Property("supplierNumber", th.StringType),
        th.Property("name", th.StringType),
        th.Property("shortName", th.StringType),
        th.Property("organizationNumber", th.StringType),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("address3", th.StringType),
        th.Property("postalCode", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("countryId", th.StringType),
        th.Property("companyPhone", th.StringType),
        th.Property("rating", th.StringType),
        th.Property("manualRating", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/Supplier", params={"pageNumber": page}
            ).json()
            items = data.get("SupplierList", [])
            pagination = data.get("paginationInfo", {})

            for s in items:
                yield {
                    "supplierNumber": s.get("supplierNumber"),
                    "name": s.get("name"),
                    "shortName": s.get("shortName"),
                    "organizationNumber": s.get("organizationNumber"),
                    "address1": s.get("address1"),
                    "address2": s.get("address2"),
                    "address3": s.get("address3"),
                    "postalCode": s.get("postalCode"),
                    "city": s.get("city"),
                    "state": s.get("state"),
                    "countryId": s.get("countryId"),
                    "companyPhone": s.get("companyPhone"),
                    "rating": s.get("rating"),
                    "manualRating": s.get("manualRating"),
                }

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1


# ---------------------------------------------------------------------------
# SupplierAgreementsStream  —  GET /SupplierAgreement
# ---------------------------------------------------------------------------


class SupplierAgreementsStream(ExtendStream):
    """Extend Commerce Supplier Agreements (contract/brand level).

    These are the entities used as Optiply Suppliers — they carry lead times,
    currency and payment terms needed for replenishment. Each agreement maps
    to one purchasing contract (e.g. "Gandalf - Corsair", "Fifine").

    NOTE: supplierNumber (FK to parent Supplier) is NOT returned by the list
    endpoint — only by GET /SupplierAgreement/{id}. Omitted here to avoid
    525 extra detail calls. Add if the ETL needs the company-level grouping.

    Schema from SupplierAgreementListItem definition.
    FULL_TABLE — no change-date filter available on this endpoint.
    Pagination: pageNumber (1-based).
    Response wrapper key: SupplierAgreementList.
    """

    name = "supplier_agreements"
    primary_keys = ["supplierAgreementNumber"]
    replication_method = "FULL_TABLE"

    schema = th.PropertiesList(
        th.Property("supplierAgreementNumber", th.IntegerType),
        th.Property("name", th.StringType),
        # supplierNumber (FK to Supplier) only on detail endpoint — not in list response
        th.Property("active", th.BooleanType),
        th.Property("currencyId", th.StringType),
        th.Property("manufacturingLeadTime", th.NumberType),
        th.Property("transportLeadTime", th.NumberType),
        th.Property("customsLeadTime", th.NumberType),
        th.Property("paymentTerms", th.StringType),
        th.Property("deliveryMethod", th.StringType),
        th.Property("incoterms", th.StringType),
        th.Property("validFrom", th.DateTimeType),
        th.Property("validTo", th.DateTimeType),
        th.Property("ordererAddress1", th.StringType),
        th.Property("ordererPostalCode", th.StringType),
        th.Property("ordererCity", th.StringType),
        th.Property("ordererCountryId", th.StringType),
        th.Property("makeAutomaticPurchase", th.BooleanType),
        th.Property("capacity", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/SupplierAgreement", params={"pageNumber": page}
            ).json()
            items = data.get("SupplierAgreementList", [])
            pagination = data.get("paginationInfo", {})

            for a in items:
                yield {
                    "supplierAgreementNumber": a.get("supplierAgreementNumber"),
                    "name": a.get("name"),
                    "active": a.get("active"),
                    "currencyId": a.get("currencyId"),
                    "manufacturingLeadTime": a.get("manufacturingLeadTime"),
                    "transportLeadTime": a.get("transportLeadTime"),
                    "customsLeadTime": a.get("customsLeadTime"),
                    "paymentTerms": a.get("paymentTerms"),
                    "deliveryMethod": a.get("deliveryMethod"),
                    "incoterms": a.get("incoterms"),
                    "validFrom": a.get("validFrom"),
                    "validTo": a.get("validTo"),
                    "ordererAddress1": a.get("ordererAddress1"),
                    "ordererPostalCode": a.get("ordererPostalCode"),
                    "ordererCity": a.get("ordererCity"),
                    "ordererCountryId": a.get("ordererCountryId"),
                    "makeAutomaticPurchase": a.get("makeAutomaticPurchase"),
                    "capacity": a.get("capacity"),
                }

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1


# ---------------------------------------------------------------------------
# ProductsStream  —  GET /Products  +  GET /Products/{id}
# ---------------------------------------------------------------------------


class ProductsStream(ExtendStream):
    """Extend Commerce Products with per-warehouse stock and supplier agreements.

    List endpoint (GET /Products) returns one row per product-per-warehouse.
    This stream deduplicates by productNumber, aggregating warehouse stock
    into a JSON array. For each unique product the detail endpoint is called
    to retrieve productSuppliers (= ProductSupplierAgreement data).

    Incremental via modifiedDateFrom server-side filter.
    Pagination: pageCount + pageOffset (NOT pageNumber).
    Replication key: createDate (only date field present on list response).

    Schema from ProductListItem + ProductSupplier definitions.
    """

    name = "products"
    primary_keys = ["productNumber"]
    replication_key = "createDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # ProductListItem fields
        th.Property("productNumber", th.StringType),
        th.Property("productName", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("productUnit", th.StringType),
        th.Property("cost", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("countryOfOrigin", th.StringType),
        th.Property("supplyMode", th.StringType),
        th.Property("manufacturer", th.StringType),
        th.Property("manufacturerProductNumber", th.StringType),
        th.Property("gtinNumberList", th.StringType),
        th.Property("enabled", th.BooleanType),
        th.Property("statisticalCategory1", th.StringType),
        th.Property("statisticalCategory2", th.StringType),
        th.Property("statisticalCategory3", th.StringType),
        th.Property("companyGroup", th.StringType),
        th.Property("financialCategory", th.StringType),
        # Aggregated per-warehouse stock as JSON: [{warehouse, availableBalance}]
        th.Property("warehouse_stock", th.StringType),
        # ProductSupplierAgreement list as JSON from GET /Products/{id}
        th.Property("product_suppliers", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        seen: dict[str, dict] = {}
        stock_map: dict[str, list] = {}

        start_replication = self.get_starting_replication_key_value(context)
        if start_replication:
            modified_date_from = str(start_replication)
        elif self.config.get("start_date"):
            modified_date_from = str(self.config["start_date"])
        else:
            modified_date_from = None

        page_offset = 0
        page_count = 100

        while True:
            params: dict[str, Any] = {"pageCount": page_count, "pageOffset": page_offset}
            if modified_date_from:
                params["modifiedDateFrom"] = modified_date_from

            product_list = self._request(
                f"{self.base_url}/Products", params=params
            ).json()
            if not isinstance(product_list, list) or not product_list:
                break

            for p in product_list:
                pn = str(p.get("productNumber") or "")
                if not pn:
                    continue

                # Accumulate per-warehouse stock (list returns one row per warehouse)
                stock_map.setdefault(pn, []).append({
                    "warehouse": p.get("warehouse") or "",
                    "availableBalance": p.get("availableBalance") or 0,
                })

                if pn not in seen:
                    groups = p.get("productGroupsAndCategories") or {}
                    seen[pn] = {
                        "productNumber": pn,
                        "productName": p.get("productName"),
                        "createDate": p.get("createDate"),
                        "productUnit": p.get("productUnit"),
                        "cost": p.get("cost"),
                        "currency": p.get("currency"),
                        "countryOfOrigin": p.get("countryOfOrigin"),
                        "supplyMode": p.get("supplyMode"),
                        "manufacturer": p.get("manufacturer"),
                        "manufacturerProductNumber": p.get("manufacturerProductNumber"),
                        "gtinNumberList": p.get("gtinNumberList"),
                        "enabled": p.get("enabled"),
                        "statisticalCategory1": p.get("statisticalCategory1"),
                        "statisticalCategory2": p.get("statisticalCategory2"),
                        "statisticalCategory3": p.get("statisticalCategory3"),
                        "companyGroup": groups.get("companyGroup"),
                        "financialCategory": groups.get("financialCategory"),
                    }

            if len(product_list) < page_count:
                break
            page_offset += page_count

        logger.info("Products: %d unique products", len(seen))

        for pn, record in seen.items():
            record["warehouse_stock"] = json.dumps(stock_map.get(pn, []))
            record["product_suppliers"] = json.dumps(self._fetch_product_suppliers(pn))
            yield record

    def _fetch_product_suppliers(self, product_number: str) -> list:
        """Fetch productSuppliers from GET /Products/{productNumber}.

        Returns the productSuppliers array from the detail response.
        Fields per ProductSupplier: supplierAgreement, supplierProductNumber,
        supplierProductName, price, vatPercent, country,
        manufacturingLeadTimeHour, supplierAgreementNumber, supplierCurrency.
        """
        try:
            detail = self._request(f"{self.base_url}/Products/{product_number}").json()
            suppliers = detail.get("productSuppliers", [])
            return suppliers if isinstance(suppliers, list) else []
        except Exception:
            logger.warning("Failed to fetch suppliers for product %s", product_number, exc_info=True)
            return []


# ---------------------------------------------------------------------------
# CustomerOrdersStream  —  GET /CustomerOrders  +  GET /CustomerOrders/{id}
# ---------------------------------------------------------------------------


class CustomerOrdersStream(ExtendStream):
    """Extend Commerce CustomerOrders with full detail (header + rows).

    List endpoint returns CustomerOrderListItem summaries.
    Detail endpoint is called per order to get orderRows.
    Rows serialised as JSON string for ETL processing.

    CRITICAL: Always pass modifiedDateFrom — without it the endpoint times out.
    Pagination: pageCount + pageOffset (NOT pageNumber).
    Replication key: changeDate (from CustomerOrderListItem).

    Schema from CustomerOrderListItem + CustomerOrderRow definitions.
    """

    name = "customer_orders"
    primary_keys = ["orderNumber"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # CustomerOrderListItem fields
        th.Property("orderNumber", th.StringType),
        th.Property("orderNumberExternal", th.StringType),
        th.Property("orderType", th.StringType),
        th.Property("orderStatus", th.StringType),
        th.Property("orderDate", th.DateTimeType),
        th.Property("askedDeliveryDate", th.DateTimeType),
        th.Property("slaDate", th.DateTimeType),
        th.Property("customerNumber", th.StringType),
        th.Property("customerName", th.StringType),
        th.Property("totalPrice", th.NumberType),
        th.Property("changeDate", th.DateTimeType),
        # Detail: CustomerOrderRow list as JSON
        # Key fields per row: position, orderRowStatus,
        #   product.productNumber, salesData.quantity, salesData.unitPrice,
        #   salesData.vatPercent, salesData.currency, warehouse
        th.Property("order_rows", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        start_replication = self.get_starting_replication_key_value(context)
        if start_replication:
            modified_date_from = str(start_replication)
        elif self.config.get("start_date"):
            modified_date_from = str(self.config["start_date"])
        else:
            two_years_ago = datetime.now(timezone.utc) - timedelta(days=730)
            modified_date_from = two_years_ago.strftime("%Y-%m-%dT%H:%M:%S")

        page_offset = 0
        page_count = 100

        while True:
            order_list = self._request(
                f"{self.base_url}/CustomerOrders",
                params={
                    "pageCount": page_count,
                    "pageOffset": page_offset,
                    "modifiedDateFrom": modified_date_from,
                },
            ).json()

            if not isinstance(order_list, list) or not order_list:
                break

            for o in order_list:
                order_number = str(o.get("orderNumber") or "")
                if not order_number:
                    continue

                yield {
                    "orderNumber": order_number,
                    "orderNumberExternal": o.get("orderNumberExternal"),
                    "orderType": o.get("orderType"),
                    "orderStatus": o.get("orderStatus"),
                    "orderDate": o.get("orderDate"),
                    "askedDeliveryDate": o.get("askedDeliveryDate"),
                    "slaDate": o.get("slaDate"),
                    "customerNumber": str(o.get("customerNumber") or ""),
                    "customerName": o.get("customerName"),
                    "totalPrice": o.get("totalPrice"),
                    "changeDate": o.get("changeDate"),
                    "order_rows": json.dumps(self._fetch_order_rows(order_number)),
                }

            if len(order_list) < page_count:
                break
            page_offset += page_count

    def _fetch_order_rows(self, order_number: str) -> list:
        """Fetch orderRows from GET /CustomerOrders/{id}.

        Response: {orderHeader: {...}, orderRows: [...]}.
        Key fields per CustomerOrderRow:
          position, orderRowStatus, supplyMode, warehouse,
          product.productNumber, product.productName,
          salesData.quantity, salesData.unitPrice, salesData.vatPercent,
          salesData.currency, expectedDeliveryDate, changeDate.
        """
        try:
            detail = self._request(f"{self.base_url}/CustomerOrders/{order_number}").json()
            rows = detail.get("orderRows", [])
            return rows if isinstance(rows, list) else []
        except Exception:
            logger.warning("Failed to fetch rows for order %s", order_number, exc_info=True)
            return []


# ---------------------------------------------------------------------------
# PurchaseOrdersStream  —  GET /PurchaseOrders  +  GET /PurchaseOrders/{id}
# ---------------------------------------------------------------------------


class PurchaseOrdersStream(ExtendStream):
    """Extend Commerce PurchaseOrders with full detail.

    List endpoint returns PurchaseOrderListItem summaries.
    Detail endpoint is called per PO for header (incl. supplierAgreementNumber),
    rows, and shipments.

    Incremental via createDateFrom.
    Pagination: pageNumber (1-based).
    Replication key: createDate (from PurchaseOrderListItem).

    Schema from PurchaseOrderListItem + PurchaseOrderSupplier +
    PurchaseOrderRow definitions.
    """

    name = "purchase_orders"
    primary_keys = ["purchaseNumber"]
    replication_key = "createDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # PurchaseOrderListItem fields
        th.Property("purchaseNumber", th.StringType),
        th.Property("status", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("warehouse", th.StringType),
        th.Property("isOpen", th.BooleanType),
        th.Property("isReceived", th.BooleanType),
        th.Property("externalOrderNumber", th.StringType),
        th.Property("supplierOrderNumber", th.StringType),
        th.Property("shippedDate", th.DateTimeType),
        # PurchaseOrderSupplier fields (from detail header)
        th.Property("supplierNumber", th.StringType),
        th.Property("supplierName", th.StringType),
        th.Property("supplierAgreementNumber", th.IntegerType),  # FK to SupplierAgreement
        th.Property("supplierAgreement", th.StringType),
        th.Property("paymentTerms", th.StringType),
        th.Property("deliveryMethod", th.StringType),
        th.Property("deliveryMethodName", th.StringType),
        th.Property("transportCondition", th.StringType),
        th.Property("forwarder", th.StringType),
        # Header fields
        th.Property("reference", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("requestedDeliveryDate", th.DateTimeType),
        # Delivery address (flattened)
        th.Property("deliveryAddress_name1", th.StringType),
        th.Property("deliveryAddress_name2", th.StringType),
        th.Property("deliveryAddress_address1", th.StringType),
        th.Property("deliveryAddress_postalCode", th.StringType),
        th.Property("deliveryAddress_city", th.StringType),
        th.Property("deliveryAddress_countryCode", th.StringType),
        # Buyer contact (flattened)
        th.Property("buyerContactName", th.StringType),
        th.Property("buyerContactEmail", th.StringType),
        # PurchaseOrderRow list as JSON
        # Key fields per row: position, rowStatus, productNumber, productName,
        #   supplierProductNumber, purchaseDataProductUnit.quantity,
        #   purchaseDataProductUnit.unitPrice, purchaseDataProductUnit.vatPercent,
        #   purchaseDataProductUnit.currency, expectedDeliveryDate
        th.Property("rows", th.StringType),
        # PurchaseOrderShipment list as JSON (populated when isReceived=True)
        th.Property("shipments", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        warehouse_codes = self.config.get("warehouse_codes")
        start_replication = self.get_starting_replication_key_value(context)

        params_base: dict[str, Any] = {}
        if start_replication:
            params_base["createDateFrom"] = str(start_replication)
        elif self.config.get("start_date"):
            params_base["createDateFrom"] = str(self.config["start_date"])

        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/PurchaseOrders",
                params={**params_base, "pageNumber": page},
            ).json()

            po_list = data.get("purchaseOrderList", [])
            pagination = data.get("paginationInfo", {})

            if not po_list:
                break

            for po in po_list:
                purchase_number = po.get("purchaseNumber")
                if not purchase_number:
                    continue
                if warehouse_codes and po.get("warehouse") not in warehouse_codes:
                    continue

                detail = self._fetch_detail(purchase_number)
                if detail:
                    yield self._map_detail(detail, po)
                else:
                    yield self._map_summary(po)

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1

    def _fetch_detail(self, purchase_number: str) -> Optional[dict]:
        try:
            return self._request(f"{self.base_url}/PurchaseOrders/{purchase_number}").json()
        except Exception:
            logger.warning("Failed to fetch detail for PO %s", purchase_number, exc_info=True)
            return None

    def _map_detail(self, detail: dict, summary: dict) -> dict:
        header = detail.get("header", {}) or {}
        rows = detail.get("rows", [])
        shipments = detail.get("shipments", [])
        supplier = header.get("supplier", {}) or {}
        delivery_addr = header.get("deliveryAddress", {}) or {}
        buyer = header.get("buyerContact", {}) or {}

        return {
            "purchaseNumber": header.get("purchaseNumber") or summary.get("purchaseNumber"),
            "status": header.get("status") or summary.get("status"),
            "createDate": header.get("createDate") or summary.get("createDate"),
            "warehouse": header.get("warehouse") or summary.get("warehouse"),
            "isOpen": summary.get("isOpen", True),
            "isReceived": summary.get("isReceived", False),
            "externalOrderNumber": header.get("externalOrderNumber") or summary.get("externalOrderNumber", ""),
            "supplierOrderNumber": header.get("supplierOrderNumber") or summary.get("supplierOrderNumber", ""),
            "shippedDate": header.get("shippedDate") or summary.get("shippedDate"),
            # PurchaseOrderSupplier
            "supplierNumber": supplier.get("supplierNumber") or summary.get("supplierNumber"),
            "supplierName": supplier.get("supplierName") or summary.get("supplierName"),
            "supplierAgreementNumber": supplier.get("supplierAgreementNumber"),
            "supplierAgreement": supplier.get("supplierAgreement"),
            "paymentTerms": supplier.get("paymentTerms"),
            "deliveryMethod": supplier.get("deliveryMethod"),
            "deliveryMethodName": supplier.get("deliveryMethodName"),
            "transportCondition": supplier.get("transportCondition"),
            "forwarder": supplier.get("forwarder"),
            # Header
            "reference": header.get("reference", ""),
            "notes": header.get("notes", ""),
            "requestedDeliveryDate": header.get("requestedDeliveryDate"),
            # Delivery address
            "deliveryAddress_name1": delivery_addr.get("name1"),
            "deliveryAddress_name2": delivery_addr.get("name2"),
            "deliveryAddress_address1": delivery_addr.get("address1"),
            "deliveryAddress_postalCode": delivery_addr.get("postalCode"),
            "deliveryAddress_city": delivery_addr.get("city"),
            "deliveryAddress_countryCode": delivery_addr.get("countryCode"),
            # Buyer contact
            "buyerContactName": buyer.get("name"),
            "buyerContactEmail": buyer.get("email"),
            # Nested
            "rows": json.dumps(rows),
            "shipments": json.dumps(shipments),
        }

    def _map_summary(self, summary: dict) -> dict:
        """Fallback when detail fetch fails — summary fields only."""
        return {
            "purchaseNumber": summary.get("purchaseNumber"),
            "status": summary.get("status"),
            "createDate": summary.get("createDate"),
            "warehouse": summary.get("warehouse"),
            "isOpen": summary.get("isOpen", True),
            "isReceived": summary.get("isReceived", False),
            "externalOrderNumber": summary.get("externalOrderNumber", ""),
            "supplierOrderNumber": summary.get("supplierOrderNumber", ""),
            "shippedDate": summary.get("shippedDate"),
            "supplierNumber": summary.get("supplierNumber"),
            "supplierName": summary.get("supplierName"),
            "supplierAgreementNumber": None,
            "supplierAgreement": None,
            "paymentTerms": None,
            "deliveryMethod": None,
            "deliveryMethodName": None,
            "transportCondition": None,
            "forwarder": None,
            "reference": None,
            "notes": None,
            "requestedDeliveryDate": None,
            "deliveryAddress_name1": None,
            "deliveryAddress_name2": None,
            "deliveryAddress_address1": None,
            "deliveryAddress_postalCode": None,
            "deliveryAddress_city": None,
            "deliveryAddress_countryCode": None,
            "buyerContactName": None,
            "buyerContactEmail": None,
            "rows": "[]",
            "shipments": "[]",
        }
