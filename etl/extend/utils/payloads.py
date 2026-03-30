import datetime

import pandas as pd

from utils.models import (
    BuyOrder,
    BuyOrderLine,
    Product,
    ProductCompositions,
    ReceiptLine,
    SellOrder,
    SellOrderLine,
    SellOrderWithLines,
    Supplier,
    SupplierProduct,
)
from utils.tools import nan_to_none
from utils.utils import clean_payload


def get_product_payload(row):
    action = "POST" if row.get('optiply_id') is None else "PATCH"

    if action == "POST" and 'unlimitedStock' not in row:
        unlimited_stock = False
    else:
        unlimited_stock = None if 'unlimitedStock' not in row else bool(row['unlimitedStock'])

    product = {
        "remoteId": row.get('remoteId', None),
        "name": row.get('name', None),
        "skuCode": row.get('skuCode', None),
        "articleCode": row.get('articleCode', None),
        "price": nan_to_none(row.get('price', None)),
        "unlimitedStock": unlimited_stock,
        "stockLevel": row.get('stockLevel', None),
        "status": row.get('status', None),
        "eanCode": row.get('eanCode', None),
        "assembled": row.get('assembled', None),
        "createdAtRemote": None if pd.isna(row.get('created_at')) else row.get('created_at'),
        "notBeingBought": None if 'notBeingBought' not in row else bool(row['notBeingBought']),
        "minimumStock": row.get('minimumStock', None),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    product = Product(**product)
    product_payload = clean_payload(product.dict())
    return product_payload


def get_product_compositions_payload(row):
    product_compositions = {
        "composedProductId": row.get('composedProductId', None),
        "partProductId": row.get('partProductId', None),
        "partQuantity": row.get('partQuantity', None),
        "remoteId": row.get('remoteId', None),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    product_compositions = ProductCompositions(**product_compositions)
    product_compositions_payload = clean_payload(product_compositions.dict())
    return product_compositions_payload


def get_supplier_payload(row):
    emails = []
    if row.get('emails'):
        emails.append(row.get('emails', None))

    supplier = {
        "name": row.get('name', None),
        "remoteId": row.get('remoteId', None),
        "emails": emails if len(emails) else None,
        "ignored": row.get('ignored', None),
        "deliveryTime": nan_to_none(row.get('deliveryTime', None)),
        "fixedCosts": nan_to_none(row.get('fixedCosts', None)),
        "userReplenishmentPeriod": nan_to_none(row.get('userReplenishmentPeriod', None)),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    supplier = Supplier(**supplier)
    supplier_payload = clean_payload(supplier.dict())
    return supplier_payload


def get_supplier_product_payload(row):
    supplier_product = {
        "name": row.get('name', None),
        "remoteId": row.get('remoteId', None),
        "skuCode": row.get('skuCode', None),
        "articleCode": nan_to_none(row.get('articleCode', None)),
        "status": row.get('status', None),
        "eanCode": row.get('eanCode', None),
        "preferred": row.get('preferred', None),
        "price": nan_to_none(row.get('price', None)),
        "deliveryTime": nan_to_none(row.get('deliveryTime', None)),
        "productId": row.get('productId', None),
        "supplierId": row.get('supplierId', None),
        "lotSize": nan_to_none(row.get('lotSize', None)),
        "minimumPurchaseQuantity": nan_to_none(row.get('minimumPurchaseQuantity', None)),
        "weight": nan_to_none(row.get('weight', None)),
        "volume": nan_to_none(row.get('volume', None)),
        "freeStock": nan_to_none(row.get('freeStock', None)),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    supplier_product = SupplierProduct(**supplier_product)
    supplier_product_payload = clean_payload(supplier_product.dict())
    return supplier_product_payload


def get_sell_order_withlines_payload(row):
    sell_order_withlines = {
        "totalValue": row.get('totalValue', None),
        "remoteId": row.get('remoteId', None),
        "placed": row.get('placed', None),
        "orderLines": row.get('lines', None),
        "completed": nan_to_none(row.get('completed', None)),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    sell_order_withlines = SellOrderWithLines(**sell_order_withlines)
    sell_order_withlines_payload = clean_payload(sell_order_withlines.dict())
    return sell_order_withlines_payload


def get_sell_order_payload(row):
    sell_order = {
        "totalValue": row.get('totalValue', None),
        "remoteId": row.get('remoteId', None),
        "placed": row.get('placed', None),
        "completed": nan_to_none(row.get('completed', None)),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    sell_order = SellOrder(**sell_order)
    sell_order_payload = clean_payload(sell_order.dict())
    return sell_order_payload


def get_sell_order_line_payload(row):
    sell_order_line = {
        "quantity": row.get('quantity', None),
        "sellOrderId": row.get('sellOrderId', None),
        "productId": row.get('productId', None),
        "subtotalValue": row.get('subtotalValue', None),
        "remoteId": row.get('remoteId', None)
    }

    sell_order_line = SellOrderLine(**sell_order_line)
    sell_order_line_payload = clean_payload(sell_order_line.dict())
    return sell_order_line_payload


def get_buy_order_payload(row):
    buy_order = {
        "remoteId": row.get('remoteId', None),
        "supplierId": row.get('supplierId', None),
        "placed": row.get('placed', None),
        "totalValue": row.get('totalValue', None),
        "completed": nan_to_none(row.get('completed', None)),
        "expectedDeliveryDate": nan_to_none(row.get('expectedDeliveryDate', None)),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    buy_order = BuyOrder(**buy_order)
    buy_order_payload = clean_payload(buy_order.dict())
    return buy_order_payload


def get_buy_order_line_payload(row):
    buy_order_line = {
        "productId": row.get('productId', None),
        "quantity": row.get('quantity', None),
        "subtotalValue": row.get('subtotalValue', None),
        "buyOrderId": row.get('buyOrderId', None),
        "expectedDeliveryDate": nan_to_none(row.get('expectedDeliveryDate', None)),
        "remoteId": row.get('remoteId', None),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    buy_order_line = BuyOrderLine(**buy_order_line)
    buy_order_line_payload = clean_payload(buy_order_line.dict())
    return buy_order_line_payload


def get_receipt_line_payload(row):
    receipt_line = {
        "occurred": row.get('occurred', None),
        "quantity": row.get('quantity', None),
        "buyOrderLineId": row.get('buyOrderLineId', None),
        "remoteId": row.get('remoteId', None),
        "remoteDataSyncedToDate": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    receipt_line = ReceiptLine(**receipt_line)
    receipt_line_payload = clean_payload(receipt_line.dict())
    return receipt_line_payload
