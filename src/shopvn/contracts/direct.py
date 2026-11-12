"""Direct source contract; source columns and parser schemas have one owner."""

import re

RULE_VERSION = "1.1"
MONEY = "decimal(20,2)"
SCHEMAS = {
    "orders": "order_id string,customer_id int,voucher_code string,channel string,order_date date,status string,subtotal decimal(20,2),shipping_fee decimal(20,2),discount_amount decimal(20,2),total_amount decimal(20,2),payment_method string,payment_status string,created_at timestamp_ntz",
    "order_items": "item_id int,order_id string,product_id int,quantity int,unit_price decimal(20,2),discount_per_item decimal(20,2),total_price decimal(20,2)",
    "returns": "return_id int,order_id string,reason string,status string,refund_amount decimal(20,2),created_at timestamp_ntz",
    "customers": "customer_id int,loyalty_tier string,gender string,city string",
    "products": "product_id int,sku string,name string,category string,subcategory string,brand string,base_price decimal(20,2),cost_price decimal(20,2),weight_gram int,is_active string",
    "vouchers": "voucher_code string,type string,value decimal(20,4),min_order_value decimal(20,2),max_discount decimal(20,2),campaign_id string,valid_from date,valid_to date,max_usage int",
    "inventory": "product_id int,warehouse_id string,stock_qty int,last_updated timestamp_ntz",
    "inventory_transactions": "txn_id int,product_id int,order_id string,type string,qty_change int,created_at timestamp_ntz",
    "product_reviews": "review_id int,order_id string,product_id int,customer_id int,rating int,created_at timestamp_ntz",
}
PRIMARY_KEYS = {
    "orders": "order_id",
    "order_items": "item_id",
    "returns": "return_id",
    "customers": "customer_id",
    "products": "product_id",
    "vouchers": "voucher_code",
    "inventory": "product_id",
    "inventory_transactions": "txn_id",
    "product_reviews": "review_id",
}

CONTRACT_VERSION = 2
FINANCE_TABLES = frozenset(
    {"orders", "order_items", "returns", "customers", "products", "vouchers"}
)


def source_columns(table: str) -> tuple[str, ...]:
    """Extract column names from our explicit DDL; decimal commas are not field separators."""
    return tuple(re.findall(r"(?:^|,)\s*([a-z_]\w*)\s+", SCHEMAS[table]))


EXPORTS = {table: ",".join(source_columns(table)) for table in SCHEMAS}
