-- Certified direct Finance output after source/Spark/Trino reconciliation.
-- FIN02 includes customer shipping and subtracts only refunded returns.
SELECT order_date,
       channel,
       eligible_order_count,
       net_customer_amount,
       refunded_order_amount,
       merchandise_less_order_refund_proxy,
       rule_version,
       publication_id
FROM lakehouse.analytics.mart_fin02_daily
ORDER BY order_date, channel;
