-- PostgreSQL read-only reference; BR02/BR11/BR27, FIN01/FIN02/FIN03.
-- Group returns BEFORE joining to orders to avoid header fanout.
WITH refunded AS (
    SELECT order_id,
           SUM(refund_amount) FILTER (WHERE status = 'refunded') AS refunded_order_amount
    FROM returns
    GROUP BY order_id
)
SELECT o.order_date,
       o.channel,
       COUNT(*) AS eligible_order_count,
       SUM(o.subtotal - o.discount_amount) AS merchandise_before_refund,
       SUM(COALESCE(r.refunded_order_amount, 0)) AS refunded_order_amount,
       SUM(o.total_amount - COALESCE(r.refunded_order_amount, 0)) AS net_customer_amount,
       -- P01: source refund may include shipping, so this measure is a proxy.
       SUM(o.subtotal - o.discount_amount - COALESCE(r.refunded_order_amount, 0))
           AS merchandise_less_order_refund_proxy
FROM orders o
LEFT JOIN refunded r ON o.order_id = r.order_id
WHERE o.status IN ('delivered', 'returned')
  AND o.payment_status IN ('paid', 'refunded')
  AND o.discount_amount BETWEEN 0 AND o.subtotal
GROUP BY o.order_date, o.channel
ORDER BY o.order_date, o.channel;
