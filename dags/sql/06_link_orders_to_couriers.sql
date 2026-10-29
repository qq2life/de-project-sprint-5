-- Run AFTER delivery STG -> DDS. Idempotent and based on latest delivery per order.
UPDATE dds.dm_orders AS o
SET courier_id = d.courier_id,
    updated_at = NOW()
FROM (
    SELECT DISTINCT ON (f.order_id) f.order_id, f.courier_id
    FROM dds.fct_deliveries AS f
    ORDER BY f.order_id, f.delivery_ts DESC, f.delivery_id DESC
) AS d
WHERE o.order_id = d.order_id
  AND o.courier_id IS DISTINCT FROM d.courier_id;
