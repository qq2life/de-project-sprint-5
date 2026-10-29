CREATE OR REPLACE VIEW dds.v_courier_orders AS
SELECT
    o.order_id,
    o.order_created_ts AS order_ts,
    o.order_total_sum
FROM dds.dm_orders AS o;
