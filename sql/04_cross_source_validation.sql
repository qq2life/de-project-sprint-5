-- Cross-source integrity & financial validation (read only).
SELECT
    (SELECT COUNT(*) FROM stg.mongo_orders) AS mongo_stg,
    (SELECT COUNT(*) FROM dds.dm_orders) AS mongo_dds,
    (SELECT COUNT(*) FROM stg.api_deliveries) AS deliveries_stg,
    (SELECT COUNT(*) FROM dds.fct_deliveries) AS deliveries_dds,
    COUNT(*) FILTER (WHERE o.order_id IS NULL) AS missing_mongo_orders,
    COUNT(*) FILTER (WHERE o.order_id IS NOT NULL AND v.order_ts IS DISTINCT FROM o.order_created_ts)
       AS wrong_reporting_dates
FROM dds.fct_deliveries d
LEFT JOIN dds.dm_orders o ON o.order_id = d.order_id
LEFT JOIN dds.v_courier_orders v ON v.order_id = d.order_id;

SELECT settlement_year, settlement_month,
       COUNT(*) AS couriers, SUM(orders_count) AS orders,
       SUM(orders_total_sum) AS orders_total,
       SUM(courier_reward_sum) AS rewards,
       COUNT(*) FILTER (WHERE order_processing_fee IS DISTINCT FROM ROUND(orders_total_sum * 0.25, 2)) AS fee_errors,
       COUNT(*) FILTER (WHERE courier_reward_sum IS DISTINCT FROM ROUND(courier_order_sum + courier_tips_sum * 0.95, 2)) AS reward_errors
FROM cdm.dm_courier_ledger
GROUP BY settlement_year, settlement_month
ORDER BY settlement_year, settlement_month;

SELECT courier_id, settlement_year, settlement_month, COUNT(*) AS duplicates
FROM cdm.dm_courier_ledger
GROUP BY courier_id, settlement_year, settlement_month
HAVING COUNT(*) > 1;
