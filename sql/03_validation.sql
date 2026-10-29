-- Проверки после запуска ETL; для работоспособности CDM нужен адаптер dds.v_courier_orders.
SELECT 'stg_couriers' AS src, COUNT(*) AS total FROM stg.api_couriers
UNION ALL SELECT 'stg_deliveries', COUNT(*) FROM stg.api_deliveries
UNION ALL SELECT 'dds_couriers', COUNT(*) FROM dds.dm_couriers
UNION ALL SELECT 'dds_deliveries', COUNT(*) FROM dds.fct_deliveries
UNION ALL SELECT 'cdm_ledger', COUNT(*) FROM cdm.dm_courier_ledger;

-- Все ли API курьеры попали в DDS?
SELECT s.courier_id
FROM stg.api_couriers s
LEFT JOIN dds.dm_couriers c ON c.courier_id = s.courier_id
WHERE c.id IS NULL;

-- Доставки в STG без факта DDS.
SELECT s.delivery_id, s.courier_id, s.order_id
FROM stg.api_deliveries s
LEFT JOIN dds.fct_deliveries d ON d.delivery_id = s.delivery_id
WHERE d.id IS NULL;

-- Доставки без соответствующего заказа в старом DWH: в расчёт не попадут.
SELECT d.order_id, d.delivery_id
FROM dds.fct_deliveries d
LEFT JOIN dds.v_courier_orders o ON o.order_id = d.order_id
WHERE o.order_id IS NULL;

-- В адаптере должны быть уникальные заказы.
SELECT order_id, COUNT(*) AS n
FROM dds.v_courier_orders
GROUP BY order_id HAVING COUNT(*) > 1;

-- Дубли в витрине (ожидается 0 строк).
SELECT courier_id, settlement_year, settlement_month, COUNT(*) AS n
FROM cdm.dm_courier_ledger
GROUP BY courier_id, settlement_year, settlement_month
HAVING COUNT(*) > 1;

-- Финансовая формула (ожидается 0 строк).
SELECT * FROM cdm.dm_courier_ledger
WHERE ABS(courier_reward_sum - ROUND(courier_order_sum + courier_tips_sum * 0.95, 2)) > 0.01
   OR ABS(order_processing_fee - ROUND(orders_total_sum * 0.25, 2)) > 0.01;
