-- Параметры Psycopg2: %(period_start)s и %(period_end)s.
-- Период включительно по началу и исключительно по концу, по дате заказа.
-- Не используем API поле deliveries.sum.
WITH candidate_deliveries AS (
    SELECT DISTINCT ON (d.order_id)
        c.courier_id,
        c.courier_name,
        d.order_id,
        o.order_ts,
        o.order_total_sum::NUMERIC AS order_total_sum,
        d.rate::NUMERIC AS rate,
        d.tip_sum::NUMERIC AS tip_sum
    FROM dds.fct_deliveries AS d
    JOIN dds.dm_couriers AS c ON c.id = d.courier_id
    JOIN dds.v_courier_orders AS o ON o.order_id = d.order_id
    WHERE o.order_ts >= %(period_start)s::TIMESTAMP
      AND o.order_ts < %(period_end)s::TIMESTAMP
    ORDER BY d.order_id, d.delivery_ts DESC, d.delivery_id DESC
),
monthly_ratings AS (
    SELECT courier_id, AVG(rate) AS rate_avg
    FROM candidate_deliveries
    GROUP BY courier_id
),
per_order AS (
    SELECT
        d.*,
        r.rate_avg,
        CASE
            WHEN r.rate_avg < 4 THEN GREATEST(d.order_total_sum * 0.05, 100)
            WHEN r.rate_avg < 4.5 THEN GREATEST(d.order_total_sum * 0.07, 150)
            WHEN r.rate_avg < 4.9 THEN GREATEST(d.order_total_sum * 0.08, 175)
            ELSE GREATEST(d.order_total_sum * 0.10, 200)
        END AS courier_order_reward
    FROM candidate_deliveries AS d
    JOIN monthly_ratings AS r USING (courier_id)
),
aggregated AS (
    SELECT
        courier_id,
        MAX(courier_name) AS courier_name,
        EXTRACT(YEAR FROM %(period_start)s::TIMESTAMP)::INTEGER AS settlement_year,
        EXTRACT(MONTH FROM %(period_start)s::TIMESTAMP)::INTEGER AS settlement_month,
        COUNT(*)::INTEGER AS orders_count,
        ROUND(SUM(order_total_sum), 2) AS orders_total_sum,
        ROUND(MAX(rate_avg), 5) AS rate_avg,
        ROUND(SUM(order_total_sum) * 0.25, 2) AS order_processing_fee,
        ROUND(SUM(courier_order_reward), 2) AS courier_order_sum,
        ROUND(SUM(tip_sum), 2) AS courier_tips_sum,
        ROUND(ROUND(SUM(courier_order_reward), 2) + ROUND(SUM(tip_sum), 2) * 0.95, 2) AS courier_reward_sum
    FROM per_order
    GROUP BY courier_id
)
INSERT INTO cdm.dm_courier_ledger (
    courier_id, courier_name, settlement_year, settlement_month,
    orders_count, orders_total_sum, rate_avg, order_processing_fee,
    courier_order_sum, courier_tips_sum, courier_reward_sum, updated_at
)
SELECT
    courier_id, courier_name, settlement_year, settlement_month,
    orders_count, orders_total_sum, rate_avg, order_processing_fee,
    courier_order_sum, courier_tips_sum, courier_reward_sum, NOW()
FROM aggregated
ON CONFLICT (courier_id, settlement_year, settlement_month)
DO UPDATE SET
    courier_name = EXCLUDED.courier_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    rate_avg = EXCLUDED.rate_avg,
    order_processing_fee = EXCLUDED.order_processing_fee,
    courier_order_sum = EXCLUDED.courier_order_sum,
    courier_tips_sum = EXCLUDED.courier_tips_sum,
    courier_reward_sum = EXCLUDED.courier_reward_sum,
    updated_at = NOW();


-- Удаляем записи этого же отчётного месяца, если после исправлений в DDS
-- у курьера больше не осталось доставленных заказов за указанный период.
DELETE FROM cdm.dm_courier_ledger AS old
WHERE old.settlement_year = EXTRACT(YEAR FROM %(period_start)s::TIMESTAMP)::INTEGER
  AND old.settlement_month = EXTRACT(MONTH FROM %(period_start)s::TIMESTAMP)::INTEGER
  AND NOT EXISTS (
      SELECT 1
      FROM dds.fct_deliveries AS d
      JOIN dds.dm_couriers AS c ON c.id = d.courier_id
      JOIN dds.v_courier_orders AS o ON o.order_id = d.order_id
      WHERE c.courier_id = old.courier_id
        AND o.order_ts >= %(period_start)s::TIMESTAMP
        AND o.order_ts < %(period_end)s::TIMESTAMP
  );
