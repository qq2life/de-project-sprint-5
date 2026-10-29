-- Idempotent merge: order cost only from MongoDB, never deliveries.sum.
-- Invalid or missing cost stays in STG for diagnostics instead of failing the whole merge.
WITH parsed AS (
    SELECT
        s.order_id,
        s.order_open_ts,
        s.source_date,
        CASE
            WHEN COALESCE(s.payload ->> 'cost', '') ~ '^[0-9]+(\.[0-9]+)?$'
            THEN (s.payload ->> 'cost')::NUMERIC
            ELSE NULL
        END AS order_total_sum,
        NULLIF(s.payload ->> 'final_status', '') AS final_status,
        s.source_updated_ts
    FROM stg.mongo_orders AS s
)
INSERT INTO dds.dm_orders (
    order_id, order_created_ts, source_date, order_total_sum,
    final_status, source_updated_ts, updated_at
)
SELECT
    p.order_id,
    p.order_open_ts,
    p.source_date,
    p.order_total_sum::NUMERIC(18,2),
    p.final_status,
    p.source_updated_ts,
    NOW()
FROM parsed AS p
WHERE p.order_total_sum IS NOT NULL AND p.order_total_sum >= 0
ON CONFLICT (order_id)
DO UPDATE SET
    order_created_ts = EXCLUDED.order_created_ts,
    source_date = EXCLUDED.source_date,
    order_total_sum = EXCLUDED.order_total_sum,
    final_status = EXCLUDED.final_status,
    source_updated_ts = EXCLUDED.source_updated_ts,
    updated_at = NOW();
