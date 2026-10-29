-- Выполнять после обеих загрузок STG.
-- Если в /deliveries встретился courier_id, отсутствующий в /couriers,
-- факт не загрузится; проверять контрольным запросом в README.
INSERT INTO dds.dm_couriers (courier_id, courier_name, updated_at)
SELECT
    c.courier_id,
    NULLIF(BTRIM(c.payload ->> 'name'), ''),
    NOW()
FROM stg.api_couriers AS c
WHERE NULLIF(BTRIM(c.payload ->> 'name'), '') IS NOT NULL
ON CONFLICT (courier_id)
DO UPDATE SET
    courier_name = EXCLUDED.courier_name,
    updated_at = NOW();

INSERT INTO dds.fct_deliveries
    (delivery_id, order_id, courier_id, order_ts, delivery_ts, rate, tip_sum, updated_at)
SELECT
    s.delivery_id,
    s.order_id,
    c.id,
    s.order_ts,
    s.delivery_ts,
    (s.payload ->> 'rate')::SMALLINT,
    COALESCE(NULLIF(s.payload ->> 'tip_sum', '')::NUMERIC, 0)::NUMERIC(16,2),
    NOW()
FROM stg.api_deliveries AS s
INNER JOIN dds.dm_couriers AS c
    ON c.courier_id = s.courier_id
WHERE (s.payload ->> 'rate') IS NOT NULL
ON CONFLICT (delivery_id)
DO UPDATE SET
    order_id = EXCLUDED.order_id,
    courier_id = EXCLUDED.courier_id,
    order_ts = EXCLUDED.order_ts,
    delivery_ts = EXCLUDED.delivery_ts,
    rate = EXCLUDED.rate,
    tip_sum = EXCLUDED.tip_sum,
    updated_at = NOW();
