-- Расширение официального DWH пятого спринта: MongoDB orders -> STG / DDS.
-- Запустить один раз после sql/01_create_schema.sql для курьеров.
-- Не удаляет существующие таблицы и данные.
BEGIN;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS stg.mongo_orders (
    order_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    order_open_ts TIMESTAMP WITHOUT TIME ZONE,
    source_date TIMESTAMP WITHOUT TIME ZONE,
    source_updated_ts TIMESTAMP WITHOUT TIME ZONE,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_mongo_orders_updated_ts ON stg.mongo_orders (source_updated_ts);
CREATE INDEX IF NOT EXISTS ix_mongo_orders_order_open_ts ON stg.mongo_orders (order_open_ts);

CREATE TABLE IF NOT EXISTS dds.dm_orders (
    order_id TEXT PRIMARY KEY,
    order_created_ts TIMESTAMP WITHOUT TIME ZONE,
    source_date TIMESTAMP WITHOUT TIME ZONE,
    order_total_sum NUMERIC(18,2) NOT NULL CHECK (order_total_sum >= 0),
    final_status TEXT,
    source_updated_ts TIMESTAMP WITHOUT TIME ZONE,
    courier_id BIGINT REFERENCES dds.dm_couriers(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_dm_orders_created_ts ON dds.dm_orders (order_created_ts);
CREATE INDEX IF NOT EXISTS ix_dm_orders_courier_id ON dds.dm_orders (courier_id);

-- Отчётный месяц определяем по событию OPEN из MongoDB.
CREATE OR REPLACE VIEW dds.v_courier_orders AS
SELECT
    o.order_id,
    o.order_created_ts AS order_ts,
    o.order_total_sum
FROM dds.dm_orders AS o;
COMMIT;
