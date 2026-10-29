"""Airflow 2.3: load MongoDB orders -> STG (raw BSON as Extended JSON) -> DDS.

Read MongoDB settings from Airflow Variables. Do not print passwords/URIs.
First run does full bootstrap; later runs re-read a configurable lookback from
last source update timestamp so changes to order states and costs are captured.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bson import json_util
from pymongo import MongoClient
from psycopg2.extras import Json, execute_values

SQL_DIR = Path(__file__).parent / "sql"
PAGE_SIZE = 200


def _parse_time(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return _parse_time(parsed)
        except ValueError:
            return None
    return None


def _first_open(doc):
    candidates = [
        _parse_time(s.get("dttm"))
        for s in (doc.get("statuses") or [])
        if isinstance(s, dict) and str(s.get("status", "")).upper() == "OPEN"
    ]
    candidates = [value for value in candidates if value is not None]
    return min(candidates) if candidates else None


def _pg():
    return PostgresHook(postgres_conn_id=Variable.get(
        "courier_postgres_conn_id", default_var="PG_WAREHOUSE_CONNECTION"
    ))


def _mongo():
    g = Variable.get
    return MongoClient(
        host=g("MONGO_DB_HOST"),
        username=g("MONGO_DB_USER"),
        password=g("MONGO_DB_PASSWORD"),
        authSource=g("MONGO_DB_DATABASE_NAME"),
        replicaSet=g("MONGO_DB_REPLICA_SET"),
        tls=True,
        tlsCAFile=g("MONGO_DB_CERTIFICATE_PATH"),
        serverSelectionTimeoutMS=20000,
        connectTimeoutMS=15000,
        socketTimeoutMS=60000,
    )


@dag(
    dag_id="mongo_orders_pipeline",
    description="MongoDB orders: source JSON, OPEN timestamp and cost for courier ledger",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule_interval="15 2 * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["sprint5", "mongo", "stg", "dds"],
)
def mongo_orders_pipeline():
    @task
    def load_mongo_orders_stg():
        pg_conn = _pg().get_conn()
        client = None
        batch = []
        processed = 0
        sql = """
            INSERT INTO stg.mongo_orders
                (order_id, payload, order_open_ts, source_date, source_updated_ts, loaded_at)
            VALUES %s
            ON CONFLICT (order_id)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                order_open_ts = EXCLUDED.order_open_ts,
                source_date = EXCLUDED.source_date,
                source_updated_ts = EXCLUDED.source_updated_ts,
                loaded_at = NOW()
        """
        try:
            with pg_conn.cursor() as cursor:
                cursor.execute("SELECT MAX(source_updated_ts) FROM stg.mongo_orders")
                latest = cursor.fetchone()[0]
                # First run: full load. Daily runs: retain 7-day update overlap.
                lookback = int(Variable.get("mongo_orders_lookback_days", default_var="7"))
                if lookback < 0:
                    raise ValueError("mongo_orders_lookback_days must be nonnegative")
                query = {"update_ts": {"$gte": latest - timedelta(days=lookback)}} if latest else {}

                client = _mongo()
                db = client[Variable.get("MONGO_DB_DATABASE_NAME")]
                docs = db["orders"].find(query).sort("_id", 1).batch_size(PAGE_SIZE)

                for doc in docs:
                    batch.append((
                        str(doc["_id"]),
                        Json(json.loads(json_util.dumps(doc))),
                        _first_open(doc),
                        _parse_time(doc.get("date")),
                        _parse_time(doc.get("update_ts")),
                    ))
                    if len(batch) >= PAGE_SIZE:
                        execute_values(cursor, sql, batch,
                                       template="(%s, %s, %s, %s, %s, NOW())")
                        pg_conn.commit()
                        processed += len(batch)
                        batch.clear()
                if batch:
                    execute_values(cursor, sql, batch,
                                   template="(%s, %s, %s, %s, %s, NOW())")
                    pg_conn.commit()
                    processed += len(batch)
            return processed
        except Exception:
            pg_conn.rollback()
            raise
        finally:
            pg_conn.close()
            if client is not None:
                client.close()

    @task
    def merge_mongo_orders_dds():
        _pg().run((SQL_DIR / "05_merge_mongo_orders.sql").read_text(encoding="utf-8"))

    load_mongo_orders_stg() >> merge_mongo_orders_dds()


dag = mongo_orders_pipeline()
