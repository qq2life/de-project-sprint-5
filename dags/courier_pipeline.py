"""Airflow 2.7+/3: API курьеров -> STG -> DDS -> CDM.

Это самодостаточная демонстрационная оркестрация. В исходном проекте
DDS факты и связь с заказами должны выполняться с учётом уже существующего
DAG заказов (см. README). Не отключайте исходные DAG.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
PAGE_SIZE = 50  # максимальный limit по спецификации API
SQL_DIR = Path(__file__).parent / "sql"


def _pg_hook() -> PostgresHook:
    conn_id = Variable.get("courier_postgres_conn_id", default_var="postgres_dwh")
    return PostgresHook(postgres_conn_id=conn_id)


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def _headers() -> dict[str, str]:
    # Значения задаются через Airflow Variables / AIRFLOW_VAR_*.
    # Никогда не печатать их в логи и не коммитить ключ API в репозиторий.
    return {
        "X-Nickname": Variable.get("courier_api_nickname"),
        "X-Cohort": str(Variable.get("courier_api_cohort")),
        "X-API-KEY": Variable.get("courier_api_key"),
    }


def _pages(endpoint: str, sort_field: str, extra_params: dict | None = None):
    """Итератор по страницам; предсказуемая сортировка и последовательный offset."""
    params = dict(extra_params or {})
    params.update(sort_field=sort_field, sort_direction="asc", limit=PAGE_SIZE)
    offset = 0
    with _session() as session:
        headers = _headers()
        while True:
            params["offset"] = offset
            response = session.get(
                f"{Variable.get('courier_api_base_url', default_var=BASE_URL).rstrip('/')}/{endpoint}",
                params=params,
                headers=headers,
                timeout=(10, 60),
            )
            response.raise_for_status()
            items = response.json()
            if not isinstance(items, list):
                raise AirflowException(f"API /{endpoint}: ожидался массив JSON")
            if not items:
                break
            yield items
            if len(items) < PAGE_SIZE:
                break
            offset += PAGE_SIZE


def _parse_ts(value: str) -> datetime:
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is not None:
        raise AirflowException(
            "У API обнаружена временная зона в timestamp; проверьте единый timezone"
        )
    return result


def _delivery_window(context: dict) -> tuple[str, str]:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    if conf.get("delivery_from") or conf.get("delivery_to"):
        if not (conf.get("delivery_from") and conf.get("delivery_to")):
            raise AirflowException("Задайте оба поля delivery_from и delivery_to")
        from_day = date.fromisoformat(conf["delivery_from"])
        to_day = date.fromisoformat(conf["delivery_to"])
    else:
        to_day = max(context["data_interval_end"].date(), context["execution_date"].date())
        from_day = to_day - timedelta(days=7)
    if from_day >= to_day:
        raise AirflowException("delivery_from должен быть раньше delivery_to")
    return f"{from_day:%Y-%m-%d} 00:00:00", f"{to_day:%Y-%m-%d} 00:00:00"


def _report_window(context: dict) -> tuple[date, date]:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    report_month = conf.get("report_month")
    if report_month:
        try:
            month = date.fromisoformat(report_month + "-01")
        except ValueError as exc:
            raise AirflowException("report_month должен иметь формат YYYY-MM") from exc
    else:
        current = context["data_interval_end"].date().replace(day=1)
        month = (current - timedelta(days=1)).replace(day=1)
    if month.month == 12:
        next_month = date(month.year + 1, 1, 1)
    else:
        next_month = date(month.year, month.month + 1, 1)
    return month, next_month


@dag(
    dag_id="courier_ledger_pipeline",
    description="Курьеры и доставки из API + помесячная витрина выплат",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule_interval="0 3 * * *",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["dwh", "couriers", "stg", "dds", "cdm"],
)
def courier_ledger_pipeline():
    @task
    def load_couriers_stg() -> int:
        hook = _pg_hook()
        connection = hook.get_conn()
        total = 0
        sql = """
            INSERT INTO stg.api_couriers(courier_id, payload, loaded_at)
            VALUES %s
            ON CONFLICT (courier_id)
            DO UPDATE SET payload = EXCLUDED.payload, loaded_at = NOW()
        """
        try:
            with connection.cursor() as cursor:
                for page in _pages("couriers", "id"):
                    data = [(str(row["_id"]), Json(row)) for row in page]
                    execute_values(cursor, sql, data, template="(%s, %s, NOW())")
                    connection.commit()
                    total += len(data)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return total

    @task
    def load_deliveries_stg(**context) -> int:
        start_ts, end_ts = _delivery_window(context)
        print(f"DELIVERIES_WINDOW: {start_ts} -> {end_ts}")
        hook = _pg_hook()
        connection = hook.get_conn()
        total = 0
        sql = """
            INSERT INTO stg.api_deliveries
                (delivery_id, order_id, courier_id, order_ts, delivery_ts, payload, loaded_at)
            VALUES %s
            ON CONFLICT (delivery_id)
            DO UPDATE SET
                order_id = EXCLUDED.order_id,
                courier_id = EXCLUDED.courier_id,
                order_ts = EXCLUDED.order_ts,
                delivery_ts = EXCLUDED.delivery_ts,
                payload = EXCLUDED.payload,
                loaded_at = NOW()
        """
        try:
            with connection.cursor() as cursor:
                for page in _pages("deliveries", "_id", {"from": start_ts, "to": end_ts}):
                    data = [
                        (
                            str(row["delivery_id"]),
                            str(row["order_id"]),
                            str(row["courier_id"]),
                            _parse_ts(row["order_ts"]),
                            _parse_ts(row["delivery_ts"]),
                            Json(row),
                        )
                        for row in page
                    ]
                    execute_values(cursor, sql, data, template="(%s, %s, %s, %s, %s, %s, NOW())")
                    connection.commit()
                    total += len(data)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return total

    @task
    def update_dds() -> None:
        _pg_hook().run((SQL_DIR / "02_load_dds.sql").read_text(encoding="utf-8"))

    @task
    def update_cdm(**context) -> None:
        hook = _pg_hook()
        hook.run((SQL_DIR / "06_link_orders_to_couriers.sql").read_text(encoding="utf-8"))

        previous_start, current_start = _report_window(context)
        if current_start.month == 12:
            next_start = date(current_start.year + 1, 1, 1)
        else:
            next_start = date(current_start.year, current_start.month + 1, 1)

        periods = [(previous_start, current_start)]
        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        if not conf.get("report_month"):
            periods.append((current_start, next_start))

        sql = (SQL_DIR / "03_load_cdm.sql").read_text(encoding="utf-8")
        for start, end in periods:
            hook.run(sql, parameters={
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
            })
            print(f"CDM_REFRESHED: {start} -> {end}")

    # DAG Mongo: 02:15 UTC; DAG couriers: 03:00 UTC.
    # The same day's scheduled logical dates differ by 45 minutes.
    wait_for_orders = ExternalTaskSensor(
        task_id="wait_for_mongo_orders_dds",
        external_dag_id="mongo_orders_pipeline",
        external_task_id="merge_mongo_orders_dds",
        execution_delta=timedelta(minutes=45),
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60,
        timeout=7200,
        check_existence=True,
    )

    c = load_couriers_stg()
    d = load_deliveries_stg()
    details = update_dds()
    c >> details
    d >> details
    details >> wait_for_orders >> update_cdm()


dag = courier_ledger_pipeline()
