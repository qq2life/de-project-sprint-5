# Sprint 5 — DWH для нескольких источников: расчёты с курьерами

Учебный проект: загрузка заказов из MongoDB и информации о курьерах и доставках из REST API, построение хранилища PostgreSQL по слоям **STG → DDS → CDM** и помесячной витрины выплат курьерам.

## Архитектура

- **MongoDB** (`db-mongo.orders`) → `stg.mongo_orders` (полный BSON/JSON) → `dds.dm_orders` (стоимость `cost`, дата создания из статуса **OPEN**) через DAG `mongo_orders_pipeline`.
- **REST API** (`GET /couriers`, `GET /deliveries`) → `stg.api_couriers`, `stg.api_deliveries` → `dds.dm_couriers`, `dds.fct_deliveries` через DAG `courier_ledger_pipeline`.
- `dds.v_courier_orders` предоставляет ID, дату **OPEN** и сумму заказа из MongoDB. `dags/sql/06_link_orders_to_couriers.sql` связывает факты доставки с заказами.
- **CDM**: `cdm.dm_courier_ledger`, одна запись на курьера за календарный месяц, определяемый по дате открытия заказа. CDM считает стоимость заказов, комиссию, средний рейтинг, вознаграждение и чаевые.
- `ExternalTaskSensor` в DAG курьеров ожидает успешной задачи `mongo_orders_pipeline.merge_mongo_orders_dds` за соответствующий запуск (сдвиг логических дат 45 минут).

## Состав репозитория

```
.
├── README.md
├── api_entities.md
├── dags/
│   ├── mongo_orders_pipeline.py
│   ├── courier_pipeline.py
│   └── sql/
│       ├── 02_load_dds.sql
│       ├── 03_load_cdm.sql
│       ├── 04_create_mongo_orders.sql
│       ├── 05_merge_mongo_orders.sql
│       ├── 06_link_orders_to_couriers.sql
│       └── 07_fix_order_creation_date.sql
├── sql/
│   ├── 01_create_schema.sql
│   ├── 03_validation.sql
│   └── 04_cross_source_validation.sql
└── tests/test_payout_rules.py
```

## Развёртывание

Используется официальная инфраструктура курса `practicum-de/s5-lessons`, контейнер `de-pg-cr-af`, PostgreSQL `de` и Airflow 2.3. В рабочем окружении DAG находятся в `s5-lessons/dags/`.

1. Запустить официальную инфраструктуру с помощью `docker compose up -d` в каталоге `s5-lessons`. Скопировать **два `.py`** из `dags/` и содержимое `dags/sql/` в одноимённые каталоги официальной инфраструктуры.
2. Создать Airflow Connection `PG_WAREHOUSE_CONNECTION` с доступом к PostgreSQL `de`. Назначить Airflow Variable `courier_postgres_conn_id=PG_WAREHOUSE_CONNECTION`.
3. Настроить Airflow Variables `courier_api_nickname`, `courier_api_cohort`, `courier_api_key` (секрет не хранится в Git). Для MongoDB: `MONGO_DB_HOST`, `MONGO_DB_USER`, `MONGO_DB_PASSWORD`, `MONGO_DB_REPLICA_SET`, `MONGO_DB_DATABASE_NAME`, `MONGO_DB_CERTIFICATE_PATH`. Установить доверенный CA сертификат отдельно от репозитория в каталог, доступный контейнеру, и задать его **контейнерный** путь в `MONGO_DB_CERTIFICATE_PATH`.
4. Создать таблицы **в указанном порядке** (из каталога `s5-lessons`):

```bash
docker compose exec -T -e PGPASSWORD=jovyan de-pg-cr-af \
  psql -h 127.0.0.1 -U jovyan -d de -v ON_ERROR_STOP=1 \
  < /path/to/submission/sql/01_create_schema.sql

docker compose exec -T -e PGPASSWORD=jovyan de-pg-cr-af \
  psql -h 127.0.0.1 -U jovyan -d de -v ON_ERROR_STOP=1 \
  < dags/sql/04_create_mongo_orders.sql
```

Используйте свои реальные параметры подключения вместо учебного примера `jovyan`. Актуальный файл `04_create_mongo_orders.sql` **уже** создаёт представление, где `order_ts = order_created_ts` из MongoDB; `07_fix_order_creation_date.sql` нужен лишь для обновления прежних развёртываний.

5. Включить DAG `mongo_orders_pipeline` (02:15 UTC) и `courier_ledger_pipeline` (03:00 UTC). Оба выполняются ежедневно, `catchup=False`. Курьерский DAG использует семидневный период пересечения по дате доставки для инкрементальной выгрузки API. Исторические периоды загружаются отдельными запусками/backfill.
6. Проверить импорт и выполнение задач через `airflow dags list-import-errors`, `airflow dags list-runs -d mongo_orders_pipeline`, `airflow dags list-runs -d courier_ledger_pipeline`. Секреты не выводить в журналы.

**Особенность учебного стенда:** пример `examples/stg/bonus_system_ranks_dag` требует отдельного подключения `PG_ORIGIN_BONUS_SYSTEM_CONNECTION`. Если бонусный источник не используется, его можно исключить из сканирования Airflow через локальный `.airflowignore`, не удаляя учебный DAG.

## Правила расчёта

Месяц определяется по событию **OPEN**, а не по MongoDB `date` или API `order_ts`: в проверенных данных последние два поля совпадают со временем закрытия заказа. Стоимость заказа — исключительно `MongoDB.orders.cost`, не поле `deliveries.sum`.

- Средний рейтинг курьера рассчитывается **по всем заказам курьера в данном месяце**, перед выбором тарифа не округляется.
- Если рейтинг ниже 4: `max(5% стоимости заказа, 100 ₽)`; от 4 до 4.5: `max(7%, 150 ₽)`; от 4.5 до 4.9: `max(8%, 175 ₽)`; от 4.9: `max(10%, 200 ₽)`.
- Минимум применяется к **каждому заказу** отдельно.
- `order_processing_fee = orders_total_sum × 25%`.
- `courier_reward_sum = courier_order_sum + courier_tips_sum × 95%`.
- Повторные загрузки реализованы через UPSERT (`ON CONFLICT`).

## Проверки

```bash
python3 -m py_compile dags/courier_pipeline.py dags/mongo_orders_pipeline.py
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

SQL-проверки в `sql/03_validation.sql` и `sql/04_cross_source_validation.sql` выполняются в подключённой БД. Для второго файла используйте команду `psql -f`, чтобы увидеть все результаты.

### Результаты интеграционной проверки 29.10.2026

- 4 166 заказов MongoDB в STG и DDS, все с датой создания и стоимостью.
- 100 курьеров, 3 013 доставок в STG и DDS; все 3 013 связаны с заказами.
- Обнаружено 19 переходящих между месяцами заказов, после исправления все 3 013 дат в `dds.v_courier_orders` совпадают с MongoDB OPEN.
- CDM за сентябрь: 19 заказов, 16 курьеров, выплаты **23 253,95 ₽**.
- CDM за октябрь: 2 994 заказа, 100 курьеров, выплаты **2 983 829,39 ₽**.
- Тесты тарифов (3 теста), сравнение количества заказов между DDS/CDM и проверка финансовых формул прошли успешно на проверенных данных.
- Оба DAG завершили запланированные запуски в Airflow со статусом `success`; ошибок импорта нет.

**Ограничения:** октябрь ещё не завершён; за 15–21 октября API вернул 0 записей при проверке, поэтому приведённые цифры — расчёт по **доступным и загруженным данным**, а не гарантия полноты всей истории. Поздние изменения старых месяцев за пределами окна текущего/предыдущего месяца потребуют отдельного перерасчёта. Расписания и настройки показаны для учебного стенда, а не продуктивной системы.
