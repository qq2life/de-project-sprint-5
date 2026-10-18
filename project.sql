/*
Проект: обновление DWH и инкрементальная витрина по заказчикам
СУБД: PostgreSQL

Скрипт:
1. Собирает данные из source1, source2, source3 и external_source.
2. Обновляет измерения dwh.d_craftsman, dwh.d_product, dwh.d_customer.
3. Обновляет таблицу фактов dwh.f_order.
4. Создаёт витрину dwh.customer_report_datamart и таблицу дат загрузки.
5. Выполняет инкрементальный расчёт витрины.

Важно:
- DDL витрины (DROP/CREATE) выполняется один раз при развёртывании.
- Блоки обновления DWH и инкрементального расчёта витрины можно запускать повторно.
*/

/* =========================================================
   1. СБОР ДАННЫХ ИЗ ВСЕХ ИСТОЧНИКОВ
   ========================================================= */

DROP TABLE IF EXISTS tmp_sources;

CREATE TEMP TABLE tmp_sources AS

SELECT
    order_id,
    order_created_date,
    order_completion_date,
    order_status,
    craftsman_id,
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email,
    product_id,
    product_name,
    product_description,
    product_type,
    product_price,
    customer_id,
    customer_name,
    customer_address,
    customer_birthday,
    customer_email
FROM source1.craft_market_wide

UNION

SELECT
    t2.order_id,
    t2.order_created_date,
    t2.order_completion_date,
    t2.order_status,
    t1.craftsman_id,
    t1.craftsman_name,
    t1.craftsman_address,
    t1.craftsman_birthday,
    t1.craftsman_email,
    t1.product_id,
    t1.product_name,
    t1.product_description,
    t1.product_type,
    t1.product_price,
    t2.customer_id,
    t2.customer_name,
    t2.customer_address,
    t2.customer_birthday,
    t2.customer_email
FROM source2.craft_market_masters_products t1
JOIN source2.craft_market_orders_customers t2
    ON t2.product_id = t1.product_id
   AND t2.craftsman_id = t1.craftsman_id

UNION

SELECT
    t1.order_id,
    t1.order_created_date,
    t1.order_completion_date,
    t1.order_status,
    t2.craftsman_id,
    t2.craftsman_name,
    t2.craftsman_address,
    t2.craftsman_birthday,
    t2.craftsman_email,
    t1.product_id,
    t1.product_name,
    t1.product_description,
    t1.product_type,
    t1.product_price,
    t3.customer_id,
    t3.customer_name,
    t3.customer_address,
    t3.customer_birthday,
    t3.customer_email
FROM source3.craft_market_orders t1
JOIN source3.craft_market_craftsmans t2
    ON t1.craftsman_id = t2.craftsman_id
JOIN source3.craft_market_customers t3
    ON t1.customer_id = t3.customer_id

UNION

SELECT
    t1.order_id,
    t1.order_created_date,
    t1.order_completion_date,
    t1.order_status,
    t1.craftsman_id,
    t1.craftsman_name,
    t1.craftsman_address,
    t1.craftsman_birthday,
    t1.craftsman_email,
    t1.product_id,
    t1.product_name,
    t1.product_description,
    t1.product_type,
    t1.product_price,
    t2.customer_id,
    t2.customer_name,
    t2.customer_address,
    t2.customer_birthday,
    t2.customer_email
FROM external_source.craft_products_orders t1
JOIN external_source.customers t2
    ON t1.customer_id = t2.customer_id;


/* =========================================================
   2. ОБНОВЛЕНИЕ ИЗМЕРЕНИЙ
   ========================================================= */

MERGE INTO dwh.d_craftsman AS d
USING (
    SELECT DISTINCT
        craftsman_name,
        craftsman_address,
        craftsman_birthday,
        craftsman_email
    FROM tmp_sources
) AS t
ON d.craftsman_name = t.craftsman_name
AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
    UPDATE SET
        craftsman_address = t.craftsman_address,
        craftsman_birthday = t.craftsman_birthday,
        load_dttm = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        craftsman_name,
        craftsman_address,
        craftsman_birthday,
        craftsman_email,
        load_dttm
    )
    VALUES (
        t.craftsman_name,
        t.craftsman_address,
        t.craftsman_birthday,
        t.craftsman_email,
        CURRENT_TIMESTAMP
    );


MERGE INTO dwh.d_product AS d
USING (
    SELECT DISTINCT
        product_name,
        product_description,
        product_type,
        product_price
    FROM tmp_sources
) AS t
ON d.product_name = t.product_name
AND d.product_description = t.product_description
AND d.product_price = t.product_price
WHEN MATCHED THEN
    UPDATE SET
        product_type = t.product_type,
        load_dttm = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        product_name,
        product_description,
        product_type,
        product_price,
        load_dttm
    )
    VALUES (
        t.product_name,
        t.product_description,
        t.product_type,
        t.product_price,
        CURRENT_TIMESTAMP
    );


MERGE INTO dwh.d_customer AS d
USING (
    SELECT DISTINCT
        customer_name,
        customer_address,
        customer_birthday,
        customer_email
    FROM tmp_sources
) AS t
ON d.customer_name = t.customer_name
AND d.customer_email = t.customer_email
WHEN MATCHED THEN
    UPDATE SET
        customer_address = t.customer_address,
        customer_birthday = t.customer_birthday,
        load_dttm = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        load_dttm
    )
    VALUES (
        t.customer_name,
        t.customer_address,
        t.customer_birthday,
        t.customer_email,
        CURRENT_TIMESTAMP
    );


/* =========================================================
   3. ОБНОВЛЕНИЕ ТАБЛИЦЫ ФАКТОВ
   ========================================================= */

DROP TABLE IF EXISTS tmp_sources_fact;

CREATE TEMP TABLE tmp_sources_fact AS
SELECT
    dp.product_id,
    dc.craftsman_id,
    dcs.customer_id,
    src.order_created_date,
    src.order_completion_date,
    src.order_status,
    CURRENT_TIMESTAMP AS load_dttm
FROM tmp_sources src
JOIN dwh.d_craftsman dc
    ON dc.craftsman_name = src.craftsman_name
   AND dc.craftsman_email = src.craftsman_email
JOIN dwh.d_customer dcs
    ON dcs.customer_name = src.customer_name
   AND dcs.customer_email = src.customer_email
JOIN dwh.d_product dp
    ON dp.product_name = src.product_name
   AND dp.product_description = src.product_description
   AND dp.product_price = src.product_price;


MERGE INTO dwh.f_order AS f
USING tmp_sources_fact AS t
ON f.product_id = t.product_id
AND f.craftsman_id = t.craftsman_id
AND f.customer_id = t.customer_id
AND f.order_created_date = t.order_created_date
WHEN MATCHED THEN
    UPDATE SET
        order_completion_date = t.order_completion_date,
        order_status = t.order_status,
        load_dttm = t.load_dttm
WHEN NOT MATCHED THEN
    INSERT (
        product_id,
        craftsman_id,
        customer_id,
        order_created_date,
        order_completion_date,
        order_status,
        load_dttm
    )
    VALUES (
        t.product_id,
        t.craftsman_id,
        t.customer_id,
        t.order_created_date,
        t.order_completion_date,
        t.order_status,
        t.load_dttm
    );


/* =========================================================
   4. DDL ВИТРИНЫ ПО ЗАКАЗЧИКАМ
   Выполняется один раз при развёртывании.
   ========================================================= */

DROP TABLE IF EXISTS dwh.customer_report_datamart;

CREATE TABLE dwh.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
    customer_id BIGINT NOT NULL, -- идентификатор заказчика
    customer_name VARCHAR NOT NULL, -- Ф.И.О. заказчика
    customer_address VARCHAR NOT NULL, -- адрес заказчика
    customer_birthday DATE NOT NULL, -- дата рождения заказчика
    customer_email VARCHAR NOT NULL, -- электронная почта заказчика
    customer_money NUMERIC(15,2) NOT NULL, -- сумма, которую потратил заказчик за месяц
    platform_money NUMERIC(15,2) NOT NULL, -- сумма, которую заработала платформа: 10% от покупок заказчика за месяц
    count_order BIGINT NOT NULL, -- количество заказов заказчика за месяц
    avg_price_order NUMERIC(10,2) NOT NULL, -- средняя стоимость одного заказа заказчика за месяц
    median_time_order_completed NUMERIC(10,1), -- медианное время выполнения заказа в днях за месяц
    top_product_category VARCHAR NOT NULL, -- самая популярная категория товаров у заказчика за месяц
    top_craftsman_id BIGINT NOT NULL, -- идентификатор самого популярного мастера у заказчика
    count_order_created BIGINT NOT NULL, -- количество заказов в статусе created за месяц
    count_order_in_progress BIGINT NOT NULL, -- количество заказов в статусе in progress за месяц
    count_order_delivery BIGINT NOT NULL, -- количество заказов в статусе delivery за месяц
    count_order_done BIGINT NOT NULL, -- количество завершённых заказов за месяц
    count_order_not_done BIGINT NOT NULL, -- количество незавершённых заказов за месяц
    report_period VARCHAR NOT NULL, -- отчётный период в формате YYYY-MM
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id) -- первичный ключ витрины
);


/* Таблица с датами загрузок для инкрементального обновления витрины */
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи загрузки
    load_dttm DATE NOT NULL, -- дата последней обработанной загрузки
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id) -- первичный ключ таблицы загрузок
);


/* =========================================================
   5. ИНКРЕМЕНТАЛЬНОЕ ОБНОВЛЕНИЕ ВИТРИНЫ
   Подходит и для первой загрузки.
   ========================================================= */

WITH
last_load AS (
    SELECT COALESCE(MAX(load_dttm), DATE '1900-01-01') AS load_dttm
    FROM dwh.load_dates_customer_report_datamart
),

changed_rows AS (
    SELECT
        fo.customer_id,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
        fo.load_dttm AS order_load_dttm,
        dc.load_dttm AS craftsman_load_dttm,
        dcs.load_dttm AS customer_load_dttm,
        dp.load_dttm AS product_load_dttm
    FROM dwh.f_order fo
    JOIN dwh.d_customer dcs
        ON fo.customer_id = dcs.customer_id
    JOIN dwh.d_craftsman dc
        ON fo.craftsman_id = dc.craftsman_id
    JOIN dwh.d_product dp
        ON fo.product_id = dp.product_id
    CROSS JOIN last_load l
    WHERE fo.load_dttm > l.load_dttm
       OR dc.load_dttm > l.load_dttm
       OR dcs.load_dttm > l.load_dttm
       OR dp.load_dttm > l.load_dttm
),

changed_keys AS (
    SELECT DISTINCT
        customer_id,
        report_period
    FROM changed_rows
),

base AS (
    SELECT
        dcs.customer_id,
        dcs.customer_name,
        dcs.customer_address,
        dcs.customer_birthday,
        dcs.customer_email,
        fo.order_id,
        dc.craftsman_id,
        dp.product_id,
        dp.product_price,
        dp.product_type,
        fo.order_completion_date - fo.order_created_date AS diff_order_date,
        fo.order_status,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period
    FROM dwh.f_order fo
    JOIN dwh.d_customer dcs
        ON fo.customer_id = dcs.customer_id
    JOIN dwh.d_craftsman dc
        ON fo.craftsman_id = dc.craftsman_id
    JOIN dwh.d_product dp
        ON fo.product_id = dp.product_id
    JOIN changed_keys ck
        ON fo.customer_id = ck.customer_id
       AND TO_CHAR(fo.order_created_date, 'yyyy-mm') = ck.report_period
),

main_metrics AS (
    SELECT
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        report_period,
        SUM(product_price)::NUMERIC(15,2) AS customer_money,
        (SUM(product_price) * 0.1)::NUMERIC(15,2) AS platform_money,
        COUNT(order_id) AS count_order,
        AVG(product_price)::NUMERIC(10,2) AS avg_price_order,
        PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY diff_order_date)::NUMERIC(10,1)
            AS median_time_order_completed,
        SUM(CASE WHEN order_status = 'created' THEN 1 ELSE 0 END)
            AS count_order_created,
        SUM(CASE WHEN order_status = 'in progress' THEN 1 ELSE 0 END)
            AS count_order_in_progress,
        SUM(CASE WHEN order_status = 'delivery' THEN 1 ELSE 0 END)
            AS count_order_delivery,
        SUM(CASE WHEN order_status = 'done' THEN 1 ELSE 0 END)
            AS count_order_done,
        SUM(CASE WHEN order_status <> 'done' THEN 1 ELSE 0 END)
            AS count_order_not_done
    FROM base
    GROUP BY
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        report_period
),

top_category AS (
    SELECT
        customer_id,
        report_period,
        product_type,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, report_period
            ORDER BY COUNT(*) DESC, product_type
        ) AS rn
    FROM base
    GROUP BY
        customer_id,
        report_period,
        product_type
),

top_craftsman AS (
    SELECT
        customer_id,
        report_period,
        craftsman_id,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, report_period
            ORDER BY COUNT(*) DESC, craftsman_id
        ) AS rn
    FROM base
    GROUP BY
        customer_id,
        report_period,
        craftsman_id
),

calculated AS (
    SELECT
        m.customer_id,
        m.customer_name,
        m.customer_address,
        m.customer_birthday,
        m.customer_email,
        m.customer_money,
        m.platform_money,
        m.count_order,
        m.avg_price_order,
        m.median_time_order_completed,
        tc.product_type AS top_product_category,
        tcr.craftsman_id AS top_craftsman_id,
        m.count_order_created,
        m.count_order_in_progress,
        m.count_order_delivery,
        m.count_order_done,
        m.count_order_not_done,
        m.report_period
    FROM main_metrics m
    JOIN top_category tc
        ON m.customer_id = tc.customer_id
       AND m.report_period = tc.report_period
       AND tc.rn = 1
    JOIN top_craftsman tcr
        ON m.customer_id = tcr.customer_id
       AND m.report_period = tcr.report_period
       AND tcr.rn = 1
),

update_delta AS (
    UPDATE dwh.customer_report_datamart d
    SET
        customer_name = c.customer_name,
        customer_address = c.customer_address,
        customer_birthday = c.customer_birthday,
        customer_email = c.customer_email,
        customer_money = c.customer_money,
        platform_money = c.platform_money,
        count_order = c.count_order,
        avg_price_order = c.avg_price_order,
        median_time_order_completed = c.median_time_order_completed,
        top_product_category = c.top_product_category,
        top_craftsman_id = c.top_craftsman_id,
        count_order_created = c.count_order_created,
        count_order_in_progress = c.count_order_in_progress,
        count_order_delivery = c.count_order_delivery,
        count_order_done = c.count_order_done,
        count_order_not_done = c.count_order_not_done
    FROM calculated c
    WHERE d.customer_id = c.customer_id
      AND d.report_period = c.report_period
    RETURNING d.id
),

insert_delta AS (
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_money,
        platform_money,
        count_order,
        avg_price_order,
        median_time_order_completed,
        top_product_category,
        top_craftsman_id,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_period
    )
    SELECT
        c.customer_id,
        c.customer_name,
        c.customer_address,
        c.customer_birthday,
        c.customer_email,
        c.customer_money,
        c.platform_money,
        c.count_order,
        c.avg_price_order,
        c.median_time_order_completed,
        c.top_product_category,
        c.top_craftsman_id,
        c.count_order_created,
        c.count_order_in_progress,
        c.count_order_delivery,
        c.count_order_done,
        c.count_order_not_done,
        c.report_period
    FROM calculated c
    WHERE NOT EXISTS (
        SELECT 1
        FROM dwh.customer_report_datamart d
        WHERE d.customer_id = c.customer_id
          AND d.report_period = c.report_period
    )
    RETURNING id
),

insert_load_date AS (
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
    SELECT GREATEST(
        MAX(order_load_dttm),
        MAX(craftsman_load_dttm),
        MAX(customer_load_dttm),
        MAX(product_load_dttm)
    )::DATE
    FROM changed_rows
    HAVING COUNT(*) > 0
    RETURNING id
)

SELECT 'increment customer datamart';


/* Контрольные проверки */
/*
SELECT COUNT(*)
FROM dwh.customer_report_datamart;

SELECT
    customer_id,
    report_period,
    COUNT(*)
FROM dwh.customer_report_datamart
GROUP BY customer_id, report_period
HAVING COUNT(*) > 1;
*/
