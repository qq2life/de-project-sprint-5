DELETE FROM mart.f_customer_retention
WHERE period_name = 'weekly'
  AND period_id = (
      SELECT week_of_year
      FROM mart.d_calendar
      WHERE date_actual = '{{ ds }}'::date
  );


INSERT INTO mart.f_customer_retention (
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    period_name,
    period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)

WITH customer_week_stat AS (
    SELECT
        dc.week_of_year AS period_id,
        fs.item_id,
        fs.customer_id,

        COUNT(*) FILTER (
            WHERE fs.status = 'shipped'
        ) AS shipped_orders,

        COALESCE(
            SUM(fs.payment_amount) FILTER (
                WHERE fs.status = 'shipped'
            ),
            0
        ) AS shipped_revenue,

        COUNT(*) FILTER (
            WHERE fs.status = 'refunded'
        ) AS refund_count

    FROM mart.f_sales fs

    JOIN mart.d_calendar dc
        ON dc.date_id = fs.date_id

    WHERE dc.week_of_year = (
        SELECT week_of_year
        FROM mart.d_calendar
        WHERE date_actual = '{{ ds }}'::date
    )

    GROUP BY
        dc.week_of_year,
        fs.item_id,
        fs.customer_id
)

SELECT
    COUNT(*) FILTER (
        WHERE shipped_orders = 1
    ) AS new_customers_count,

    COUNT(*) FILTER (
        WHERE shipped_orders > 1
    ) AS returning_customers_count,

    COUNT(*) FILTER (
        WHERE refund_count > 0
    ) AS refunded_customer_count,

    'weekly' AS period_name,

    period_id,

    item_id,

    COALESCE(
        SUM(shipped_revenue) FILTER (
            WHERE shipped_orders = 1
        ),
        0
    ) AS new_customers_revenue,

    COALESCE(
        SUM(shipped_revenue) FILTER (
            WHERE shipped_orders > 1
        ),
        0
    ) AS returning_customers_revenue,

    COALESCE(
        SUM(refund_count),
        0
    ) AS customers_refunded

FROM customer_week_stat

GROUP BY
    period_id,
    item_id;