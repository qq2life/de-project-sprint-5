CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    new_customers_count INT NOT NULL DEFAULT 0,
    returning_customers_count INT NOT NULL DEFAULT 0,
    refunded_customer_count INT NOT NULL DEFAULT 0,
    period_name VARCHAR(20) NOT NULL,
    period_id INT NOT NULL,
    item_id INT NOT NULL,
    new_customers_revenue NUMERIC(14,2) NOT NULL DEFAULT 0,
    returning_customers_revenue NUMERIC(14,2) NOT NULL DEFAULT 0,
    customers_refunded INT NOT NULL DEFAULT 0
);