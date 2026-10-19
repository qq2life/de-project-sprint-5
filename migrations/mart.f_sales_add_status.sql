ALTER TABLE staging.user_order_log
ADD COLUMN IF NOT EXISTS status VARCHAR(10) NOT NULL DEFAULT 'shipped';

ALTER TABLE staging.user_order_log
DROP CONSTRAINT IF EXISTS user_order_log_pk;

ALTER TABLE staging.user_order_log
ADD CONSTRAINT user_order_log_pk
PRIMARY KEY (uniq_id, status);

ALTER TABLE mart.f_sales
ADD COLUMN IF NOT EXISTS status VARCHAR(10) NOT NULL DEFAULT 'shipped';