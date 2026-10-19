insert into mart.f_sales (
    date_id,
    item_id,
    customer_id,
    city_id,
    quantity,
    payment_amount,
    status
)
select
    dc.date_id,
    item_id,
    customer_id,
    city_id,

    case
        when status = 'refunded' then -abs(quantity)
        else abs(quantity)
    end as quantity,

    case
        when status = 'refunded' then -abs(payment_amount)
        else abs(payment_amount)
    end as payment_amount,

    status

from staging.user_order_log uol
left join mart.d_calendar as dc
    on uol.date_time::date = dc.date_actual

where uol.date_time::date = '{{ ds }}';