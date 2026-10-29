# Сущности и правила получения данных для витрины выплат курьерам

## 1. MongoDB: `db-mongo.orders`

| Поле | Использование |
|---|---|
| `_id` | внешний `order_id` для связи с API доставок |
| `statuses[].status`, `statuses[].dttm` | момент `OPEN` — дата создания заказа, отчётный период |
| `cost` | полная стоимость заказа в рублях |
| `date` | время закрытия в проверенных данных; **не использовать для месяца** |
| `final_status` | финальный статус заказа |
| `update_ts` | метка изменения для инкрементального извлечения |

STG: `stg.mongo_orders` хранит полный JSON и извлечённые даты. DDS: `dds.dm_orders`; представление `dds.v_courier_orders(order_id,order_ts,order_total_sum)` использует `order_created_ts`, а не `delivery.order_ts`.

## 2. REST API

Базовый API: `https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net`. Аутентификация заголовками `X-Nickname`, `X-Cohort`, `X-API-KEY` из Airflow Variables (ключ никогда не хранить в репозитории).

| Endpoint | Поля | STG → DDS |
|---|---|---|
| `/couriers` | `_id`, `name` | `stg.api_couriers` → `dds.dm_couriers` |
| `/deliveries` | `delivery_id`, `order_id`, `courier_id`, `order_ts`, `delivery_ts`, `rate`, `tip_sum`, `sum`, `address` | `stg.api_deliveries` → `dds.fct_deliveries` |

Запрашиваем страницы `limit=50`, `offset=0,50,100,...`, с фиксированной сортировкой. Для `/deliveries` добавлены `from` и `to` по времени доставки; верхняя граница исключается. Сохраняем исходный `payload` полностью. `deliveries.sum` **не** используется для стоимости заказа.

## 3. `cdm.dm_courier_ledger`

Гранулярность: курьер (`courier_id`) + отчётный год (`settlement_year`) + месяц (`settlement_month`).

| Поле | Значение |
|---|---|
| `id` | суррогатный PK |
| `courier_id`, `courier_name` | идентификатор и имя из `/couriers` |
| `settlement_year`, `settlement_month` | год и месяц по событию `OPEN` MongoDB |
| `orders_count` | число уникальных доставленных заказов курьера |
| `orders_total_sum` | сумма `MongoDB.orders.cost` |
| `rate_avg` | неокруглённое среднее для тарифа; в витрине хранится округлённое значение |
| `order_processing_fee` | 25% стоимости заказов |
| `courier_order_sum` | сумма тарифа за **каждый** заказ |
| `courier_tips_sum` | сумма `tip_sum` |
| `courier_reward_sum` | `courier_order_sum + courier_tips_sum * 0.95` |

Тарифы по среднемесячному рейтингу: `<4`: 5%, минимум 100 ₽; `[4,4.5)`: 7%, минимум 150 ₽; `[4.5,4.9)`: 8%, минимум 175 ₽; `>=4.9`: 10%, минимум 200 ₽.

Связь `dds.fct_deliveries.order_id = dds.dm_orders.order_id`; курьер хранится в `dds.dm_couriers` и дополнительно проставляется в `dds.dm_orders.courier_id`. Для одного заказа в расчёте выбирается последняя доставка по `delivery_ts`, чтобы не дублировать сумму.

## 4. Идемпотентность и качество

UPsert в STG/DDS и CDM, уникальность `(courier_id, settlement_year, settlement_month)`, проверки отсутствующих заказов и совпадения дат. Отдельно проверены 19 переходящих между месяцами доставок. Исторические диапазоны подгружаются вручную; ежедневный семидневный lookback не заменяет исторический backfill.
