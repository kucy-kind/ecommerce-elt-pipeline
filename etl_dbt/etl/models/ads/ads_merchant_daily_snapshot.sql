{{
    config(
        materialized='table',
        schema='ads'
    )
}}


{% set target_date = var('target_date') %}
SELECT
    d.stat_date,
    d.merchant_id,
    p.merchant_name,
    p.merchant_level,
    d.daily_sales,
    d.order_count,
    d.customer_count,
    d.refund_count,
    d.sales_mom,
    d.sales_yoy,
    d.order_count_mom,
    d.refund_rate,
    d.zero_sales_flag,
    d.abnormal_sales_flag,
    NOW() AS etl_time
FROM {{ ref('dws_merchant_daily')}} d
left join {{ ref ('dws_merchant_profile')}} p on d.stat_date=p.stat_date and p.merchant_id=d.merchant_id
where d.stat_date='{{target_date}}'