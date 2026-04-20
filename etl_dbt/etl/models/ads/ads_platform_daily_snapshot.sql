{{
    config(
        materialized='table',
        schema='ads'
    )
}}

{% set target_date = var('target_date') %}
{% if target_date is not none %}
    select
        '{{ target_date }}' as stat_date,
        daily_sales,
        daily_orders,
        active_users,
        new_users,
        conversion_rate,
        refund_count,
        refund_rate,
        health_score,
        sales_drop_warning,
        conversion_drop_warning,
        refund_spike_warning,
        now() as etl_time
    from {{ ref('dws_business_daily_health') }} d
    where d.stat_date = '{{ target_date }}'
{% else %}
    {{ log("WARNING: target_date not provided for ads_platform_daily_snapshot, returning empty result", info=True) }}
    select * from (select 1 as dummy) where 1=0
{% endif %}