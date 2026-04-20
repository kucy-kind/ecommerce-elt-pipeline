{{ log("target_month value: " ~ var('target_month'), info=True) }}

{{
    config(
        materialized='incremental',
        unique_key=['stat_month', 'user_id'],
        schema='ads'
    )
}}


{% set target_month = var('target_month') %}

with user_base as (
    select
        '{{ target_month }}' as stat_month,
        dws_u.user_id,
        u.user_name,
        dws_u.register_date,
        dws_u.user_level,
        dws_u.last_30d_login_days as month_login_days,
        dws_u.last_30d_view_cnt as month_view_cnt,
        dws_u.last_30d_order_cnt as month_order_cnt,
        dws_u.last_30d_order_amount as month_order_amount,
        dws_u.last_30d_return_cnt as month_return_cnt,
        case
            when dws_u.last_30d_order_cnt = 0 then 0
            else dws_u.last_30d_order_amount / dws_u.last_30d_order_cnt
        end as month_avg_order_amount,
        dws_u.preferred_category
    from {{ ref('dws_user_profile') }} dws_u
    left join {{ source('dwd', 'users') }} u on dws_u.user_id = u.user_id
),

prev_snapshot as (
    select
        user_id,
        total_order_amount as prev_total_order_amount,
        total_order_count as prev_total_order_count,
        total_return_count as prev_total_return_count,
        month_order_cnt as prev_month_order_cnt
    from {{ this }}
    where stat_month = DATE_SUB('{{ target_month }}', INTERVAL 1 MONTH)
),

cumulative as (
    select
        ub.user_id,
        coalesce(prev.prev_total_order_amount, 0) + ub.month_order_amount as total_order_amount,
        coalesce(prev.prev_total_order_count, 0) + ub.month_order_cnt as total_order_count,
        coalesce(prev.prev_total_return_count, 0) + ub.month_return_cnt as total_return_count
    from user_base ub
    left join prev_snapshot prev on ub.user_id = prev.user_id
),

user_merchant_daily as (
    select
        o.user_id,
        dp.merchant_id,
        date(o.order_time) as order_date,
        count(distinct o.order_id) as daily_order_cnt
    from {{ source('dwd', 'orders') }} o
    left join {{ source('dwd', 'dim_products') }} dp
        on o.product_id = dp.product_id
    where o.order_time >= DATE_SUB('{{ target_month }}', INTERVAL 1 MONTH)
      and o.order_time < '{{ target_month }}'
    group by o.user_id, dp.merchant_id, date(o.order_time)
),

user_merchant_monthly as (
    select
        user_id,
        merchant_id,
        sum(daily_order_cnt) as monthly_order_cnt
    from user_merchant_daily
    group by user_id, merchant_id
),

suspicious as (
    select
        d.user_id,
        max(case when d.daily_order_cnt > 10 then 1 else 0 end) as has_daily_high,
        max(case when m.monthly_order_cnt > 50 then 1 else 0 end) as has_monthly_high
    from user_merchant_daily d
    left join user_merchant_monthly m
        on d.user_id = m.user_id and d.merchant_id = m.merchant_id
    group by d.user_id
)

select
    ub.stat_month,
    ub.user_id,
    ub.user_name,
    ub.register_date,
    ub.user_level,
    cum.total_order_amount,
    cum.total_order_count,
    cum.total_return_count,
    ub.month_login_days,
    ub.month_view_cnt,
    ub.month_order_cnt,
    ub.month_order_amount,
    ub.month_return_cnt,
    ub.month_avg_order_amount,
    ub.preferred_category,

    case
        when ub.register_date >= '{{ target_month }}'
             and ub.register_date < DATE_ADD('{{ target_month }}', INTERVAL 1 MONTH) then '新客'
        when coalesce(prev.prev_month_order_cnt, 0) > 0
             and ub.month_order_cnt < prev.prev_month_order_cnt * 0.3 then '消费降级（预沉睡）'
        when ub.user_level in ('优质','高潜质','中潜质') then '活跃'
        when ub.month_order_cnt = 0 then '流失'
        else '无明显特征'
    end as lifecycle_stage,
    case
        when ub.month_return_cnt >= 50
             and ub.month_return_cnt / nullif(ub.month_order_cnt, 0) > 0.7 then '异常退货'
        when coalesce(s.has_daily_high, 0) = 1 or coalesce(s.has_monthly_high, 0) = 1 then '刷单嫌疑'
        else '正常'
    end as risk_flag,
    now() as etl_time
from user_base ub
left join cumulative cum on ub.user_id = cum.user_id
left join prev_snapshot prev on ub.user_id = prev.user_id
left join suspicious s on ub.user_id = s.user_id