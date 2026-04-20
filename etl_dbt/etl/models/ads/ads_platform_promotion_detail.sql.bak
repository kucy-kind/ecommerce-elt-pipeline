{{
    config(
        materialized='incremental',
        unique_key=['stat_month', 'promotion_id', 'user_level'],
        schema='ads'
    )
}}

-- Receive Airflow passed month start date, format 'YYYY-MM-01'
{% set target_month = var('target_month') %}

-- =====================================================
-- 1. Active promotions during the month
-- =====================================================
with active_promotions as (
    select
        promotion_id,
        promotion_name,
        start_time,
        end_time
    from {{ source('dwd', 'dim_promotions') }}
    where promotion_status = 'In Progress'
      and start_time < '{{ target_month }}'
      and end_time >= DATE_SUB('{{ target_month }}', INTERVAL 1 MONTH)
),

-- =====================================================
-- 2. All orders in the month, join user level
-- =====================================================
all_orders as (
    select
        o.order_id,
        o.order_amount,
        coalesce(u.user_level, 'Normal') as user_level,
        o.promotion_id
    from {{ source('dwd', 'orders') }} o
    left join {{ ref('dws_user_profile') }} u on o.user_id = u.user_id
    where o.order_time >= DATE_SUB('{{ target_month }}', INTERVAL 1 MONTH)
      and o.order_time < '{{ target_month }}'
),

-- =====================================================
-- 3. Orders with promotions
-- =====================================================
orders_with_promo as (
    select
        o.order_id,
        o.order_amount,
        o.user_level,
        p.promotion_id,
        p.promotion_name
    from all_orders o
    inner join active_promotions p on o.promotion_id = p.promotion_id
),

-- =====================================================
-- 4. Orders without any promotion (control group)
-- =====================================================
orders_without_promo as (
    select
        order_id,
        order_amount,
        user_level
    from all_orders
    where promotion_id is null
),

-- =====================================================
-- 5. Aggregate promotion orders by promotion and user level
-- =====================================================
promo_stats as (
    select
        promotion_id,
        promotion_name,
        user_level,
        avg(order_amount) as avg_order_amount_promo,
        count(distinct order_id) as promo_order_cnt,
        sum(order_amount) as promo_gmv
    from orders_with_promo
    group by promotion_id, promotion_name, user_level
),

-- =====================================================
-- 6. Aggregate control group orders by user level
-- =====================================================
base_stats as (
    select
        user_level,
        avg(order_amount) as avg_order_amount_base,
        count(distinct order_id) as base_order_cnt,
        sum(order_amount) as base_gmv
    from orders_without_promo
    group by user_level
),

-- =====================================================
-- 7. Combine and calculate lift for each promotion per user level
-- =====================================================
promo_effect_detail as (
    select
        '{{ target_month }}' as stat_month,
        ps.promotion_id,
        ps.promotion_name,
        ps.user_level,
        round(ps.avg_order_amount_promo, 2) as avg_order_amount_promo,
        round(ps.avg_order_amount_base, 2) as avg_order_amount_base,
        round(
            case
                when ps.avg_order_amount_base > 0
                then (ps.avg_order_amount_promo - ps.avg_order_amount_base) / ps.avg_order_amount_base
                else null
            end, 4
        ) as lift,
        ps.promo_order_cnt,
        ps.base_order_cnt,
        round(ps.promo_gmv, 2) as promo_gmv,
        round(ps.base_gmv, 2) as base_gmv,
        now() as etl_time
    from promo_stats ps
    left join base_stats bs on ps.user_level = bs.user_level
)

-- =====================================================
-- 8. Final output: one row per promotion + user level
-- =====================================================
select
    stat_month,
    promotion_id,
    promotion_name,
    user_level,
    avg_order_amount_promo,
    avg_order_amount_base,
    lift,
    promo_order_cnt,
    base_order_cnt,
    promo_gmv,
    base_gmv,
    etl_time
from promo_effect_detail