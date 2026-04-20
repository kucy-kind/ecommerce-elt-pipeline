{{
    config(
        materialized='incremental',
        unique_key=['stat_year'],
        schema='ads'
    )
}}

-- Receive Airflow passed year (first day of next year), format 'YYYY-01-01'
-- Example: target_year = '2026-01-01' means statistics for year 2025

{% set target_year = var('target_year') %}

-- =====================================================
-- 1. Yearly summary: GMV, total orders
-- =====================================================
with year_sum as (
    select
        '{{ target_year }}' as stat_year,
        sum(total_gmv) as total_gmv,
        sum(total_orders) as total_orders,
        sum(total_returns) as total_returns,
        avg(avg_health_score) as avg_health_score,
        sum(total_new_user) as total_new_user,
        avg(avg_conversion_rate) as avg_conversion_rate
    from {{ ref('ads_platform_monthly_summary') }}
    where stat_month >= DATE_SUB('{{ target_year }}', INTERVAL 1 YEAR)
      and stat_month < '{{ target_year }}'
),

-- =====================================================
-- 2. Active merchants (merchants with orders in the year)
-- =====================================================
active_merchants as (
    select
        '{{ target_year }}' as stat_year,
        count(distinct merchant_id) as active_merchants
    from {{ ref('ads_merchant_yearly_snapshot') }}
    where stat_year >= DATE_SUB('{{ target_year }}', INTERVAL 1 YEAR)
      and stat_year < '{{ target_year }}'
      and order_count > 0
),

-- =====================================================
-- 3. Category sales JSON construction
-- =====================================================
monthly_category as (
    select
        category_sales
    from {{ ref('ads_platform_monthly_summary') }}
    where category_sales is not null
      and category_sales != '{}'
      and stat_month >= DATE_SUB('{{ target_year }}', INTERVAL 1 YEAR)
      and stat_month < '{{ target_year }}'
),

-- Expand monthly JSON into rows using JSON_KEYS
unpivot as (
    select
        jt.category_name,
        json_extract(m.category_sales, concat('$.', jt.category_name, '.order_cnt')) as order_cnt,
        json_extract(m.category_sales, concat('$.', jt.category_name, '.gmv')) as gmv
    from monthly_category m,
    json_table(
        json_keys(m.category_sales),
        '$[*]' columns (category_name varchar(50) path '$')
    ) as jt
),

-- Aggregate by category for the whole year
aggregated as (
    select
        category_name,
        sum(gmv) as total_gmv,
        sum(order_cnt) as total_orders
    from unpivot
    group by category_name
),

-- Build yearly JSON
yearly_json as (
    select
        '{{ target_year }}' as stat_year,
        json_objectagg(
            category_name,
            json_object(
                'gmv', total_gmv,
                'order_cnt', total_orders
            )
        ) as category_sales
    from aggregated
),

-- =====================================================
-- 4. User annual retention rate (active users in Jan / active users in Dec)
--    Active defined: any user action in the given month (types as listed)
-- =====================================================
jan_active AS (
    SELECT DISTINCT user_id
    FROM {{ source('dwd', 'user_actions') }}
    WHERE DATE(action_time) >= DATE_SUB('{{ target_year }}', INTERVAL 1 YEAR)
      AND DATE(action_time) < DATE_ADD(DATE_SUB('{{ target_year }}', INTERVAL 1 YEAR), INTERVAL 1 MONTH)
      AND action_type IN ('register','login','browse','favorite','add_cart','purchase','search')
),

dec_active AS (
    SELECT DISTINCT user_id
    FROM {{ source('dwd', 'user_actions') }}
    WHERE DATE(action_time) >= DATE_SUB('{{ target_year }}', INTERVAL 1 MONTH)
      AND DATE(action_time) < '{{ target_year }}'
      AND action_type IN ('register','login','browse','favorite','add_cart','purchase','search')
),

retained AS (
    SELECT user_id
    FROM jan_active
    WHERE user_id IN (SELECT user_id FROM dec_active)
),

user_r as (
    select
        '{{ target_year }}' as stat_year,
        (SELECT COUNT(*) FROM jan_active) AS jan_active_cnt,
        (SELECT COUNT(*) FROM dec_active) AS dec_active_cnt,
        (SELECT COUNT(*) FROM retained) AS retained_cnt,
        CASE
            WHEN (SELECT COUNT(*) FROM jan_active) = 0 THEN 0
            ELSE ROUND((SELECT COUNT(*) FROM retained) * 100.0 / (SELECT COUNT(*) FROM jan_active), 2)
        END AS user_retention_rate
)

-- =====================================================
-- 5. Final output
-- =====================================================
select
    '{{ target_year }}' as stat_year,
    t.total_gmv,
    t.total_orders,
    t.total_returns,
    t.avg_health_score,
    t.total_new_user,
    t.avg_conversion_rate,
    a.active_merchants,
    coalesce(c.category_sales, '{}') as category_sales,
    u.user_retention_rate,
    now() as etl_time
from year_sum t
left join active_merchants a on t.stat_year = a.stat_year
left join yearly_json c on t.stat_year = c.stat_year
left join user_r u on t.stat_year = u.stat_year