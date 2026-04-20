{{
    config(
        materialized='incremental',
        unique_key=['stat_year', 'merchant_id'],
        schema='ads'
    )
}}

{% set target_date = var('target_date') %}
{% if target_date is not none %}
    {% set stat_year = target_date[0:4] | int - 1 %}
    {% set year_start = stat_year ~ '-01-01' %}
    {% set year_end = (stat_year + 1) ~ '-01-01' %}

    with merchant_monthly as (
        select *
        from {{ ref('ads_merchant_monthly_snapshot') }}
        where stat_month >= '{{ year_start }}'
          and stat_month < '{{ year_end }}'
    ),

    yearly_agg as (
        select
            merchant_id,
            max(merchant_name) as merchant_name,           
            max(merchant_level) as merchant_level,         
            sum(total_sales) as total_sales,
            sum(order_count) as order_count,
            sum(refund_count) as refund_count,
            
            case
                when sum(order_count) = 0 then 0
                else sum(refund_count) / sum(order_count)
            end as refund_rate
        from merchant_monthly
        group by merchant_id
    ),

    customer_count_annual as (
        select
            dp.merchant_id,
            count(distinct o.user_id) as customer_count
        from {{ source('dwd', 'orders') }} o
        inner join {{ source('dwd', 'dim_products') }} dp on o.product_id = dp.product_id
        where o.order_time >= '{{ year_start }}'
          and o.order_time < '{{ year_end }}'
        group by dp.merchant_id
    ),

    prev_year as (
        select
            merchant_id,
            total_sales as prev_total_sales
        from {{ this }}
        where stat_year = {{ stat_year - 1 }}
    )

    select
        {{ stat_year }} as stat_year,
        ya.merchant_id,
        ya.merchant_name,
        ya.merchant_level,
        round(ya.total_sales, 2) as total_sales,
        ya.order_count,
        coalesce(cc.customer_count, 0) as customer_count,
        ya.refund_count,
        round(ya.refund_rate, 4) as refund_rate,
        case
            when coalesce(py.prev_total_sales, 0) = 0 then null
            else round((ya.total_sales - py.prev_total_sales) / py.prev_total_sales * 100, 2)
        end as yoy_sales_growth,
        now() as etl_time
    from yearly_agg ya
    left join customer_count_annual cc on ya.merchant_id = cc.merchant_id
    left join prev_year py on ya.merchant_id = py.merchant_id

{% else %}
    {{ log("WARNING: target_date not provided for ads_merchant_yearly_snapshot, returning empty result", info=True) }}
    select * from (select 1 as dummy) where 1=0
{% endif %}