{{
    config(
        materialized='incremental',
        unique_key=['stat_month', 'merchant_id'],
        schema='ads'
    )
}}

{% set target_month = var('target_month') %}

with base_merchant as (
    select
        '{{ target_month }}' as stat_month,
        merchant_id,
        merchant_name,
        merchant_level
    from {{ ref('ads_merchant_daily_snapshot') }}
    where stat_date = '{{ target_month }}'
),

cou_merchant as (
    select
        '{{ target_month }}' as stat_month,
        merchant_id,
        sum(daily_sales) as total_sales,
        sum(order_count) as order_count,
        sum(refund_count) as refund_count
    from {{ ref('ads_merchant_daily_snapshot') }}
    where stat_date >= date_sub('{{ target_month }}', interval 1 month)
      and stat_date < '{{ target_month }}'
    group by merchant_id
),

cus_merchant as (
    select
        '{{ target_month }}' as stat_month,
        p.merchant_id,
        count(distinct o.user_id) as customer_count
    from {{ source('dwd', 'orders') }} o
    left join {{ source('dwd', 'dim_products') }} p on o.product_id = p.product_id
    where o.order_time >= date_sub('{{ target_month }}', interval 1 month)
      and o.order_time < '{{ target_month }}'
    group by p.merchant_id
),

sales_his as (
    select
        stat_month,
        merchant_id,
        total_sales
    from {{ this }}
    where stat_month = date_sub('{{ target_month }}', interval 1 month)
),

product_sales as (
    select
        dp.merchant_id,
        dp.product_id,
        dp.product_name,
        count(distinct o.order_id) as product_order_cnt
    from {{ source('dwd', 'orders') }} o
    inner join {{ source('dwd', 'dim_products') }} dp on o.product_id = dp.product_id
    where o.order_time >= date_sub('{{ target_month }}', interval 1 month)
      and o.order_time < '{{ target_month }}'
    group by dp.merchant_id, dp.product_id, dp.product_name
),

merchant_product_count as (
    select
        merchant_id,
        count(distinct product_id) as total_products
    from {{ source('dwd', 'dim_products') }}
    group by merchant_id
),

top_products_ranked as (
    select
        merchant_id,
        product_name,
        product_order_cnt,
        row_number() over (partition by merchant_id order by product_order_cnt desc) as rn
    from product_sales
),

top_products_agg as (
    select
        merchant_id,
        group_concat(product_name order by rn separator ',') as top_products
    from top_products_ranked
    where rn <= 5
    group by merchant_id
),

all_products_agg as (
    select
        merchant_id,
        group_concat(product_name order by product_order_cnt desc separator ',') as all_products
    from product_sales
    group by merchant_id
),

final_top_products as (
    select
        mpc.merchant_id,
        case
            when coalesce(mpc.total_products, 0) < 5 then coalesce(apa.all_products, '')
            else coalesce(tpa.top_products, '')
        end as top_products
    from merchant_product_count mpc
    left join all_products_agg apa on mpc.merchant_id = apa.merchant_id
    left join top_products_agg tpa on mpc.merchant_id = tpa.merchant_id
)

select
    '{{ target_month }}' as stat_month,
    b.merchant_id,
    b.merchant_name,
    b.merchant_level,
    c.total_sales,
    c.order_count,
    coalesce(cs.customer_count, 0) as customer_count,
    c.refund_count,
    case when c.order_count > 0 then c.refund_count / c.order_count else 0 end as refund_rate,
    coalesce(fp.top_products, '') as top_products,
    case
        when c.total_sales > 1.2 * coalesce(pms.total_sales, 0) then '上升'
        when c.total_sales < 0.8 * coalesce(pms.total_sales, 0) then '下降'
        else '平稳'
    end as sales_trend,
    now() as etl_time
from base_merchant b
left join cou_merchant c on b.merchant_id = c.merchant_id and b.stat_month = c.stat_month
left join cus_merchant cs on b.merchant_id = cs.merchant_id and b.stat_month = cs.stat_month
left join final_top_products fp on b.merchant_id = fp.merchant_id
left join sales_his pms on c.merchant_id = pms.merchant_id