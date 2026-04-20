{{
    config(
        materialized='incremental',
        unique_key=['stat_date','product_id'],
        schema='dws'
    )
}}

-- 接收 Airflow 传入的统计日期，格式 'YYYY-MM-DD'
{% set target_date = var('target_date') %}

-- ================= 1. 定义哪些商品需要重新计算 =================
{% set product_change_window = 30 %}

with products_to_update as (
    -- 商品基础信息最近有更新的产品
    select product_id
    from {{ source('dwd', 'dim_products') }}
    where updated_time >= date_sub('{{ target_date }}', interval {{ product_change_window }} day)
    {% if is_incremental() %}
    union
    -- 近 {{ product_change_window }} 天有订单的产品
    select distinct product_id
    from {{ source('dwd', 'orders') }}
    where order_time >= date_sub('{{ target_date }}', interval {{ product_change_window }} day)
    union
    -- 近 {{ product_change_window }} 天有退货的产品
    select distinct dp.product_id
    from {{ source('dwd', 'returns') }} r
    left join {{ source('dwd', 'orders') }} o on r.order_id = o.order_id
    left join {{ source('dwd', 'dim_products') }} dp on o.product_id = dp.product_id
    where r.apply_time >= date_sub('{{ target_date }}', interval {{ product_change_window }} day)
      and dp.product_id is not null
    {% endif %}
),

-- ================= 2. 商品基础信息（只取需要更新的商品） =================
product_base AS (
    SELECT
        p.product_id,
        p.merchant_id,
        p.product_name,
        p.product_category_id AS category_id,
        p.category_name,
        p.listing_time AS shelf_time,
        p.product_status,
        p.updated_time AS last_status_change,
        p.current_stock
    FROM {{ source('dwd', 'dim_products') }} p
    {% if is_incremental() %}
    inner join products_to_update u on p.product_id = u.product_id
    {% endif %}
),

-- ================= 3. 订单累计指标（全量计算，只针对目标商品） =================
order_lifetime AS (
    SELECT
        dp.product_id,
        COALESCE(SUM(o.order_amount), 0) AS total_sales_amount,
        COUNT(DISTINCT o.order_id) AS total_sales_quantity,   -- 用订单数代替销量
        COUNT(DISTINCT o.order_id) AS order_count
    FROM {{ source('dwd', 'orders') }} o
    LEFT JOIN {{ source('dwd', 'dim_products') }} dp ON o.product_id = dp.product_id
    WHERE dp.product_id IS NOT NULL
      {% if is_incremental() %}
      and dp.product_id in (select product_id from products_to_update)
      {% endif %}
    GROUP BY dp.product_id
),

-- ================= 4. 订单近期指标（近7天） =================
order_recent AS (
    SELECT
        dp.product_id,
        COUNT(DISTINCT o.order_id) AS last_7d_sales_quantity   -- 用订单数代替销量
    FROM {{ source('dwd', 'orders') }} o
    LEFT JOIN {{ source('dwd', 'dim_products') }} dp ON o.product_id = dp.product_id
    WHERE dp.product_id IS NOT NULL
      AND DATE(o.order_time) >= DATE_SUB('{{ target_date }}', INTERVAL 7 DAY)
      AND DATE(o.order_time) < '{{ target_date }}'
      {% if is_incremental() %}
      and dp.product_id in (select product_id from products_to_update)
      {% endif %}
    GROUP BY dp.product_id
),

-- ================= 5. 退货累计指标 =================
return_lifetime AS (
    SELECT
        dp.product_id,
        COUNT(DISTINCT r.return_id) AS total_return_times
    FROM {{ source('dwd', 'returns') }} r
    LEFT JOIN {{ source('dwd', 'orders') }} o ON r.order_id = o.order_id
    LEFT JOIN {{ source('dwd', 'dim_products') }} dp ON o.product_id = dp.product_id
    WHERE dp.product_id IS NOT NULL
      {% if is_incremental() %}
      and dp.product_id in (select product_id from products_to_update)
      {% endif %}
    GROUP BY dp.product_id
),

-- ================= 6. 退货近期指标（近7天） =================
return_recent AS (
    SELECT
        dp.product_id,
        COUNT(DISTINCT r.return_id) AS last_7d_return_times
    FROM {{ source('dwd', 'returns') }} r
    LEFT JOIN {{ source('dwd', 'orders') }} o ON r.order_id = o.order_id
    LEFT JOIN {{ source('dwd', 'dim_products') }} dp ON o.product_id = dp.product_id
    WHERE dp.product_id IS NOT NULL
      AND DATE(r.apply_time) >= DATE_SUB('{{ target_date }}', INTERVAL 7 DAY)
      AND DATE(r.apply_time) < '{{ target_date }}'
      {% if is_incremental() %}
      and dp.product_id in (select product_id from products_to_update)
      {% endif %}
    GROUP BY dp.product_id
),

-- ================= 7. 库存周转天数（基于近7天日均销量） =================
turnover_days AS (
    SELECT
        p.product_id,
        CASE
            WHEN COALESCE(rec.last_7d_sales_quantity, 0) > 0
            THEN p.current_stock / (rec.last_7d_sales_quantity / 7.0)
            ELSE NULL
        END AS stock_turnover_days
    FROM product_base p
    LEFT JOIN order_recent rec ON p.product_id = rec.product_id
)

-- ================= 8. 最终合并 =================
SELECT
    '{{target_date}}' AS stat_date,
    p.product_id,
    p.merchant_id,
    p.product_name,
    p.category_id,
    p.category_name,
    p.shelf_time,
    p.product_status,
    p.last_status_change,
    COALESCE(ol.total_sales_amount, 0) AS total_sales_amount,
    COALESCE(ol.total_sales_quantity, 0) AS total_sales_quantity,
    p.current_stock,
    td.stock_turnover_days,
    COALESCE(rl.total_return_times, 0) AS total_return_times,
    -- 退货率 = 累计退货次数 / 累计销量（若销量为0则0）
    CASE
        WHEN COALESCE(ol.total_sales_quantity, 0) = 0 THEN 0
        ELSE COALESCE(rl.total_return_times, 0) / COALESCE(ol.total_sales_quantity, 1)
    END AS return_rate,
    COALESCE(rec.last_7d_sales_quantity, 0) AS last_7d_sales_quantity,
    COALESCE(rrec.last_7d_return_times, 0) AS last_7d_return_times,
    -- 库存积压标志
    CASE WHEN td.stock_turnover_days > 15 THEN TRUE ELSE FALSE END AS stock_over_15d_flag,
    CASE WHEN td.stock_turnover_days > 30 THEN TRUE ELSE FALSE END AS stock_over_30d_flag,
    NOW() AS etl_time
FROM product_base p
LEFT JOIN order_lifetime ol ON p.product_id = ol.product_id
LEFT JOIN order_recent rec ON p.product_id = rec.product_id
LEFT JOIN return_lifetime rl ON p.product_id = rl.product_id
LEFT JOIN return_recent rrec ON p.product_id = rrec.product_id
LEFT JOIN turnover_days td ON p.product_id = td.product_id