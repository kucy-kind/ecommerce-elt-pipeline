{{
    config(
        materialized='incremental',
        unique_key=['stat_date', 'merchant_id', 'category_id'],
        schema='dws'
    )
}}

{% set target_date = var('target_date') %}

-- 1. 聚合订单数据（按日期、商家、品类）
WITH order_agg AS (
    SELECT
        DATE(o.order_time) AS stat_date,
        dp.merchant_id,
        dp.product_category_id AS category_id,
        COALESCE(SUM(o.order_amount), 0) AS sales_amount,
        COUNT(DISTINCT o.order_id) AS order_count,
        COUNT(DISTINCT o.user_id) AS customer_count,
        COUNT(DISTINCT o.product_id) AS product_count
    FROM {{ source('dwd', 'orders') }} o
    LEFT JOIN {{ source('dwd', 'dim_products') }} dp
        ON o.product_id = dp.product_id
    WHERE DATE(o.order_time) = '{{ target_date }}'
      AND dp.merchant_id IS NOT NULL
    GROUP BY DATE(o.order_time), dp.merchant_id, dp.product_category_id
),

-- 2. 聚合退货数据（按日期、商家、品类），退款金额从订单表获取
return_agg AS (
    SELECT
        DATE(r.apply_time) AS stat_date,
        dp.merchant_id,
        dp.product_category_id AS category_id,
        COALESCE(SUM(o.order_amount), 0) AS return_amount,  -- 使用订单金额作为退款金额
        COUNT(DISTINCT r.return_id) AS return_count,
        COUNT(DISTINCT r.user_id) AS return_customer_count
    FROM {{ source('dwd', 'returns') }} r
    LEFT JOIN {{ source('dwd', 'orders') }} o
        ON r.order_id = o.order_id
    LEFT JOIN {{ source('dwd', 'dim_products') }} dp
        ON o.product_id = dp.product_id
    WHERE DATE(r.apply_time) = '{{ target_date }}'
      AND dp.merchant_id IS NOT NULL
    GROUP BY DATE(r.apply_time), dp.merchant_id, dp.product_category_id
),

-- 3. 获取所有需要出现的（商家,品类）组合（来自订单或退货）
all_combinations AS (
    SELECT stat_date, merchant_id, category_id
    FROM order_agg
    UNION
    SELECT stat_date, merchant_id, category_id
    FROM return_agg
)

-- 4. 最终输出
SELECT
    ac.stat_date,
    ac.merchant_id,
    ac.category_id,
    COALESCE(o.sales_amount, 0) AS sales_amount,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.customer_count, 0) AS customer_count,
    COALESCE(o.product_count, 0) AS product_count,
    COALESCE(r.return_amount, 0) AS return_amount,
    COALESCE(r.return_count, 0) AS return_count,
    COALESCE(r.return_customer_count, 0) AS return_customer_count,
    -- 显式计算退款率，避免分母为0
    CASE
        WHEN COALESCE(o.order_count, 0) = 0 THEN 0
        ELSE COALESCE(r.return_count, 0) / COALESCE(o.order_count, 0) * 100
    END AS return_rate,
    NOW() AS etl_time
FROM all_combinations ac
LEFT JOIN order_agg o
    ON ac.stat_date = o.stat_date
    AND ac.merchant_id = o.merchant_id
    AND ac.category_id = o.category_id
LEFT JOIN return_agg r
    ON ac.stat_date = r.stat_date
    AND ac.merchant_id = r.merchant_id
    AND ac.category_id = r.category_id