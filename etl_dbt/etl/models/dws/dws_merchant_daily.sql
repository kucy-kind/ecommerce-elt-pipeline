{{
    config(
        materialized='incremental',
        unique_key=['stat_date', 'merchant_id'],
        schema='dws'
    )
}}

-- 接收 Airflow 传入的日期变量，格式 'YYYY-MM-DD'
{% set target_date = var('target_date') %}

-- 获得所有商家id,确保所有商家都可以获得记录
WITH merchant AS (
    SELECT DISTINCT merchant_id
    FROM {{ source('dwd', 'dim_products') }}
),

-- 获得订单相关的商家数据
order_merchant AS (
    SELECT
        DATE(o.order_time) AS stat_date,
        dp.merchant_id,
        COALESCE(SUM(o.order_amount), 0) AS daily_sales,
        COUNT(DISTINCT o.order_id) AS order_count,
        COUNT(DISTINCT o.user_id) AS customer_count
    FROM {{ source('dwd', 'orders') }} o
    LEFT JOIN {{ source('dwd', 'dim_products') }} dp ON o.product_id = dp.product_id
    WHERE DATE(o.order_time) = '{{ target_date }}'
      AND dp.merchant_id IS NOT NULL
    GROUP BY DATE(o.order_time), dp.merchant_id
),

-- 获得退货相关的商家数据
return_merchant AS (
    SELECT
        r.merchant_id,
        COUNT(DISTINCT r.return_id) AS refund_count
    FROM {{ source('dwd', 'returns') }} r
    WHERE DATE(r.apply_time) = '{{ target_date }}'
    GROUP BY r.merchant_id
),

-- 合并所有信息
all_k_m AS (
    SELECT
        '{{ target_date }}' AS stat_date,
        m.merchant_id,
        COALESCE(om.daily_sales, 0) AS daily_sales,
        COALESCE(om.order_count, 0) AS order_count,
        COALESCE(om.customer_count, 0) AS customer_count,
        COALESCE(rm.refund_count, 0) AS refund_count,
        CASE
            WHEN COALESCE(om.order_count, 0) = 0 THEN 0
            ELSE COALESCE(rm.refund_count, 0) / COALESCE(om.order_count, 1) * 100
        END AS refund_rate
    FROM merchant m
    LEFT JOIN order_merchant om ON m.merchant_id = om.merchant_id
    LEFT JOIN return_merchant rm ON m.merchant_id = rm.merchant_id
),

-- 获取前一天和上周同一天的历史数据（仅在增量运行时查询）
{% if is_incremental() %}
history_record AS (
    SELECT
        stat_date,
        merchant_id,
        daily_sales AS hist_sales,
        order_count AS hist_orders
    FROM {{ this }}
    WHERE stat_date IN (
        DATE_SUB('{{ target_date }}', INTERVAL 1 DAY),
        DATE_SUB('{{ target_date }}', INTERVAL 7 DAY)
    )
),
{% else %}
history_record AS (
    SELECT NULL AS stat_date, NULL AS merchant_id, NULL AS hist_sales, NULL AS hist_orders
    WHERE 1 = 0
),
{% endif %}

-- 计算近30天均值和标准差（仅在增量运行时查询）
{% if is_incremental() %}
stats_30d AS (
    SELECT
        merchant_id,
        AVG(daily_sales) AS avg_sales_30d,
        STDDEV(daily_sales) AS stddev_sales_30d
    FROM {{ this }}
    WHERE stat_date >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY)
      AND stat_date < '{{ target_date }}'
    GROUP BY merchant_id
),
{% else %}
stats_30d AS (
    SELECT NULL AS merchant_id, NULL AS avg_sales_30d, NULL AS stddev_sales_30d
    WHERE 1 = 0
),
{% endif %}

-- 将历史记录按日期分别引用
h_yest AS (
    SELECT merchant_id, hist_sales, hist_orders
    FROM history_record
    WHERE stat_date = DATE_SUB('{{ target_date }}', INTERVAL 1 DAY)
),
h_week AS (
    SELECT merchant_id, hist_sales, hist_orders
    FROM history_record
    WHERE stat_date = DATE_SUB('{{ target_date }}', INTERVAL 7 DAY)
)

SELECT
    c.stat_date,
    c.merchant_id,
    c.daily_sales,
    c.order_count,
    c.customer_count,
    c.refund_count,
    -- 销售额环比
    CASE
        WHEN y.hist_sales IS NULL OR y.hist_sales = 0 THEN NULL
        ELSE (c.daily_sales - y.hist_sales) / y.hist_sales * 100
    END AS sales_mom,
    -- 销售额同比
    CASE
        WHEN w.hist_sales IS NULL OR w.hist_sales = 0 THEN NULL
        ELSE (c.daily_sales - w.hist_sales) / w.hist_sales * 100
    END AS sales_yoy,
    -- 订单量环比
    CASE
        WHEN y.hist_orders IS NULL OR y.hist_orders = 0 THEN NULL
        ELSE (c.order_count - y.hist_orders) / y.hist_orders * 100
    END AS order_count_mom,
    -- 退款率（已计算）
    c.refund_rate,
    -- 异常销售额标志
    CASE
        WHEN s.avg_sales_30d IS NOT NULL
             AND c.daily_sales > s.avg_sales_30d + 3 * s.stddev_sales_30d
        THEN TRUE
        ELSE FALSE
    END AS abnormal_sales_flag,
    -- 零销售额标志
    CASE WHEN c.daily_sales = 0 THEN TRUE ELSE FALSE END AS zero_sales_flag,
    NOW() AS etl_time
FROM all_k_m c
LEFT JOIN h_yest y ON c.merchant_id = y.merchant_id
LEFT JOIN h_week w ON c.merchant_id = w.merchant_id
LEFT JOIN stats_30d s ON c.merchant_id = s.merchant_id