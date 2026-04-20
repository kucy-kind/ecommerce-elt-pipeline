{{
    config(
        materialized='incremental',
        unique_key=['stat_date', 'merchant_id'],
        schema='dws'
    )
}}

{% set target_date = var('target_date') %}

-- 全部商家基础信息
WITH merchant_base AS (
    SELECT
        merchant_id,
        DATE(register_time) AS register_date,
        merchant_name,
        updated_time   -- 用于增量判断
    FROM {{ source('dwd', 'dim_merchants') }}
),

-- 商品统计
product_stats AS (
    SELECT
        merchant_id,
        COUNT(DISTINCT product_id) AS total_product_count,
        COUNT(DISTINCT CASE WHEN product_status = 'active' THEN product_id END) AS on_sale_product_count
    FROM {{ source('dwd', 'dim_products') }}
    GROUP BY merchant_id
),

-- 订单累计指标：直接从 dws_merchant_daily 聚合
order_lifetime AS (
    SELECT
        merchant_id,
        SUM(daily_sales) AS total_sales_amount,
        SUM(order_count) AS total_order_count
    FROM {{ ref('dws_merchant_daily') }}
    GROUP BY merchant_id
),

-- 订单近期指标（近30天）
order_recent AS (
    SELECT
        merchant_id,
        SUM(daily_sales) AS last_30d_sales_amount,
        SUM(order_count) AS last_30d_order_count
    FROM {{ ref('dws_merchant_daily') }}
    WHERE stat_date >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY)
      AND stat_date < '{{ target_date }}'
    GROUP BY merchant_id
),

-- 退货累计指标
return_lifetime AS (
    SELECT
        merchant_id,
        COUNT(DISTINCT return_id) AS total_return_count
    FROM {{ source('dwd', 'returns') }}
    GROUP BY merchant_id
),

-- 退货近期指标（近30天）
return_recent AS (
    SELECT
        merchant_id,
        COUNT(DISTINCT return_id) AS last_30d_return_count
    FROM {{ source('dwd', 'returns') }}
    WHERE DATE(apply_time) >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY)
      AND DATE(apply_time) < '{{ target_date }}'
    GROUP BY merchant_id
),

-- 订单处理时长（小时）
order_response AS (
    SELECT
        dp.merchant_id,
        AVG(TIMESTAMPDIFF(HOUR, o.order_time, o.shipping_time)) AS avg_response_time_hours_shop
    FROM {{ source('dwd', 'orders') }} o
    LEFT JOIN {{ source('dwd', 'dim_products') }} dp ON o.product_id = dp.product_id
    WHERE dp.merchant_id IS NOT NULL
      AND o.shipping_time IS NOT NULL
      AND o.order_time IS NOT NULL
    GROUP BY dp.merchant_id
),

-- 退货处理时长（小时）
return_response AS (
    SELECT
        merchant_id,
        AVG(
            CASE
                WHEN approved_time IS NOT NULL THEN TIMESTAMPDIFF(HOUR, apply_time, approved_time)
                WHEN rejected_time IS NOT NULL THEN TIMESTAMPDIFF(HOUR, apply_time, rejected_time)
                ELSE NULL
            END
        ) AS avg_response_time_hours_return
    FROM {{ source('dwd', 'returns') }}
    WHERE apply_time IS NOT NULL
      AND (approved_time IS NOT NULL OR rejected_time IS NOT NULL)
    GROUP BY merchant_id
),

-- 每日销售额统计（基于 dws_merchant_daily，仅计算过去30天的每日聚合）
daily_stats AS (
    SELECT
        merchant_id,
        AVG(daily_sales) AS daily_sales_avg_30d,
        STDDEV(daily_sales) AS daily_sales_stddev_30d,
        SUM(CASE WHEN daily_sales = 0 THEN 1 ELSE 0 END) AS zero_sales_days_7d
    FROM {{ ref('dws_merchant_daily') }}
    WHERE stat_date >= DATE_SUB('{{ target_date }}', INTERVAL 7 DAY)
      AND stat_date < '{{ target_date }}'
    GROUP BY merchant_id
),

-- 突然下架标志（最近一天内下架商品数超过5）
sudden_offline AS (
    SELECT
        merchant_id,
        COUNT(*) AS offline_count
    FROM {{ source('dwd', 'merchant_operations') }}
    WHERE operation_type = 'remove_product'
      AND DATE(operation_time) >= DATE_SUB('{{ target_date }}', INTERVAL 1 DAY)
      AND DATE(operation_time) < '{{ target_date }}'
    GROUP BY merchant_id
)

-- 最终合并
SELECT
    '{{ target_date }}' AS stat_date,   -- 补全逗号
    mb.merchant_id,
    mb.register_date,
    mb.merchant_name,
    CASE
        WHEN COALESCE(ol.total_order_count, 0) = 0 THEN 'D'
        ELSE
            CASE
                WHEN 100 - POWER((COALESCE(rl.total_return_count, 0) / COALESCE(ol.total_order_count, 1)) * 100, 2) >= 80 THEN 'A'
                WHEN 100 - POWER((COALESCE(rl.total_return_count, 0) / COALESCE(ol.total_order_count, 1)) * 100, 2) >= 60 THEN 'B'
                WHEN 100 - POWER((COALESCE(rl.total_return_count, 0) / COALESCE(ol.total_order_count, 1)) * 100, 2) >= 40 THEN 'C'
                ELSE 'D'
            END
    END AS merchant_level,
    COALESCE(ps.total_product_count, 0) AS total_product_count,
    COALESCE(ps.on_sale_product_count, 0) AS on_sale_product_count,
    COALESCE(ol.total_sales_amount, 0) AS total_sales_amount,
    COALESCE(ol.total_order_count, 0) AS total_order_count,
    COALESCE(rl.total_return_count, 0) AS total_return_count,
    COALESCE(ors.avg_response_time_hours_shop, 0) AS avg_response_time_hours_shop,
    COALESCE(rrs.avg_response_time_hours_return, 0) AS avg_response_time_hours_return,
    COALESCE(orr.last_30d_sales_amount, 0) AS last_30d_sales_amount,
    COALESCE(orr.last_30d_order_count, 0) AS last_30d_order_count,
    COALESCE(rr.last_30d_return_count, 0) AS last_30d_return_count,
    CASE
        WHEN COALESCE(orr.last_30d_order_count, 0) = 0 THEN 0
        ELSE COALESCE(rr.last_30d_return_count, 0) / COALESCE(orr.last_30d_order_count, 1)
    END AS last_30d_return_rate,
    COALESCE(ds.daily_sales_avg_30d, 0) AS daily_sales_avg_30d,
    COALESCE(ds.daily_sales_stddev_30d, 0) AS daily_sales_stddev_30d,
    COALESCE(ds.zero_sales_days_7d, 0) AS zero_sales_days_7d,
    CASE WHEN COALESCE(so.offline_count, 0) > 5 THEN TRUE ELSE FALSE END AS sudden_offline_flag,
    NOW() AS etl_time
FROM merchant_base mb
LEFT JOIN product_stats ps ON mb.merchant_id = ps.merchant_id
LEFT JOIN order_lifetime ol ON mb.merchant_id = ol.merchant_id
LEFT JOIN order_recent orr ON mb.merchant_id = orr.merchant_id
LEFT JOIN return_lifetime rl ON mb.merchant_id = rl.merchant_id
LEFT JOIN return_recent rr ON mb.merchant_id = rr.merchant_id
LEFT JOIN order_response ors ON mb.merchant_id = ors.merchant_id
LEFT JOIN return_response rrs ON mb.merchant_id = rrs.merchant_id
LEFT JOIN daily_stats ds ON mb.merchant_id = ds.merchant_id
LEFT JOIN sudden_offline so ON mb.merchant_id = so.merchant_id
{% if is_incremental() %}
    -- 增量逻辑：只处理最近30天内有变化的商家
    WHERE mb.merchant_id IN (
        SELECT DISTINCT merchant_id FROM (
            SELECT merchant_id FROM {{ ref('dws_merchant_daily') }} WHERE stat_date >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY)
            UNION
            SELECT merchant_id FROM {{ source('dwd', 'returns') }} WHERE apply_time >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY)
            UNION
            SELECT merchant_id FROM {{ source('dwd', 'merchant_operations') }} WHERE operation_time >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY)
            UNION
            SELECT merchant_id FROM {{ source('dwd', 'dim_merchants') }} WHERE updated_time >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY)
        ) t
    )
{% endif %}