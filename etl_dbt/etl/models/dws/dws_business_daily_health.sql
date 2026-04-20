{{
    config(
        materialized='incremental',
        unique_key=['stat_date'],
        schema='dws'
    )
}}

{% set target_date = var('target_date') %}

-- 订单聚合
WITH order_daily AS (
    SELECT
        DATE(order_time) AS stat_date,
        COALESCE(SUM(order_amount), 0) AS daily_sales,
        COUNT(DISTINCT order_id) AS daily_orders,
        COUNT(DISTINCT user_id) AS daily_order_cus
    FROM {{ source('dwd', 'orders') }}
    WHERE DATE(order_time) = '{{ target_date }}'
    GROUP BY DATE(order_time)
),

-- 退货聚合
return_daily AS (
    SELECT
        DATE(apply_time) AS stat_date,
        COUNT(DISTINCT return_id) AS refund_count
    FROM {{ source('dwd', 'returns') }}
    WHERE DATE(apply_time) = '{{ target_date }}'
    GROUP BY DATE(apply_time)
),

-- 活跃用户（排除 logout）
action_daily1 AS (
    SELECT
        DATE(action_time) AS stat_date,
        COUNT(DISTINCT user_id) AS active_users
    FROM {{ source('dwd', 'user_actions') }}
    WHERE DATE(action_time) = '{{ target_date }}'
      AND action_type IN ('register','login','browse','favorite','add_cart','purchase','search')
    GROUP BY DATE(action_time)
),

-- 新注册用户
action_daily2 AS (
    SELECT
        DATE(action_time) AS stat_date,
        COUNT(DISTINCT user_id) AS new_users
    FROM {{ source('dwd', 'user_actions') }}
    WHERE DATE(action_time) = '{{ target_date }}'
      AND action_type = 'register'
    GROUP BY DATE(action_time)
),

-- 初步合并
combin AS (
    SELECT
        o.stat_date,
        o.daily_sales,
        o.daily_orders,
        a1.active_users,
        a2.new_users,
        -- 转化率 = 下单用户数 / 活跃用户数
        CASE
            WHEN a1.active_users > 0 THEN o.daily_order_cus / a1.active_users
            ELSE 0
        END AS conversion_rate,
        COALESCE(rd.refund_count, 0) AS refund_count,
        -- 退款率 = 退款订单数 / 总订单数
        CASE
            WHEN o.daily_orders > 0 THEN COALESCE(rd.refund_count, 0) / o.daily_orders
            ELSE 0
        END AS refund_rate,
        -- 平均订单金额
        CASE
            WHEN o.daily_orders > 0 THEN o.daily_sales / o.daily_orders
            ELSE 0
        END AS avg_order_amount
    FROM order_daily o
    LEFT JOIN return_daily rd ON o.stat_date = rd.stat_date
    LEFT JOIN action_daily1 a1 ON o.stat_date = a1.stat_date
    LEFT JOIN action_daily2 a2 ON o.stat_date = a2.stat_date
),

-- 计算前30天历史均值（仅在增量运行时查询）
{% if is_incremental() %}
his_avg AS (
    SELECT
        AVG(daily_sales) AS avg_sales,
        AVG(active_users) AS avg_active,
        AVG(new_users) AS avg_new,
        AVG(conversion_rate) AS avg_cvr,
        AVG(refund_rate) AS avg_refund
    FROM {{ this }}
    WHERE stat_date >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY)
      AND stat_date < '{{ target_date }}'
),
{% else %}
his_avg AS (
    SELECT NULL AS avg_sales, NULL AS avg_active, NULL AS avg_new, NULL AS avg_cvr, NULL AS avg_refund
),
{% endif %}

-- 各指标得分（基于历史均值的1.2倍上限，归一化到0-1）
score AS (
    SELECT
        c.stat_date,
        LEAST(c.daily_sales / NULLIF(ha.avg_sales * 1.2, 0), 1) AS d_s,
        LEAST(c.active_users / NULLIF(ha.avg_active * 1.2, 0), 1) AS a_s,
        LEAST(c.new_users / NULLIF(ha.avg_new * 1.2, 0), 1) AS n_s,
        LEAST(c.conversion_rate / NULLIF(ha.avg_cvr * 1.2, 0), 1) AS c_s,
        -- 退款率得分：越低越好，因此反向处理
        1 - LEAST(c.refund_rate / NULLIF(ha.avg_refund * 1.2, 0), 1) AS r_s
    FROM combin c
    CROSS JOIN his_avg ha
)

-- 最终输出
SELECT
    c.stat_date,
    c.daily_sales,
    c.daily_orders,
    c.active_users,
    c.new_users,
    c.conversion_rate,
    c.refund_count,
    c.refund_rate,
    c.avg_order_amount,
    -- 健康分：销售额25% + 活跃用户20% + 转化率20% + 退款率20% + 新用户15%
    ROUND(
        0.25 * s.d_s +
        0.20 * s.a_s +
        0.20 * s.c_s +
        0.20 * s.r_s +
        0.15 * s.n_s,
        0
    ) AS health_score,
    -- 销售额下跌预警（环比下跌20%）
    CASE
        WHEN c.daily_sales <= 0.8 * ha.avg_sales THEN TRUE
        ELSE FALSE
    END AS sales_drop_warning,
    -- 转化率下跌预警（环比下跌10%）
    CASE
        WHEN c.conversion_rate <= 0.9 * ha.avg_cvr THEN TRUE
        ELSE FALSE
    END AS conversion_drop_warning,
    -- 退款率飙升预警（环比上升10%）
    CASE
        WHEN c.refund_rate >= 1.1 * ha.avg_refund THEN TRUE
        ELSE FALSE
    END AS refund_spike_warning,
    NOW() AS etl_time
FROM combin c
CROSS JOIN his_avg ha
LEFT JOIN score s ON c.stat_date = s.stat_date