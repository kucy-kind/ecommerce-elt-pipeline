{{
    config(
        materialized='incremental',
        unique_key=['user_id','stat_date'],
        schema='dws'
    )
}}

{% set target_date = var('target_date') %}

-- 定义变化窗口（天）
{% set change_window = 30 %}

-- ================= 1. 识别需要更新的用户 =================
with users_to_update as (
    select user_id
    from {{ source('dwd', 'users') }}
    where updated_time >= date_sub('{{ target_date }}', interval {{ change_window }} day)
    {% if is_incremental() %}
    union
    select distinct user_id
    from {{ source('dwd', 'orders') }}
    where order_time >= date_sub('{{ target_date }}', interval {{ change_window }} day)
    union
    select distinct user_id
    from {{ source('dwd', 'returns') }}
    where apply_time >= date_sub('{{ target_date }}', interval {{ change_window }} day)
    union
    select distinct user_id
    from {{ source('dwd', 'user_actions') }}
    where action_time >= date_sub('{{ target_date }}', interval {{ change_window }} day)
    {% endif %}
),

-- ================= 2. 用户订单统计（只针对变化用户） =================
user_orders AS (
    SELECT
        o.user_id,
        COUNT(DISTINCT o.order_id) AS total_order_count,
        COALESCE(SUM(o.order_amount), 0) AS total_order_amount,
        MAX(o.order_time) AS last_order_date,
        SUM(CASE WHEN o.order_time >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY) THEN o.order_amount ELSE 0 END) AS last_30d_order_amount,
        COUNT(DISTINCT CASE WHEN o.order_time >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY) THEN o.order_id END) AS last_30d_order_cnt
    FROM {{ source('dwd', 'orders') }} o
    {% if is_incremental() %}
    inner join users_to_update u on o.user_id = u.user_id
    {% endif %}
    GROUP BY o.user_id
),

-- ================= 3. 用户退货统计（只针对变化用户） =================
user_returns AS (
    SELECT
        r.user_id,
        COUNT(DISTINCT r.return_id) AS total_return_count,
        COUNT(DISTINCT CASE WHEN r.apply_time >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY) THEN r.return_id END) AS last_30d_return_cnt
    FROM {{ source('dwd', 'returns') }} r
    {% if is_incremental() %}
    inner join users_to_update u on r.user_id = u.user_id
    {% endif %}
    GROUP BY r.user_id
),

-- ================= 4. 用户行为统计（只针对变化用户） =================
user_actions AS (
    SELECT
        a.user_id,
        COUNT(DISTINCT CASE WHEN a.action_type = 'login' AND a.action_time >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY) THEN DATE(a.action_time) END) AS last_30d_login_days,
        COUNT(CASE WHEN a.action_type = 'browse' AND a.action_time >= DATE_SUB('{{ target_date }}', INTERVAL 30 DAY) THEN 1 END) AS last_30d_view_cnt
    FROM {{ source('dwd', 'user_actions') }} a
    {% if is_incremental() %}
    inner join users_to_update u on a.user_id = u.user_id
    {% endif %}
    GROUP BY a.user_id
),

-- ================= 5. 用户品类购买次数（只针对变化用户） =================
user_category_counts AS (
    SELECT
        o.user_id,
        p.category_name,
        COUNT(*) AS buy_count
    FROM {{ source('dwd', 'orders') }} o
    LEFT JOIN {{ source('dwd', 'dim_products') }} p ON o.product_id = p.product_id
    {% if is_incremental() %}
    inner join users_to_update u on o.user_id = u.user_id
    {% endif %}
    GROUP BY o.user_id, p.category_name
),

-- 购买次数 >5 的品类拼接
gt5_categories AS (
    SELECT
        user_id,
        GROUP_CONCAT(category_name ORDER BY buy_count DESC SEPARATOR ',') AS pref_gt5
    FROM user_category_counts
    WHERE buy_count > 5
    GROUP BY user_id
),

-- 购买次数最多的品类
max_category AS (
    SELECT
        user_id,
        category_name AS pref_max
    FROM (
        SELECT
            user_id,
            category_name,
            buy_count,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY buy_count DESC, category_name) AS rn
        FROM user_category_counts
    ) t
    WHERE rn = 1
),

user_preferred_category AS (
    SELECT
        u.user_id,
        COALESCE(gt5.pref_gt5, maxcat.pref_max) AS preferred_category
    FROM (SELECT DISTINCT user_id FROM user_category_counts) u
    LEFT JOIN gt5_categories gt5 ON u.user_id = gt5.user_id
    LEFT JOIN max_category maxcat ON u.user_id = maxcat.user_id
),

-- ================= 6. 用户得分计算（只针对变化用户） =================
user_scores AS (
    SELECT
        u.user_id,
        ord.total_order_count,
        ord.total_order_amount,
        ord.last_order_date,
        ord.last_30d_order_cnt,
        ord.last_30d_order_amount,
        ret.total_return_count,
        ret.last_30d_return_cnt,
        act.last_30d_login_days,
        act.last_30d_view_cnt,
        -- 登录天数得分
        (CASE WHEN COALESCE(act.last_30d_login_days, 0) > 27 THEN 15
              WHEN COALESCE(act.last_30d_login_days, 0) > 20 THEN 10
              WHEN COALESCE(act.last_30d_login_days, 0) > 10 THEN 5
              ELSE 0 END) AS login_score,
        -- 订单次数得分
        (CASE WHEN COALESCE(ord.last_30d_order_cnt, 0) > 10 THEN 15
              WHEN COALESCE(ord.last_30d_order_cnt, 0) > 5 THEN 5
              ELSE 0 END) AS order_cnt_score,
        -- 消费金额得分
        (CASE WHEN COALESCE(ord.last_30d_order_amount, 0) > 50000 THEN 55
              WHEN COALESCE(ord.last_30d_order_amount, 0) > 10000 THEN 30
              WHEN COALESCE(ord.last_30d_order_amount, 0) > 1000 THEN 15
              ELSE 0 END) AS amount_score
    FROM {{ source('dwd', 'users') }} u
    LEFT JOIN user_orders ord ON u.user_id = ord.user_id
    LEFT JOIN user_returns ret ON u.user_id = ret.user_id
    LEFT JOIN user_actions act ON u.user_id = act.user_id
    {% if is_incremental() %}
    inner join users_to_update upd on u.user_id = upd.user_id
    {% endif %}
)

-- ================= 7. 最终输出 =================
SELECT
    '{{target_date}}' as stat_date,
    s.user_id,
    DATE(u.created_time) AS register_date,
    COALESCE(s.total_order_amount, 0) AS total_order_amount,
    COALESCE(s.total_order_count, 0) AS total_order_count,
    COALESCE(s.total_return_count, 0) AS total_return_count,
    s.last_order_date,
    COALESCE(s.last_30d_login_days, 0) AS last_30d_login_days,
    COALESCE(s.last_30d_view_cnt, 0) AS last_30d_view_cnt,
    COALESCE(s.last_30d_order_cnt, 0) AS last_30d_order_cnt,
    COALESCE(s.last_30d_order_amount, 0) AS last_30d_order_amount,
    COALESCE(s.last_30d_return_cnt, 0) AS last_30d_return_cnt,
    CASE
        WHEN s.last_30d_order_cnt > 0
        THEN s.last_30d_order_amount / s.last_30d_order_cnt
        ELSE 0
    END AS avg_order_amount_30d,
    (s.login_score + s.order_cnt_score + s.amount_score) AS user_score,
    CASE
        WHEN (s.login_score + s.order_cnt_score + s.amount_score) >= 80 THEN '优质'
        WHEN (s.login_score + s.order_cnt_score + s.amount_score) >= 60 THEN '高潜质'
        WHEN (s.login_score + s.order_cnt_score + s.amount_score) >= 30 THEN '中潜质'
        ELSE '普通'
    END AS user_level,
    COALESCE(pc.preferred_category, '未知') AS preferred_category,
    NOW() AS etl_time
FROM user_scores s
LEFT JOIN {{ source('dwd', 'users') }} u ON s.user_id = u.user_id
LEFT JOIN user_preferred_category pc ON s.user_id = pc.user_id