CREATE TABLE dws_user_profile (
    user_id VARCHAR(50) PRIMARY KEY,               -- 用户ID，作为主键（假设每个用户一条最新记录）

    -- 基础属性（可从ODS/DWD同步或更新）
    register_date DATE,                    -- 注册日期 create_time

    -- 累计指标（全量）
    total_order_amount DECIMAL(10,2),       -- 累计消费金额 count orders group by user_id
    total_order_count INT,                  -- 累计订单数
    total_return_count INT,                  -- 累计退货次数 count return group by user_id
    last_order_date DATE,                   -- 最近下单日期 全订单日期

    -- 近期行为指标（用于监控与动态标签）
    last_30d_login_days INT,                 -- 近30天登录天数 action
    last_30d_view_cnt INT,                    -- 近30天浏览商品次数 action
    last_30d_order_cnt INT,                   -- 近30天下单次数 order
    last_30d_order_amount DECIMAL(10,2),      -- 近30天下单金额 order
    last_30d_return_cnt INT,                   -- 近30天退货次数 return

    -- 衍生指标（用于监控异常）
    avg_order_amount_30d DECIMAL(10,2),       -- 近30天平均客单价 order_amount/order_cnt
    user_level VARCHAR(20),                    -- 用户等级（如：新用户、活跃、沉默、流失） 有一个评价标准函数
    preferred_category VARCHAR(100),           -- 偏好品类（根据近30天购买最多的品类）

    -- 元数据
    etl_time DATETIME                          -- 最后ETL时间
);

CREATE TABLE dws_merchant_profile (
    merchant_id  VARCHAR(50) PRIMARY KEY,

    -- 基础信息
    register_date DATE,
    merchant_name VARCHAR(100),
    merchant_level VARCHAR(20),      -- 开的时间，销售量，退货率

    -- 经营规模
    total_product_count INT,                 -- 累计上架商品数（去重）
    on_sale_product_count INT,                -- 当前在售商品数
    last_product_status_change DATE,          -- 最后商品状态变更日期（用于监控突然下架）

    -- 销售与售后指标
    total_sales_amount DECIMAL(12,2),          -- 累计销售额
    total_order_count INT,                     -- 累计订单量
    total_return_count INT,                     -- 累计退货量
    avg_response_time_hours DECIMAL(5,2),       -- 平均响应时长（小时）
    avg_product_rating DECIMAL(3,2),            -- 商品平均评分

    -- 近期指标（用于环比/同比监控）
    last_30d_sales_amount DECIMAL(12,2),        -- 近30天销售额
    last_30d_order_count INT,                   -- 近30天订单量
    last_30d_return_count INT,                   -- 近30天退货量
    last_30d_return_rate DECIMAL(5,2),           -- 近30天退货率

    -- 监控衍生字段
    daily_sales_avg_30d DECIMAL(12,2),          -- 近30天日均销售额（用于判断单日异常高）
    daily_sales_stddev_30d DECIMAL(12,2),        -- 近30天销售额标准差
    zero_sales_days_7d INT,                      -- 过去7天零销售额天数
    sudden_offline_flag BOOLEAN,                 -- 标记是否突然下架（可基于last_product_status_change计算）

    etl_time DATETIME
);

CREATE TABLE dws_product_summary (
    product_id VARCHAR(50) PRIMARY KEY,
    merchant_id VARCHAR(50),                           -- 所属商家ID（便于关联商家维度）

    -- 基础信息
    product_name VARCHAR(200),
    category_id VARCHAR(50),
    category_name VARCHAR(100),
    shelf_time DATETIME,                       -- 上架时间
    product_status VARCHAR(20),                  -- 当前状态（上架/下架）
    last_status_change DATETIME,                 -- 最后状态变更时间

    -- 销售与库存
    total_sales_amount DECIMAL(12,2),            -- 累计销售额
    total_sales_quantity INT,                     -- 累计销量
    current_stock INT,                            -- 当前库存
    stock_turnover_days DECIMAL(10,2),            -- 库存周转天数（=当前库存/日均销量）

    -- 反馈指标
    total_return_times INT,                        -- 累计退货次数
    return_rate DECIMAL(5,2),                      -- 退货率

    -- 近期指标（用于监控）
    last_7d_sales_quantity INT,                    -- 近7天销量
    last_7d_return_times INT,                       -- 近7天退货次数

    -- 异常标记（可通过dbt逻辑生成）
    stock_over_15d_flag BOOLEAN,                    -- 库存积压（例如周转天数>15）支付时间发货时间
    stock_over_30d_flag BOOLEAN,
    etl_time DATETIME
);

CREATE TABLE dws_daily_sales (
    stat_date DATE NOT NULL,
    merchant_id VARCHAR(50) NOT NULL,
    category_id VARCHAR(50) NOT NULL,               -- 商品大类ID，可设-1表示全类汇总,这个商家所有产品大类
    PRIMARY KEY (stat_date, merchant_id, category_id),

    -- 销售指标
    sales_amount DECIMAL(12,2) DEFAULT 0,    -- 销售额
    order_count INT DEFAULT 0,                -- 订单量
    customer_count INT DEFAULT 0,              -- 下单客户数（去重）
    product_count INT DEFAULT 0,                -- 销售商品件数

    -- 退货指标
    return_amount DECIMAL(12,2) DEFAULT 0,     -- 退货金额
    return_count INT DEFAULT 0,                 -- 退货单量
    return_customer_count INT DEFAULT 0,         -- 退货客户数（去重）

    -- 衍生比率（可选，也可在查询时计算）
    return_rate DECIMAL(5,2) GENERATED ALWAYS AS (return_count/order_count) STORED,

    etl_time DATETIME
);

CREATE TABLE dws_merchant_daily (
    stat_date DATE NOT NULL,
    merchant_id VARCHAR(50) NOT NULL,
    PRIMARY KEY (stat_date, merchant_id),

    -- 核心指标
    daily_sales DECIMAL(12,2) DEFAULT 0,
    order_count INT DEFAULT 0,
    customer_count INT DEFAULT 0,
    refund_count INT DEFAULT 0,

    -- 监控辅助字段
    sales_mom DECIMAL(5,2),                   -- 销售额环比（与昨日比）
    sales_yoy DECIMAL(5,2),                    -- 销售额同比（与上周同一天比）
    order_count_mom DECIMAL(5,2),
    refund_rate DECIMAL(5,2),                  -- 退款率
    abnormal_sales_flag BOOLEAN,               -- 是否异常高（基于历史均值+3倍标准差）
    zero_sales_flag BOOLEAN,                    -- 是否零销售额

    etl_time DATETIME
);

CREATE TABLE dws_business_daily_health (
    stat_date DATE PRIMARY KEY,

    -- 平台级KPI
    daily_sales DECIMAL(12,2),
    daily_orders INT,
    active_users INT,                         -- 日活用户（登录或下单用户）
    new_users INT,                             -- 新注册用户
    conversion_rate DECIMAL(5,2),               -- 转化率（下单用户/活跃用户）
    refund_count INT,                           -- 退款订单数
    refund_rate DECIMAL(5,2),                   -- 退款率
    avg_order_amount DECIMAL(10,2),              -- 平均订单金额

    -- 健康分计算（0-100）
    health_score INT,

    -- 预警标记（可直接用布尔值，也可用分数段标识）
    sales_drop_warning BOOLEAN,                  -- 销售额环比下跌超过20% 环比增长率 = (本期值 - 上期值) / 上期值 × 100%
    conversion_drop_warning BOOLEAN,              -- 转化率异常下降
    refund_spike_warning BOOLEAN,                  -- 退款率超过阈值

    etl_time DATETIME
);