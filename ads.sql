SET NAMES utf8mb4;

-- 用户月度快照表：每个用户每月一条记录
CREATE TABLE IF NOT EXISTS ads_user_monthly_snapshot (-- 综合dws的user_profile
    stat_month DATE NOT NULL COMMENT '统计月份，格式YYYY-MM-01', -- 这个月份从airflow中给出，也就是判断如果是一个月的第一天就会自动运行
    user_id VARCHAR(50) NOT NULL COMMENT '用户ID',
    user_name VARCHAR(50) COMMENT '用户名',
    register_date DATE COMMENT '注册日期',
    user_level VARCHAR(20) COMMENT '当月用户等级（普通/银卡/金卡等）',-- dws的user——level可以给出，统计这个月的平均值，然后给出这个等级金卡>80 银卡>60 普通<=60
    total_order_amount DECIMAL(10,2) COMMENT '截至当月累计消费金额',-- next
    total_order_count INT COMMENT '截至当月累计订单数',-- next
    total_return_count INT COMMENT '截至当月累计退货次数',-- next
    month_login_days INT COMMENT '当月登录天数',
    month_view_cnt INT COMMENT '当月浏览商品数',
    month_order_cnt INT COMMENT '当月下单次数',
    month_order_amount DECIMAL(10,2) COMMENT '当月消费金额',
    month_return_cnt INT COMMENT '当月退货次数',
    month_avg_order_amount DECIMAL(10,2) COMMENT '当月平均客单价',-- 要计算 amount/order_cnt
    preferred_category VARCHAR(50) COMMENT '当月偏好品类',-- 直接copy喜好类别就可以
    lifecycle_stage VARCHAR(20) COMMENT '当月生命周期阶段（新客/活跃/沉睡/流失）', -- 如果是当月新注册的用户那么就是新客--如果用户等级在银卡之上那么就直接是活跃用户--如果这个月订单数量减少70%那么沉睡--无订单/流失
    risk_flag VARCHAR(20) COMMENT '当月风险标记（正常/异常退货/刷单嫌疑）',-- 退货超过70%，且退货数量超过50件为异常退货 --一天内对同一店铺下单超过10单，一个月累计50单为刷单嫌疑
    etl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL更新时间',
    PRIMARY KEY (stat_month, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户月度快照表';

-- 用户年度快照表：每个用户每年一条记录
CREATE TABLE IF NOT EXISTS ads_user_yearly_snapshot (
    stat_year YEAR NOT NULL COMMENT '统计年份',-- 判断Airflow传入的日期是yyyy—01-01
    user_id VARCHAR(50) NOT NULL COMMENT '用户ID',
    user_name VARCHAR(50) COMMENT '用户名',
    register_date DATE COMMENT '注册日期',
    user_level VARCHAR(20) COMMENT '年末用户等级',-- 这里如果一年有超过8个月都是优质，其他月份不允许是中潜质/普通 --年度优质用户
    -- 超过8个月都是高潜质/优质 其他月份不允许是普通用户 --年度高潜质用户
    -- 超过8个月都是中潜质用户/高潜质/优质 -- 年度潜质用户 --都不符合 普通用户
    total_order_amount DECIMAL(10,2) COMMENT '截至年末累计消费金额',-- 直接使用ads_user_monthly_snapshot去年1201加上今年1月数据
    total_order_count INT COMMENT '截至年末累计订单数',
    total_return_count INT COMMENT '截至年末累计退货次数',
    year_login_days INT COMMENT '当年登录天数',-- 累计ads_user_monthly_snapshot一年中的每个月累加
    year_view_cnt INT COMMENT '当年浏览商品数',-- 累计ads_user_monthly_snapshot一年中的每个月累加
    year_order_cnt INT COMMENT '当年下单次数',-- 累计ads_user_monthly_snapshot一年中的每个月累加
    year_order_amount DECIMAL(10,2) COMMENT '当年消费金额',-- 累计ads_user_monthly_snapshot一年中的每个月累加
    year_return_cnt INT COMMENT '当年退货次数',-- 累计ads_user_monthly_snapshot一年中的每个月累加
    year_avg_order_amount DECIMAL(10,2) COMMENT '当年平均客单价',-- year_order_amount/year_order_cnt
    preferred_category VARCHAR(50) COMMENT '当年偏好品类',-- 累计ads_user_monthly_snapshot中的类别找出最高的5款没超过5款则全部写出
    lifecycle_stage VARCHAR(20) COMMENT '年末生命周期阶段',-- 沉睡主要是对前三个月的数据进行评定，活跃还是一致，流失就是超三月任何订单
    risk_flag VARCHAR(20) COMMENT '当年风险标记',-- 月风险标记超过三次，假如第一年被标记，第二年月风险超过一次则标记，第三年直接标记风险
    etl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL更新时间',
    PRIMARY KEY (stat_year, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户年度快照表';

-- =====================================================
-- 商家表
-- =====================================================

-- 商家每日快照表：每个商家每天一条记录
CREATE TABLE IF NOT EXISTS ads_merchant_daily_snapshot (-- dws_merchant_daily 完全一致
    stat_date DATE NOT NULL COMMENT '统计日期',
    merchant_id VARCHAR(50) NOT NULL COMMENT '商家ID',
    merchant_name VARCHAR(100) COMMENT '商家名称',
    merchant_level VARCHAR(20) COMMENT '商家等级',
    daily_sales DECIMAL(10,2) COMMENT '当日销售额',
    order_count INT COMMENT '当日订单数',
    customer_count INT COMMENT '当日客户数',
    refund_count INT COMMENT '当日退货数',
    refund_rate DECIMAL(5,2) COMMENT '当日退货率（退货数/订单数）',
    sales_mom DECIMAL(5,2) COMMENT '销售额环比（与昨日相比百分比）',
    zero_sales_flag BOOLEAN COMMENT '零销售额标记（TRUE表示当日无销售）',
    abnormal_sales_flag BOOLEAN COMMENT '异常销售额标记（TRUE表示超出正常波动范围）',
    etl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL更新时间',
    PRIMARY KEY (stat_date, merchant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商家每日快照表';

-- 商家月度快照表：每个商家每月一条记录
CREATE TABLE IF NOT EXISTS ads_merchant_monthly_snapshot (-- 使用dws_merchant_profile(增加了stat_date)
    stat_month DATE NOT NULL COMMENT '统计月份，格式YYYY-MM-01',
    merchant_id VARCHAR(50) NOT NULL COMMENT '商家ID',
    merchant_name VARCHAR(100) COMMENT '商家名称',
    merchant_level VARCHAR(20) COMMENT '商家等级',
    total_sales DECIMAL(10,2) COMMENT '当月累计销售额',
    order_count INT COMMENT '当月订单数',
    customer_count INT COMMENT '当月客户数',
    refund_count INT COMMENT '当月退货数',
    refund_rate DECIMAL(5,2) COMMENT '当月退货率',
    top_products JSON COMMENT '热销商品TOP5（JSON数组，包含商品ID、名称、销量）',-- 需要从dwd层导入
    sales_trend VARCHAR(20) COMMENT '销售趋势（上升/平稳/下降）',-- 上升：比例超过20%，下降：比例超过20%，和上个月的进行对比，从上个月的进行获取
    etl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL更新时间',
    PRIMARY KEY (stat_month, merchant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商家月度快照表';

-- 商家年度快照表：每个商家每年一条记录
CREATE TABLE IF NOT EXISTS ads_merchant_yearly_snapshot (
    stat_year YEAR NOT NULL COMMENT '统计年份',
    merchant_id VARCHAR(50) NOT NULL COMMENT '商家ID',
    merchant_name VARCHAR(100) COMMENT '商家名称',
    merchant_level VARCHAR(20) COMMENT '商家等级',
    total_sales DECIMAL(10,2) COMMENT '全年销售额',
    order_count INT COMMENT '全年订单数',
    customer_count INT COMMENT '全年客户数',
    refund_count INT COMMENT '全年退货数',
    refund_rate DECIMAL(5,2) COMMENT '全年退货率',
    yoy_sales_growth DECIMAL(5,2) COMMENT '销售额同比增长率（与去年相比百分比）',
    etl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL更新时间',
    PRIMARY KEY (stat_year, merchant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商家年度快照表';

-- =====================================================
-- 平台表
-- =====================================================

-- 平台每日健康快照：单行记录
CREATE TABLE IF NOT EXISTS ads_platform_daily_snapshot (
    stat_date DATE NOT NULL COMMENT '统计日期',
    daily_sales DECIMAL(12,2) COMMENT '当日GMV',
    daily_orders INT COMMENT '当日订单数',
    active_users INT COMMENT '当日活跃用户数（有任一行为的用户）',
    new_users INT COMMENT '当日新注册用户数',
    conversion_rate DECIMAL(5,2) COMMENT '当日转化率（下单用户数/活跃用户数）',
    refund_count INT COMMENT '当日退款数',
    refund_rate DECIMAL(5,2) COMMENT '当日退款率（退款订单数/总订单数）',
    health_score INT COMMENT '当日健康分（0-100，综合指标）',
    sales_drop_warning BOOLEAN COMMENT '销售额骤降预警（TRUE表示异常下降）',
    conversion_drop_warning BOOLEAN COMMENT '转化率骤降预警',
    refund_spike_warning BOOLEAN COMMENT '退款率飙升预警',
    etl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL更新时间',
    PRIMARY KEY (stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='平台每日健康快照表';

-- 平台月度汇总表：单行记录
CREATE TABLE IF NOT EXISTS ads_platform_monthly_summary (
    stat_month DATE NOT NULL COMMENT '统计月份，格式YYYY-MM-01',-- y
    total_gmv DECIMAL(12,2) COMMENT '当月GMV',-- sum-- y
    total_orders INT COMMENT '当月订单数',-- sum --y
    active_merchants INT COMMENT '当月有交易商家数',-- sum ads_merchant_monthly order不是0的数量
    category_sales JSON COMMENT '品类销售分布（JSON对象，如{"手机":500000,"服装":300000}）',-- 要从原始表里面获取
    promotion_effect JSON COMMENT '活动效果分析（JSON对象，活动ID -> 带来GMV增量）',-- 注意是已经开始的活动/那么status为in progress 那么就是在这个时间段内，把订单分成两组，一组是参加了这个活动的订单A，一组是没有参加这个活动的订单B,
    -- 再根据user_level把订单分成比如说A.普通用户 B.普通用户 然后使用A.普通用户.amount/B.普通用户.amount来观察活动效果
    etl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL更新时间',
    PRIMARY KEY (stat_month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='平台月度汇总表';

-- 平台年度汇总表：单行记录
CREATE TABLE IF NOT EXISTS ads_platform_yearly_summary (
    stat_year YEAR NOT NULL COMMENT '统计年份',-- y
    total_gmv DECIMAL(12,2) COMMENT '全年GMV',-- y
    total_orders INT COMMENT '全年订单数',-- y
    active_merchants INT COMMENT '全年有交易商家数',-- a
    category_sales JSON COMMENT '品类销售分布（JSON对象）',-- c
    user_retention_rate DECIMAL(5,2) COMMENT '全年用户留存率，年初用户，年后依旧在购买',
    -- new_user_stay_rate DECIMAL(5,2) COMMENT '新用户留存率，这里统计去年9月份开始，到今年9月份，即超过三个月',
    etl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL更新时间',
    PRIMARY KEY (stat_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='平台年度汇总表';