#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime, timedelta

# 导入自定义模块
from logger import logger
from read_csv import main as read_minio
from function import (
    return_name_array,
    clean_df,
    clack,
    data_export,
    clean_foreign_key,
    standardize_datetime_columns,
    normalize_boolean_column,   # 新增导入
)

# ================== 配置 ==================
MYSQL_CONN = "mysql+pymysql://root:rootpassword@mysql:3306/etl_db?charset=utf8mb4"
TARGET_TABLE = "dwd_user_actions"

# 需要保留的列名（注意：action_id 自增，created_time 由数据库默认，均不包含）
REQUIRED_COLUMNS = [
    'user_id',
    'session_id',
    'action_type',
    'action_time',
    'product_id',
    'stay_duration_seconds',
    'device_type',
    'is_abnormal',
    'abnormal_reason'
]

# 必须非空的列
NOT_NULL_COLUMNS = ['user_id', 'action_type', 'action_time']

# 允许的枚举值
action_type_ALLOWED = [
    'register', 'login', 'browse', 'favorite',
    'add_cart', 'purchase', 'search', 'logout'
]

# JSON 列（无）
JSON_COLUMNS = []

# 数据类型映射
from sqlalchemy.dialects.mysql import (
    VARCHAR, ENUM, DATETIME, INTEGER, BOOLEAN, TEXT
)

DTYPE_MAPPING = {
    'user_id': VARCHAR(50),
    'session_id': VARCHAR(100),
    'action_type': ENUM('register', 'login', 'browse', 'favorite',
                        'add_cart', 'purchase', 'search', 'logout'),
    'action_time': DATETIME,
    'product_id': VARCHAR(50),
    'stay_duration_seconds': INTEGER,
    'device_type': VARCHAR(50),
    'is_abnormal': BOOLEAN,
    'abnormal_reason': VARCHAR(200)
}

# 日志配置
LOG_FILE = 'logs/user_actions_clean.log'
log_dir = os.path.dirname(LOG_FILE)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)
log = logger(LOG_FILE, level='INFO')


def main(target_date):
    log.info(f"开始清洗 {target_date} 的 user_actions 数据")

    # 1. 读取 MinIO 数据
    dfs = read_minio(target_date)
    if 'user_actions' not in dfs:
        log.error(f"未找到 user_actions 表的数据，可用表: {list(dfs.keys())}")
        return False

    df_actions = dfs['user_actions']
    log.info(f"原始数据形状: {df_actions.shape}")

    # 2. 列名检查
    unmarked, all_cols = return_name_array(df_actions, log, set(REQUIRED_COLUMNS))
    if unmarked:
        log.warning(f"存在不需要的列: {unmarked}，将在清洗步骤中删除")

    # 3. 基础清洗
    df_cleaned = clean_df(df_actions, REQUIRED_COLUMNS, NOT_NULL_COLUMNS, log)
    if df_cleaned.empty:
        log.warning("基础清洗后无有效数据，跳过")
        return False

    # 4. 标准化日期时间列
    datetime_cols = ['action_time']
    not_null_dt = ['action_time']
    df_cleaned = standardize_datetime_columns(df_cleaned, datetime_cols, log, not_null_cols=not_null_dt)
    if df_cleaned.empty:
        log.warning("日期标准化后无有效数据，跳过")
        return False

    # 5. 对枚举列进行过滤
    df_cleaned = clack(df_cleaned, 'action_type', action_type_ALLOWED, log)

    # 6. 标准化布尔字段（调用外部函数）
    if 'is_abnormal' in df_cleaned.columns:
        df_cleaned = normalize_boolean_column(df_cleaned, 'is_abnormal', log, default_false=True)

    # 7. 外键检查
    df_cleaned = clean_foreign_key(
        df=df_cleaned,
        fk_column='user_id',
        ref_table='dwd_users',
        ref_column='user_id',
        mysql_conn=MYSQL_CONN,
        log=log
    )
    if df_cleaned.empty:
        log.warning("user_id 外键检查后无有效数据，跳过")
        return False

    df_cleaned = clean_foreign_key(
        df=df_cleaned,
        fk_column='product_id',
        ref_table='dwd_dim_products',
        ref_column='product_id',
        mysql_conn=MYSQL_CONN,
        log=log
    )

    # 8. 导出到 MySQL
    if df_cleaned.empty:
        log.warning("清洗后无有效数据，跳过导出")
        return False

    success = data_export(
        df=df_cleaned,
        mysql_conn=MYSQL_CONN,
        json_columns=JSON_COLUMNS,
        dtype=DTYPE_MAPPING,
        log=log,
        t=TARGET_TABLE
    )

    if success:
        log.info("user_actions 数据清洗并导入完成")
    else:
        log.error("user_actions 数据清洗失败")
    return success


if __name__ == '__main__':
    if len(sys.argv) > 1:
        target = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    else:
        target = (datetime.now() - timedelta(days=1)).date()
    success = main(target)
    sys.exit(0 if success else 1)