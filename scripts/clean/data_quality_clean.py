#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime, timedelta
import pandas as pd

# 导入自定义模块
from logger import logger
from read_csv import main as read_minio
from function import (
    return_name_array,
    clean_df,
    clack,
    data_export,
    standardize_datetime_columns,
    normalize_boolean_column,
)

# ================== 配置 ==================
MYSQL_CONN = "mysql+pymysql://root:rootpassword@mysql:3306/etl_db?charset=utf8mb4"
TARGET_TABLE = "dwd_data_quality_alerts"

# 需要保留的列名（alert_id 自增，created_time 由数据库默认，不包含）
REQUIRED_COLUMNS = [
    'check_type',
    'problem_description',
    'problem_count',
    'severity',
    'check_time',
    'data_date',
    'resolved',
    'resolution_notes',
    'resolved_time'
]

# 必须非空的列（根据表定义）
NOT_NULL_COLUMNS = [
    'check_type',
    'problem_description',
    'problem_count',
    'check_time',
    'data_date'
]

# 允许的枚举值（severity 允许 None，数据库将使用默认值 'medium'）
severity_ALLOWED = ['low', 'medium', 'high', 'critical', None]

# JSON 列（无）
JSON_COLUMNS = []

# 数据类型映射
from sqlalchemy.dialects.mysql import (
    VARCHAR, TEXT, INTEGER, ENUM, DATETIME, DATE, BOOLEAN
)

DTYPE_MAPPING = {
    'check_type': VARCHAR(50),
    'problem_description': TEXT,
    'problem_count': INTEGER,
    'severity': ENUM('low', 'medium', 'high', 'critical'),
    'check_time': DATETIME,
    'data_date': DATE,
    'resolved': BOOLEAN,
    'resolution_notes': TEXT,
    'resolved_time': DATETIME
}

# 日志配置
LOG_FILE = 'logs/data_quality_alerts_clean.log'
log_dir = os.path.dirname(LOG_FILE)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)
log = logger(LOG_FILE, level='INFO')


def main(target_date):
    """清洗指定日期的 data_quality_alerts 数据"""
    log.info(f"开始清洗 {target_date} 的 data_quality_alerts 数据")

    # 1. 从 MinIO 读取当天所有 ODS 数据
    dfs = read_minio(target_date)
    if 'data_quality_alerts' not in dfs:
        log.error(f"未找到 data_quality_alerts 表的数据，可用表: {list(dfs.keys())}")
        return False

    df_alerts = dfs['data_quality_alerts']
    log.info(f"原始数据形状: {df_alerts.shape}")

    # 2. 列名检查
    unmarked, all_cols = return_name_array(df_alerts, log, set(REQUIRED_COLUMNS))
    if unmarked:
        log.warning(f"存在不需要的列: {unmarked}，将在清洗步骤中删除")

    # 3. 基础清洗：删除不需要的列，删除非空列中为空的行
    df_cleaned = clean_df(df_alerts, REQUIRED_COLUMNS, NOT_NULL_COLUMNS, log)
    if df_cleaned.empty:
        log.warning("基础清洗后无有效数据，跳过")
        return False

    # 4. 标准化日期时间列（check_time 必须非空，resolved_time 允许空）
    datetime_cols = ['check_time', 'resolved_time']
    not_null_dt = ['check_time']
    df_cleaned = standardize_datetime_columns(df_cleaned, datetime_cols, log, not_null_cols=not_null_dt)
    if df_cleaned.empty:
        log.warning("日期时间标准化后无有效数据，跳过")
        return False

    # 5. 处理 data_date 列（DATE 类型）
    if 'data_date' in df_cleaned.columns:
        def parse_date(val):
            if pd.isna(val) or str(val).strip() in ['', 'null', 'None']:
                return None
            try:
                dt = pd.to_datetime(val, errors='coerce')
                if pd.isna(dt):
                    return None
                return dt.strftime('%Y-%m-%d')
            except:
                return None
        df_cleaned['data_date'] = df_cleaned['data_date'].apply(parse_date)
        # 删除 data_date 为空的行（必须非空）
        before = len(df_cleaned)
        df_cleaned = df_cleaned.dropna(subset=['data_date'])
        after = len(df_cleaned)
        if before > after:
            log.info(f"data_date 列删除了 {before - after} 行（无效日期）")
    else:
        log.error("data_date 列缺失，无法继续")
        return False

    if df_cleaned.empty:
        log.warning("data_date 处理后无有效数据，跳过")
        return False

    # 6. 对枚举列进行标准化和过滤（severity）
    df_cleaned = clack(df_cleaned, 'severity', severity_ALLOWED, log)

    # 7. 标准化布尔字段 resolved
    df_cleaned = normalize_boolean_column(df_cleaned, 'resolved', log, default_false=True)

    # 8. 注意：alert_id 自增，无需主键处理

    # 9. 导出到 MySQL
    success = data_export(
        df=df_cleaned,
        mysql_conn=MYSQL_CONN,
        json_columns=JSON_COLUMNS,
        dtype=DTYPE_MAPPING,
        log=log,
        t=TARGET_TABLE
    )

    if success:
        log.info("data_quality_alerts 数据清洗并导入完成")
    else:
        log.error("data_quality_alerts 数据清洗失败")
    return success


if __name__ == '__main__':
    if len(sys.argv) > 1:
        target = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    else:
        target = (datetime.now() - timedelta(days=1)).date()
    success = main(target)
    sys.exit(0 if success else 1)