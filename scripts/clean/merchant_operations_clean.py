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
)

# ================== 配置 ==================
MYSQL_CONN = "mysql+pymysql://root:rootpassword@mysql:3306/etl_db?charset=utf8mb4"
TARGET_TABLE = "dwd_merchant_operations"

# 需要保留的列名（operation_id 自增，created_time/updated_time 由数据库默认，不包含）
REQUIRED_COLUMNS = [
    'merchant_id',
    'operation_type',
    'new_place',
    'product_id',
    'operation_time'
]

# 必须非空的列（根据表定义）
NOT_NULL_COLUMNS = [
    'merchant_id',
    'operation_type',
    'product_id',
    'operation_time'
]

# 允许的枚举值
operation_type_ALLOWED = [
    'add_product', 'remove_product', 'update_price', 'update_stock'
]

# JSON 列（无）
JSON_COLUMNS = []

# 数据类型映射（用于 to_sql 的 dtype 参数）
from sqlalchemy.dialects.mysql import (
    VARCHAR, DECIMAL, ENUM, DATETIME
)

DTYPE_MAPPING = {
    'merchant_id': VARCHAR(50),
    'operation_type': ENUM('add_product', 'remove_product', 'update_price', 'update_stock'),
    'new_place': DECIMAL(10, 2),
    'product_id': VARCHAR(50),
    'operation_time': DATETIME
}

# 日志配置
LOG_FILE = 'logs/merchant_operations_clean.log'
log_dir = os.path.dirname(LOG_FILE)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)
log = logger(LOG_FILE, level='INFO')


def main(target_date):
    """清洗指定日期的 merchant_operations 数据"""
    log.info(f"开始清洗 {target_date} 的 merchant_operations 数据")

    # 1. 从 MinIO 读取当天所有 ODS 数据
    dfs = read_minio(target_date)
    if 'merchant_operations' not in dfs:
        log.error(f"未找到 merchant_operations 表的数据，可用表: {list(dfs.keys())}")
        return False

    df_ops = dfs['merchant_operations']
    log.info(f"原始数据形状: {df_ops.shape}")

    # 2. 列名检查
    unmarked, all_cols = return_name_array(df_ops, log, set(REQUIRED_COLUMNS))
    if unmarked:
        log.warning(f"存在不需要的列: {unmarked}，将在清洗步骤中删除")

    # 3. 基础清洗：删除不需要的列，删除非空列中为空的行
    df_cleaned = clean_df(df_ops, REQUIRED_COLUMNS, NOT_NULL_COLUMNS, log)
    if df_cleaned.empty:
        log.warning("基础清洗后无有效数据，跳过")
        return False

    # 4. 标准化日期时间列（operation_time 必须非空）
    datetime_cols = ['operation_time']
    not_null_dt = ['operation_time']
    df_cleaned = standardize_datetime_columns(df_cleaned, datetime_cols, log, not_null_cols=not_null_dt)
    if df_cleaned.empty:
        log.warning("日期标准化后无有效数据，跳过")
        return False

    # 5. 对枚举列进行标准化和过滤
    df_cleaned = clack(df_cleaned, 'operation_type', operation_type_ALLOWED, log)

    # 6. 外键检查（均不允许为空，使用不带 allow_null 的 clean_foreign_key）
    df_cleaned = clean_foreign_key(
        df=df_cleaned,
        fk_column='merchant_id',
        ref_table='dwd_dim_merchants',
        ref_column='merchant_id',
        mysql_conn=MYSQL_CONN,
        log=log
    )
    if df_cleaned.empty:
        log.warning("merchant_id 外键检查后无有效数据，跳过")
        return False

    df_cleaned = clean_foreign_key(
        df=df_cleaned,
        fk_column='product_id',
        ref_table='dwd_dim_products',
        ref_column='product_id',
        mysql_conn=MYSQL_CONN,
        log=log
    )
    if df_cleaned.empty:
        log.warning("product_id 外键检查后无有效数据，跳过")
        return False

    # 7. 注意：operation_id 自增，无需主键处理

    # 8. 导出到 MySQL
    success = data_export(
        df=df_cleaned,
        mysql_conn=MYSQL_CONN,
        json_columns=JSON_COLUMNS,
        dtype=DTYPE_MAPPING,
        log=log,
        t=TARGET_TABLE
    )

    if success:
        log.info("merchant_operations 数据清洗并导入完成")
    else:
        log.error("merchant_operations 数据清洗失败")
    return success


if __name__ == '__main__':
    if len(sys.argv) > 1:
        target = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    else:
        target = (datetime.now() - timedelta(days=1)).date()
    success = main(target)
    sys.exit(0 if success else 1)