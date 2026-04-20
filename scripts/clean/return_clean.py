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
    fix_duplicate_primary_key,
    resolve_pk_conflict_with_db,
    clean_foreign_key,
    standardize_datetime_columns,
    # 注意：退货表没有 JSON 列，所以无需 convert_specific_dict_column
)

# ================== 配置 ==================
MYSQL_CONN = "mysql+pymysql://root:rootpassword@mysql:3306/etl_db?charset=utf8mb4"
TARGET_TABLE = "dwd_returns"

# 需要保留的列名（与 DWD 表一致，不包含自增或默认字段）
REQUIRED_COLUMNS = [
    'return_id',
    'order_id',
    'user_id',
    'merchant_id',
    'return_reason_category',
    'return_reason_detail',
    'return_status',
    'apply_time',
    'approved_time',
    'rejected_time',
    'shipped_back_time',
    'received_time',
    'refund_time',
    'refund_amount',
    'return_quantity',
    'data_consistency_flag'
]

# 必须非空的列（根据表定义）
NOT_NULL_COLUMNS = [
    'return_id',
    'order_id',
    'user_id',
    'merchant_id',
    'return_reason_category',
    'return_status',
    'apply_time',
    'return_quantity'
]

# 允许的枚举值
return_reason_category_ALLOWED = [
    'quality_issue', 'wrong_item', 'damaged',
    'size_issue', 'not_as_described', 'other'
]
return_status_ALLOWED = [
    'applied', 'approved', 'rejected',
    'shipped_back', 'received', 'refunded'
]
data_consistency_flag_ALLOWED = ['consistent', 'inconsistent', None]  # 允许空值

# JSON 列（无）
JSON_COLUMNS = []

# 数据类型映射（用于 to_sql 的 dtype 参数）
from sqlalchemy.dialects.mysql import (
    VARCHAR, BIGINT, DECIMAL, ENUM, TEXT, DATETIME, INTEGER
)

DTYPE_MAPPING = {
    'return_id': VARCHAR(50),
    'order_id': BIGINT,
    'user_id': VARCHAR(50),
    'merchant_id': VARCHAR(50),
    'return_reason_category': ENUM('quality_issue', 'wrong_item', 'damaged',
                                   'size_issue', 'not_as_described', 'other'),
    'return_reason_detail': TEXT,
    'return_status': ENUM('applied', 'approved', 'rejected',
                          'shipped_back', 'received', 'refunded'),
    'apply_time': DATETIME,
    'approved_time': DATETIME,
    'rejected_time': DATETIME,
    'shipped_back_time': DATETIME,
    'received_time': DATETIME,
    'refund_time': DATETIME,
    'refund_amount': DECIMAL(10, 2),
    'return_quantity': INTEGER,
    'data_consistency_flag': ENUM('consistent', 'inconsistent')
}

# 日志配置
LOG_FILE = 'logs/returns_clean.log'
log_dir = os.path.dirname(LOG_FILE)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)
log = logger(LOG_FILE, level='INFO')


def main(target_date):
    """清洗指定日期的 returns 数据"""
    log.info(f"开始清洗 {target_date} 的 returns 数据")

    # 1. 从 MinIO 读取当天所有 ODS 数据
    dfs = read_minio(target_date)
    if 'returns' not in dfs:
        log.error(f"未找到 returns 表的数据，可用表: {list(dfs.keys())}")
        return False

    df_returns = dfs['returns']
    log.info(f"原始数据形状: {df_returns.shape}")

    # 2. 列名检查
    unmarked, all_cols = return_name_array(df_returns, log, set(REQUIRED_COLUMNS))
    if unmarked:
        log.warning(f"存在不需要的列: {unmarked}，将在清洗步骤中删除")

    # 3. 基础清洗：删除不需要的列，删除非空列中为空的行
    df_cleaned = clean_df(df_returns, REQUIRED_COLUMNS, NOT_NULL_COLUMNS, log)
    if df_cleaned.empty:
        log.warning("基础清洗后无有效数据，跳过")
        return False

    # 4. 标准化日期时间列（apply_time 必须非空，其他允许空）
    datetime_cols = [
        'apply_time', 'approved_time', 'rejected_time',
        'shipped_back_time', 'received_time', 'refund_time'
    ]
    not_null_dt = ['apply_time']   # 仅 apply_time 不能为空
    df_cleaned = standardize_datetime_columns(df_cleaned, datetime_cols, log, not_null_cols=not_null_dt)
    if df_cleaned.empty:
        log.warning("日期标准化后无有效数据，跳过")
        return False

    # 5. 对枚举列进行标准化和过滤
    df_cleaned = clack(df_cleaned, 'return_reason_category', return_reason_category_ALLOWED, log)
    df_cleaned = clack(df_cleaned, 'return_status', return_status_ALLOWED, log)
    df_cleaned = clack(df_cleaned, 'data_consistency_flag', data_consistency_flag_ALLOWED, log)

    # 6. 外键检查（所有外键均不允许为空，使用不带 allow_null 的 clean_foreign_key）
    # 注意：clean_foreign_key 默认行为是删除不在参考表中的行
    df_cleaned = clean_foreign_key(
        df=df_cleaned,
        fk_column='order_id',
        ref_table='dwd_orders',
        ref_column='order_id',
        mysql_conn=MYSQL_CONN,
        log=log
    )
    if df_cleaned.empty:
        log.warning("order_id 外键检查后无有效数据，跳过")
        return False

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
        fk_column='merchant_id',
        ref_table='dwd_dim_merchants',
        ref_column='merchant_id',
        mysql_conn=MYSQL_CONN,
        log=log
    )
    if df_cleaned.empty:
        log.warning("merchant_id 外键检查后无有效数据，跳过")
        return False

    # 7. 处理主键（return_id）
    # 内部重复值处理
    df_cleaned = fix_duplicate_primary_key(df_cleaned, 'return_id', log)
    # 与数据库已存在主键冲突处理
    df_cleaned = resolve_pk_conflict_with_db(
        df=df_cleaned,
        pk_column='return_id',
        table_name='dwd_returns',
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
        log.info("returns 数据清洗并导入完成")
    else:
        log.error("returns 数据清洗失败")
    return success


if __name__ == '__main__':
    if len(sys.argv) > 1:
        target = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    else:
        target = (datetime.now() - timedelta(days=1)).date()
    success = main(target)
    sys.exit(0 if success else 1)