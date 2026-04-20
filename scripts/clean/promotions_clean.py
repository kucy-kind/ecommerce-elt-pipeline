#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清洗 orders 数据，从 MinIO 读取指定日期的 ODS 层 CSV，清洗后写入 MySQL DWD 层。
用法：python orders_clean.py [日期 YYYY-MM-DD，默认为昨天]
"""

import os
import sys
from datetime import datetime, timedelta
from sqlalchemy.dialects.mysql import DATETIME

# 导入自定义模块
from logger import logger  # 假设您的日志模块路径正确
from read_csv import main as read_minio  # 从 read_csv.py 导入主函数
from function import (
    return_name_array,
    clean_df,
    clack,
    rename_df_columns,
    convert_specific_dict_column,
    data_export,
    fix_duplicate_primary_key,
    default_id_generator,
    resolve_pk_conflict_with_db,
    clean_foreign_key,
    standardize_datetime_columns 
)

# ================== 配置 ==================
# MySQL 连接信息（请根据实际环境调整）
MYSQL_CONN = "mysql+pymysql://root:rootpassword@mysql:3306/etl_db?charset=utf8mb4"
TARGET_TABLE = "dwd_dim_promotions"

# 需要保留的列名（与 DWD 表一致）
REQUIRED_COLUMNS = [
    'promotion_id', 'promotion_name', 'promotion_type', 'applicable_categories',
    'discount_rules', 'start_time', 'end_time', 'promotion_status','target_audience'
]

# 必须非空的列
NOT_NULL_COLUMNS = ['promotion_id', 'promotion_name', 'promotion_type',
    'discount_rules', 'start_time', 'end_time', 'promotion_status']

# 允许的枚举值
promotion_type_ALLOWED = ['Full reduction', 'Discount', 'Coupon', 'Limited-Time Sale', 'Bundle Deal']
promotion_status_ALLOWED = ['Not Started', 'In Progress', 'Completed', 'Cancelled']

# JSON 列（需要转换为 Python 对象）
JSON_COLUMNS = ['applicable_categories','discount_rules']

# 数据类型映射（可选，用于 to_sql 的 dtype 参数）
from sqlalchemy.dialects.mysql import JSON, VARCHAR, DECIMAL, ENUM, TEXT
DTYPE_MAPPING = {
    'promotion_id':VARCHAR(50),
    'promotion_name':VARCHAR(200),
    'promotion_type':ENUM('Full reduction', 'Discount', 'Coupon', 'Limited-Time Sale', 'Bundle Deal'),
    'applicable_categories': JSON,
    'discount_rules' :JSON,
    'start_time':DATETIME,
    'end_time':DATETIME,
    'promotion_status':ENUM('Not Started', 'In Progress', 'Completed', 'Cancelled'),
    'target_audience':VARCHAR(100),
}

# 日志配置
LOG_FILE = 'logs/dim_promotions_clean.log'
# 确保日志目录存在
log_dir = os.path.dirname(LOG_FILE)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)
log = logger(LOG_FILE, level='INFO')


def main(target_date):
    """清洗指定日期的 orders 数据"""
    log.info(f"开始清洗 {target_date} 的 promotions 数据")

    # 1. 从 MinIO 读取当天所有 ODS 数据
    dfs = read_minio(target_date)
    if 'dim_promotions' not in dfs:
        log.error(f"未找到 dim_promotions 表的数据，可用表: {list(dfs.keys())}")
        return False

    df_dim_promotions = dfs['dim_promotions']
    log.info(f"原始数据形状: {df_dim_promotions.shape}")

    # 2. 列名检查
    unmarked, all_cols = return_name_array(df_dim_promotions, log, set(REQUIRED_COLUMNS))
    if unmarked:
        log.warning(f"存在不需要的列: {unmarked}，将在清洗步骤中删除")
    
    #3.检查时间
    datetime_cols = ['start_time', 'end_time']
    df_dim_promotions = standardize_datetime_columns(df_dim_promotions, datetime_cols, log)

    # 3. 清洗数据：删除不需要的列、删除空行
    df_cleaned = clean_df(df_dim_promotions, REQUIRED_COLUMNS, NOT_NULL_COLUMNS, log)

    # 4. 对枚举列进行标准化和过滤
    df_cleaned = clack(df_cleaned, 'promotion_type', promotion_type_ALLOWED, log)
    df_cleaned = clack(df_cleaned, 'promotion_status', promotion_status_ALLOWED, log)
    
    for json_col in JSON_COLUMNS:
        df_cleaned = convert_specific_dict_column(df_cleaned, json_col, log)
    
    #5.csv数据内部检查主键唯一，有冲突就进行修改
    df_cleaned=fix_duplicate_primary_key(df_cleaned, 'promotion_id', log)
    
    #6.新数据和数据库内部进行主键唯一比对
    df_users = resolve_pk_conflict_with_db(
      df=df_cleaned,
      pk_column='promotion_id',
      table_name='dwd_dim_promotions',
      mysql_conn=MYSQL_CONN,
      log=log
    )

    # 7. 导出到 MySQL
    success = data_export(
        df=df_cleaned,
        mysql_conn=MYSQL_CONN,
        json_columns=JSON_COLUMNS,
        dtype=DTYPE_MAPPING,
        log=log,
        t=TARGET_TABLE
    )

    if success:
        log.info("dim_promotions 数据清洗并导入完成")
    else:
        log.error("dim_promotions 数据清洗失败")
    return success


if __name__ == '__main__':
    # 解析日期参数
    if len(sys.argv) > 1:
        try:
            target = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
        except ValueError:
            print("日期格式错误，应为 YYYY-MM-DD")
            sys.exit(1)
    else:
        target = (datetime.now() - timedelta(days=1)).date()

    success = main(target)
    sys.exit(0 if success else 1)