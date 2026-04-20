#!/usr/bin/env python3
# -*- coding: utf-8 -*-


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
TARGET_TABLE = "dwd_orders"

# 需要保留的列名（与 DWD 表一致）
REQUIRED_COLUMNS = [
    'order_id', 'user_id', 'product_id', 'order_amount',
    'order_status', 'shipping_address','promotion_id',
    'coupon_info','data_quality_flag','order_time',
    'payment_time','shipping_time','complete_time'
]

# 必须非空的列
NOT_NULL_COLUMNS = ['order_id', 'user_id', 'product_id', 'order_amount',
    'order_status', 'shipping_address',
    'coupon_info','data_quality_flag','order_time',
    'payment_time','shipping_time','complete_time'
    ]

# 允许的枚举值
order_status_ALLOWED = ['pending', 'paid', 'shipped', 'completed', 'cancelled', 'refunded']
data_quality_flag_ALLOWED = ['valid', 'invalid', 'suspicious',None]  # 允许空值

# JSON 列（需要转换为 Python 对象）
JSON_COLUMNS = ['coupon_info']

from sqlalchemy.dialects.mysql import JSON, VARCHAR, DECIMAL, ENUM, TEXT, BIGINT, DATETIME

DTYPE_MAPPING = {
    'order_id':BIGINT,
    'user_id':VARCHAR(50),
    'product_id':VARCHAR(50),
    'order_amount':DECIMAL(10,2),
    'order_status':ENUM('pending', 'paid', 'shipped', 'completed', 'cancelled', 'refunded'),
    'shipping_address':TEXT,
    'promotion_id':VARCHAR(50),
    'coupon_info':JSON,
    'data_quality_flag':ENUM('valid', 'invalid', 'suspicious'),
    'order_time':DATETIME,       
    'payment_time':DATETIME,                 
    'shipping_time':DATETIME,                 
    'complete_time':DATETIME

}

# 日志配置
LOG_FILE = 'logs/orders_clean.log'
# 确保日志目录存在
log_dir = os.path.dirname(LOG_FILE)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)
log = logger(LOG_FILE, level='INFO')


def main(target_date):
    """清洗指定日期的 orders 数据"""
    log.info(f"开始清洗 {target_date} 的 orders 数据")

    #1. 从 MinIO 读取当天所有 ODS 数据
    dfs = read_minio(target_date)
    if 'orders' not in dfs:
        log.error(f"未找到 orders 表的数据，可用表: {list(dfs.keys())}")
        return False

    df_orders = dfs['orders']
    log.info(f"原始数据形状: {df_orders.shape}")

    #2. 列名检查
    unmarked, all_cols = return_name_array(df_orders, log, set(REQUIRED_COLUMNS))
    if unmarked:
        log.warning(f"存在不需要的列: {unmarked}，将在清洗步骤中删除")

    #3. 清洗数据：删除不需要的列、删除空行
    df_cleaned = clean_df(df_orders, REQUIRED_COLUMNS, NOT_NULL_COLUMNS, log)

    #4.检查时间
    datetime_cols = ['order_time','payment_time','shipping_time','complete_time']
    not_null=['order_time']
    df_cleaned = standardize_datetime_columns(df_cleaned, datetime_cols, log, not_null_cols=not_null)
    
    #5. 对枚举列进行标准化和过滤
    df_cleaned = clack(df_cleaned, 'order_status', order_status_ALLOWED, log)
    df_cleaned = clack(df_cleaned, 'data_quality_flag',data_quality_flag_ALLOWED, log)
    
    #6.外键检查
    df_cleaned = clean_foreign_key(
      df=df_cleaned,
      fk_column='user_id',
      ref_table='dwd_users',      # 参考表名
      ref_column='user_id',           # 参考列名
      mysql_conn=MYSQL_CONN,
      log=log
      )
    df_cleaned = clean_foreign_key(
      df=df_cleaned,
      fk_column='product_id',
      ref_table='dwd_dim_products',      # 参考表名
      ref_column='product_id',           # 参考列名
      mysql_conn=MYSQL_CONN,
      log=log
      )
    df_cleaned = clean_foreign_key(
      df=df_cleaned,
      fk_column='promotion_id',
      ref_table='dwd_dim_promotions',      # 参考表名
      ref_column='promotion_id',           # 参考列名
      mysql_conn=MYSQL_CONN,
      log=log
      )
    if df_cleaned.empty:
        log.warning("清洗后无有效数据，跳过导出")
        return False

    #7.csv数据内部检查主键唯一，有冲突就进行修改
    df_cleaned=fix_duplicate_primary_key(df_cleaned, 'order_id', log)
    
    #8.新数据和数据库内部进行主键唯一比对
    df_users = resolve_pk_conflict_with_db(
      df=df_cleaned,
      pk_column='order_id',
      table_name='dwd_orders',
      mysql_conn=MYSQL_CONN,
      log=log
    )
    #9. 导出到 MySQL
    success = data_export(
        df=df_cleaned,
        mysql_conn=MYSQL_CONN,
        json_columns=JSON_COLUMNS,
        dtype=DTYPE_MAPPING,
        log=log,
        t=TARGET_TABLE
    )

    if success:
        log.info("orders 数据清洗并导入完成")
    else:
        log.error("orders 数据清洗失败")
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