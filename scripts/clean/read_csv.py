#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 MinIO 读取指定日期的所有 ODS 层 CSV 文件，并转换为 pandas DataFrame。
用法：python read_minio_day.py [日期，格式 YYYY-MM-DD，默认为昨天]
"""

import os
import sys
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
from minio import Minio
from minio.error import S3Error

# ================== 配置（从环境变量读取，与生成脚本保持一致）==================
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'ods-data')       # 存储 ODS 数据的桶名
MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'

def get_minio_client():
    """初始化 MinIO 客户端"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def read_csv_from_minio(client, bucket, object_name):
    """从 MinIO 读取一个 CSV 对象，返回 pandas DataFrame"""
    try:
        response = client.get_object(bucket, object_name)
        # 将响应内容读入 BytesIO，然后传给 pandas
        data = response.read()
        df = pd.read_csv(BytesIO(data), encoding='utf-8')
        return df
    except S3Error as e:
        print(f"读取 {object_name} 失败: {e}")
        return None
    finally:
        response.close()
        response.release_conn()

def main(target_date):
    """
    读取指定日期 target_date（datetime.date 对象）的所有 CSV 文件，
    返回字典 {表名: DataFrame}
    """
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"开始读取 {date_str} 的数据")

    client = get_minio_client()

    # 列出所有以日期为前缀的对象（递归）
    prefix = f"ods_/{date_str}/"   # 注意：实际对象路径为 ods_表名/日期/文件名，但前缀需灵活处理
    # 由于表名不固定，我们需要列出整个桶下所有对象，然后按日期筛选
    # 更高效的方法：列出所有对象，然后过滤路径中包含日期字符串的
    try:
        objects = client.list_objects(MINIO_BUCKET, recursive=True)
    except S3Error as e:
        print(f"无法列出桶 {MINIO_BUCKET} 中的对象: {e}")
        sys.exit(1)

    # 存储结果的字典
    dataframes = {}

    for obj in objects:
        object_name = obj.object_name
        # 检查对象名是否包含日期路径，例如 ods_orders/2026-02-27/ods_orders_2026-02-27.csv
        if f"/{date_str}/" in object_name and object_name.endswith('.csv'):
            # 提取表名：从开头到第一个斜杠，去掉前缀 "ods_"
            parts = object_name.split('/')
            if len(parts) >= 2:
                raw_table = parts[0]          # 例如 "ods_orders"
                if raw_table.startswith('ods_'):
                    table_name = raw_table[4:]  # 去掉 "ods_"，得到 "orders"
                else:
                    table_name = raw_table

                print(f"发现文件: {object_name} -> 表名: {table_name}")
                df = read_csv_from_minio(client, MINIO_BUCKET, object_name)
                if df is not None:
                    dataframes[table_name] = df
                    print(f"  加载成功，行数: {len(df)}")
                else:
                    print(f"  加载失败，跳过")

   
    return dataframes

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

    dfs = main(target)


    # 后续可在此处添加处理逻辑，如写入数据库等