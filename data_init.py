#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性初始化维度表数据。
运行前请确保MySQL已启动且表结构已创建。
建议在 etl_python 容器内执行，或通过 Airflow 的一次性 DAG 运行。
"""

import os
import random
from datetime import datetime, timedelta
import pymysql
from faker import Faker

# MySQL 连接配置（与 docker-compose 环境变量保持一致）
MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'rootpassword')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'etl_db')

fake = Faker('zh_CN')
random.seed(42)  # 固定种子，保证每次生成相同数据（可选）

# 商家ID列表（5个）
MERCHANT_IDS = [f'MERC{i:03d}' for i in range(1, 6)]
MERCHANT_NAMES = {mid: fake.company() for mid in MERCHANT_IDS}

# 促销类型（每个类型一条记录）
PROMOTION_TYPES = [
    'Full reduction',
    'Discount',
    'Coupon',
    'Limited-Time Sale',
    'Bundle Deal'
]

def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset='utf8mb4',
        autocommit=False
    )

def init_dimensions():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 临时禁用外键检查，方便清空表
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            # 清空相关表（按依赖顺序反向清空，避免外键冲突）
            tables_to_truncate = [
                'dwd_dim_products',
                'dwd_users',
                'dim_promotions',
                'dwd_merchant_operations',
            ]
            for table in tables_to_truncate:
                cursor.execute(f"TRUNCATE TABLE {table}")
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
        print("已清空维度表。")

        # 1. 插入商家操作记录 (dwd_merchant_operations)
        print("插入商家操作记录...")
        op_types = ['remove_product', 'update_price', 'update_stock']
        merchant_ops = []
        op_id = 1
        for mid in MERCHANT_IDS:
            # 每个商家生成1-3条操作记录
            for _ in range(random.randint(1, 3)):
                op_time = datetime.now() - timedelta(days=random.randint(1, 30))
                op = (
                    op_id,
                    mid,
                    MERCHANT_NAMES[mid],
                    random.choice(op_types),
                    round(random.uniform(10, 500), 2) if random.random() > 0.3 else None,
                    f'PROD{random.randint(1, 10):03d}',  # 产品ID暂随机，后续插入产品时需对应，但此处不影响
                    op_time.strftime('%Y-%m-%d %H:%M:%S'),
                    op_time.strftime('%Y-%m-%d %H:%M:%S'),
                    op_time.strftime('%Y-%m-%d %H:%M:%S')
                )
                merchant_ops.append(op)
                op_id += 1

        insert_op_sql = """
            INSERT INTO dwd_merchant_operations 
            (operation_id, merchant_id, merchant_name, operation_type, new_place, product_id, operation_time, created_time, updated_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_op_sql, merchant_ops)
        conn.commit()
        print(f"已插入 {len(merchant_ops)} 条商家操作记录。")

        # 2. 插入促销活动 (dim_promotions)
        print("插入促销活动...")
        promos = []
        for i, ptype in enumerate(PROMOTION_TYPES, 1):
            start = datetime.now() - timedelta(days=random.randint(10, 30))
            end = start + timedelta(days=random.randint(7, 30))
            promo = (
                f'PROMO{i:03d}',
                fake.catch_phrase(),
                ptype,
                '{"categories": ["electronics", "clothing"]}' if random.random() > 0.3 else None,
                '{"min_amount": 100, "discount": 0.1}',
                start.strftime('%Y-%m-%d %H:%M:%S'),
                end.strftime('%Y-%m-%d %H:%M:%S'),
                random.choice(['Not Started', 'In Progress', 'Completed', 'Cancelled']),
                fake.text(max_nb_chars=30) if random.random() > 0.5 else '',
                start.strftime('%Y-%m-%d %H:%M:%S'),
                end.strftime('%Y-%m-%d %H:%M:%S')
            )
            promos.append(promo)

        insert_promo_sql = """
            INSERT INTO dim_promotions 
            (promotion_id, promotion_name, promotion_type, applicable_categories, discount_rules, start_time, end_time, promotion_status, target_audience, created_time, updated_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_promo_sql, promos)
        conn.commit()
        print(f"已插入 {len(promos)} 条促销活动。")

        # 3. 插入用户 (dwd_users)
        print("插入用户...")
        users = []
        for i in range(1, 11):  # 10个用户
            uid = f'USER{i:03d}'
            created = datetime.now() - timedelta(days=random.randint(1, 90))
            user = (
                uid,
                fake.name() if random.random() > 0.1 else '',
                fake.address() if random.random() > 0.1 else '',
                created.strftime('%Y-%m-%d %H:%M:%S'),
                created.strftime('%Y-%m-%d %H:%M:%S')
            )
            users.append(user)

        insert_user_sql = """
            INSERT INTO dwd_users 
            (user_id, user_name, us_address, created_time, updated_time)
            VALUES (%s, %s, %s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_user_sql, users)
        conn.commit()
        print(f"已插入 {len(users)} 个用户。")

        # 4. 插入商品 (dwd_dim_products)
        print("插入商品...")
        products = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
        statuses = ['active', 'inactive', 'out_of_stock', 'banned']
        lifecycles = ['new', 'hot', 'normal', 'declining', 'discontinued']
        for i in range(1, 16):  # 15个商品
            pid = f'PROD{i:03d}'
            merchant_id = random.choice(MERCHANT_IDS)
            listing = datetime.now() - timedelta(days=random.randint(1, 60))
            delisting = listing + timedelta(days=random.randint(30, 365)) if random.random() > 0.3 else None
            product = (
                pid,
                merchant_id,
                fake.word() + ' ' + fake.word(),
                f'CAT{random.randint(1,5):03d}',
                random.choice(categories),
                listing.strftime('%Y-%m-%d %H:%M:%S'),
                delisting.strftime('%Y-%m-%d %H:%M:%S') if delisting else None,
                random.randint(10, 500),
                random.choice(statuses),
                round(random.uniform(50, 3000), 2),
                round(random.uniform(1, 5), 1) if random.random() > 0.2 else None,
                random.randint(0, 1000),
                random.choice(lifecycles),
                listing.strftime('%Y-%m-%d %H:%M:%S'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            products.append(product)

        insert_product_sql = """
            INSERT INTO dwd_dim_products 
            (product_id, merchant_id, product_name, product_category_id, category_name, listing_time, delisting_time, current_stock, product_status, price, average_rating, rating_count, product_lifecycle, created_time, updated_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_product_sql, products)
        conn.commit()
        print(f"已插入 {len(products)} 个商品。")

        print("所有维度数据初始化完成！")

    except Exception as e:
        conn.rollback()
        print(f"出错，已回滚: {e}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    init_dimensions()