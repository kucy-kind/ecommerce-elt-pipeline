#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向已存在的 MySQL 表中插入基础维度数据（适配新表结构）。
全程禁用外键检查，避免因外键引用缺失导致的失败。
"""

import os
import random
from datetime import datetime, timedelta
import pymysql
from faker import Faker

# MySQL 连接配置
MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'rootpassword')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'etl_db')

fake = Faker('zh_CN')
random.seed(42)

# 促销类型（5 种）
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

def insert_basic_data():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 全程禁用外键检查
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")

            # 清空相关维度表
            tables_to_truncate = [
                'dwd_merchant_operations',
                'dwd_dim_products',
                'dwd_dim_promotions',
                'dwd_users',
                'dwd_dim_merchants',
            ]
            for table in tables_to_truncate:
                cursor.execute(f"TRUNCATE TABLE {table}")
            print("已清空相关维度表。")

            # ================== 1. 插入商家 (dwd_dim_merchants) ==================
            print("插入商家数据...")
            merchant_ids = []
            merchant_statuses = ['active', 'inactive', 'banned']
            for i in range(1, 6):  # 5个商家
                merchant_id = f'MERC{i:03d}'
                merchant_ids.append(merchant_id)
                register_time = datetime.now() - timedelta(days=random.randint(30, 365))
                row = (
                    merchant_id,
                    fake.company(),
                    register_time.strftime('%Y-%m-%d %H:%M:%S'),
                    random.choice(merchant_statuses),
                    fake.phone_number() if random.random() > 0.3 else '',
                )
                insert_merchant_sql = """
                    INSERT INTO dwd_dim_merchants 
                    (merchant_id, merchant_name, register_time, merchant_status, contact_info)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(insert_merchant_sql, row)
            print(f"已插入 {len(merchant_ids)} 个商家。")

            # ================== 2. 插入用户 (dwd_users) ==================
            print("插入用户...")
            user_ids = []
            for i in range(1, 11):  # 10 个用户
                uid = f'USER{i:03d}'
                user_ids.append(uid)
                created = datetime.now() - timedelta(days=random.randint(1, 90))
                user = (
                    uid,
                    fake.name() if random.random() > 0.1 else '',
                    fake.address() if random.random() > 0.1 else '',
                )
                insert_user_sql = """
                    INSERT INTO dwd_users 
                    (user_id, user_name, us_address)
                    VALUES (%s, %s, %s)
                """
                cursor.execute(insert_user_sql, user)
            print(f"已插入 {len(user_ids)} 个用户。")

            # ================== 3. 插入促销活动 (dwd_dim_promotions) ==================
            print("插入促销活动...")
            promo_ids = []
            for i, ptype in enumerate(PROMOTION_TYPES, 1):
                promo_id = f'PROMO{i:03d}'
                promo_ids.append(promo_id)
                start = datetime.now() - timedelta(days=random.randint(10, 30))
                end = start + timedelta(days=random.randint(7, 30))
                promo = (
                    promo_id,
                    fake.catch_phrase(),
                    ptype,
                    '{"categories": ["electronics", "clothing"]}' if random.random() > 0.3 else None,
                    '{"min_amount": 100, "discount": 0.1}',
                    start.strftime('%Y-%m-%d %H:%M:%S'),
                    end.strftime('%Y-%m-%d %H:%M:%S'),
                    random.choice(['Not Started', 'In Progress', 'Completed', 'Cancelled']),
                    fake.text(max_nb_chars=30) if random.random() > 0.5 else '',
                )
                insert_promo_sql = """
                    INSERT INTO dwd_dim_promotions 
                    (promotion_id, promotion_name, promotion_type, applicable_categories, discount_rules, start_time, end_time, promotion_status, target_audience)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_promo_sql, promo)
            print(f"已插入 {len(promo_ids)} 条促销活动。")

            # ================== 4. 插入商品 (dwd_dim_products) ==================
            print("插入商品...")
            product_ids = []
            categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
            statuses = ['active', 'inactive', 'out_of_stock', 'banned']
            lifecycles = ['new', 'hot', 'normal', 'declining', 'discontinued']
            for i in range(1, 16):  # 15 个商品
                pid = f'PROD{i:03d}'
                product_ids.append(pid)
                merchant_id = random.choice(merchant_ids)
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
                )
                insert_product_sql = """
                    INSERT INTO dwd_dim_products 
                    (product_id, merchant_id, product_name, product_category_id, category_name, listing_time, delisting_time, current_stock, product_status, price, average_rating, rating_count, product_lifecycle)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_product_sql, product)
            print(f"已插入 {len(product_ids)} 个商品。")

            # ================== 5. 插入商家操作记录 (dwd_merchant_operations) ==================
            print("插入商家操作记录...")
            op_types = ['add_product', 'remove_product', 'update_price', 'update_stock']
            merchant_ops = []
            op_id = 1
            for mid in merchant_ids:
                for _ in range(random.randint(1, 2)):  # 每个商家1-2条操作
                    op_time = datetime.now() - timedelta(days=random.randint(1, 30))
                    product_id = random.choice(product_ids)
                    op = (
                        op_id,
                        mid,
                        random.choice(op_types),
                        round(random.uniform(10, 500), 2) if random.random() > 0.3 else None,
                        product_id,
                        op_time.strftime('%Y-%m-%d %H:%M:%S'),
                    )
                    merchant_ops.append(op)
                    op_id += 1

            insert_op_sql = """
                INSERT INTO dwd_merchant_operations 
                (operation_id, merchant_id, operation_type, new_place, product_id, operation_time)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_op_sql, merchant_ops)
            print(f"已插入 {len(merchant_ops)} 条商家操作记录。")

            # 提交事务
            conn.commit()
            print("所有基础维度数据插入完成！")

        # 恢复外键检查
        with conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")

    except Exception as e:
        conn.rollback()
        print(f"插入失败，已回滚: {e}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    insert_basic_data()