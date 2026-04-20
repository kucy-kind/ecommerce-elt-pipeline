#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日原始数据生成器（适配新表结构）
- 从 MySQL 读取现有维度 ID（用户、商家、促销、商品）
- 生成新一天的增量维度数据（新用户、新商家、新促销、新商品）
- 生成新一天的事实数据（订单、用户行为、退货等），引用新旧维度 ID
- 所有 CSV 按日期分区上传至 MinIO，路径和文件名使用 ods_ 前缀
- 数据包含脏数据特征（日期格式混乱、空值等），比例可控
- 注意：所有表的 created_time 和 updated_time 均不由脚本生成，由数据库自动填充
"""

import os
import random
import csv
import json
import tempfile
import time
from datetime import datetime, timedelta

import pymysql
from minio import Minio
from minio.error import S3Error
from faker import Faker

# ================== 配置（可通过环境变量覆盖）==================
MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'rootpassword')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'etl_db')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'ods-data')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'

# 每日增量数据量配置
DAILY_CONFIG = {
    'new_users': 200,
    'new_merchants': 20,
    'new_promotions': 10,
    'new_products': 50,
    'orders': 5000,
    'user_actions': 20000,
    'returns': 300,
    'merchant_operations': 500,
    'merchant_open': 100,
    'merchant_return': 80,
    'quality_alerts': 50,
}

fake = Faker('zh_CN')
random.seed(datetime.now().timestamp())

# ================== MySQL 连接 ==================
def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# ================== 从 MySQL 读取现有维度 ID ==================
def load_existing_dimensions(conn):
    dims = {}
    with conn.cursor() as cursor:
        cursor.execute("SELECT user_id FROM dwd_users")
        dims['users'] = [row['user_id'] for row in cursor.fetchall()]

        # 新商家表名
        cursor.execute("SELECT merchant_id FROM dwd_dim_merchants")
        dims['merchants'] = [row['merchant_id'] for row in cursor.fetchall()]

        cursor.execute("SELECT promotion_id FROM dwd_dim_promotions")
        dims['promotions'] = [row['promotion_id'] for row in cursor.fetchall()]

        cursor.execute("SELECT product_id, merchant_id FROM dwd_dim_products")
        products = cursor.fetchall()
        dims['products'] = products
        dims['product_ids'] = [p['product_id'] for p in products]
        dims['product_to_merchant'] = {p['product_id']: p['merchant_id'] for p in products}

        cursor.execute("SELECT DISTINCT promotion_type FROM dwd_dim_promotions WHERE promotion_type IS NOT NULL")
        dims['promotion_types'] = [row['promotion_type'] for row in cursor.fetchall()]
    return dims

# ================== 辅助函数 ==================
def random_datetime_on_date(target_date):
    start = datetime.combine(target_date, datetime.min.time())
    end = datetime.combine(target_date, datetime.max.time())
    return start + (end - start) * random.random()

def random_date_str_variants(dt):
    if random.random() < 0.05:
        return ''
    if random.random() < 0.03:
        return 'not_a_date'
    if random.random() < 0.02:
        return '0000-00-00 00:00:00'
    formats = [
        dt.strftime('%Y-%m-%d %H:%M:%S'),
        dt.strftime('%Y/%m/%d %H:%M'),
        dt.strftime('%d-%m-%Y %H:%M'),
        dt.strftime('%Y%m%d%H%M%S'),
        dt.strftime('%Y-%m-%d'),
    ]
    return random.choice(formats)

def maybe_null(prob=0.05):
    return random.random() < prob

def random_json_dict():
    d = {fake.word(): fake.word(), fake.word(): random.randint(1, 100)}
    if random.random() > 0.3:
        d['code'] = fake.bothify(text='???-######')
    return json.dumps(d, ensure_ascii=False)

def generate_new_id(prefix, existing_ids):
    while True:
        ts_part = int(time.time() * 1000) % 10000
        rand_part = random.randint(100, 999)
        new_id = f"{prefix}{ts_part}{rand_part}"
        if new_id not in existing_ids:
            return new_id

# ================== 生成新维度数据 ==================
def generate_new_dimensions(target_date, existing_dims):
    new_rows = {
        'dwd_users': [],
        'dwd_dim_merchants': [],      # 新表名
        'dwd_dim_promotions': [],
        'dwd_dim_products': [],
    }
    new_user_ids = set(existing_dims['users'])
    new_merchant_ids = set(existing_dims['merchants'])
    new_promotion_ids = set(existing_dims['promotions'])
    new_product_ids = set(existing_dims['product_ids'])

    # 新用户（created_time 作为业务时间保留，因为它是用户注册时间）
    for _ in range(DAILY_CONFIG['new_users']):
        user_id = generate_new_id('USER', new_user_ids)
        new_user_ids.add(user_id)
        created = random_datetime_on_date(target_date) - timedelta(days=random.randint(0, 1))
        row = {
            'user_id': user_id,
            'user_name': fake.name() if maybe_null(0.1) else '',
            'us_address': fake.address() if maybe_null(0.1) else '',
            'created_time': random_date_str_variants(created),   # 用户注册时间，保留
            # 不包含 updated_time，让数据库自动更新
        }
        new_rows['dwd_users'].append(row)

    # 新商家（register_time 业务时间，merchant_status，contact_info 新增）
    merchant_statuses = ['active', 'inactive', 'banned']
    for _ in range(DAILY_CONFIG['new_merchants']):
        merchant_id = generate_new_id('MERC', new_merchant_ids)
        new_merchant_ids.add(merchant_id)
        reg_time = random_datetime_on_date(target_date) - timedelta(days=random.randint(0, 30))
        row = {
            'merchant_id': merchant_id,
            'merchant_name': fake.company(),
            'register_time': random_date_str_variants(reg_time),
            'merchant_status': random.choice(merchant_statuses),
            'contact_info': fake.phone_number() if maybe_null(0.3) else '',
            # 不包含 created_time, updated_time
        }
        new_rows['dwd_dim_merchants'].append(row)

    # 新促销（业务时间 start_time, end_time 保留）
    promo_types = ['Full reduction', 'Discount', 'Coupon', 'Limited-Time Sale', 'Bundle Deal']
    promo_statuses = ['Not Started', 'In Progress', 'Completed', 'Cancelled']
    for _ in range(DAILY_CONFIG['new_promotions']):
        promo_id = generate_new_id('PROMO', new_promotion_ids)
        new_promotion_ids.add(promo_id)
        start = random_datetime_on_date(target_date) - timedelta(days=random.randint(1, 10))
        end = start + timedelta(days=random.randint(7, 30))
        row = {
            'promotion_id': promo_id,
            'promotion_name': fake.catch_phrase(),
            'promotion_type': random.choice(promo_types),
            'applicable_categories': random_json_dict() if maybe_null(0.3) else None,
            'discount_rules': random_json_dict(),
            'start_time': random_date_str_variants(start),
            'end_time': random_date_str_variants(end),
            'promotion_status': random.choice(promo_statuses),
            'target_audience': fake.text(max_nb_chars=30) if maybe_null(0.5) else '',
            # 不包含 created_time, updated_time
        }
        new_rows['dwd_dim_promotions'].append(row)

    # 新商品（listing_time, delisting_time 业务时间保留）
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    statuses = ['active', 'inactive', 'out_of_stock', 'banned']
    lifecycles = ['new', 'hot', 'normal', 'declining', 'discontinued']
    all_merchant_ids = list(new_merchant_ids)
    for _ in range(DAILY_CONFIG['new_products']):
        product_id = generate_new_id('PROD', new_product_ids)
        new_product_ids.add(product_id)
        merchant_id = random.choice(all_merchant_ids)
        listing = random_datetime_on_date(target_date) - timedelta(days=random.randint(0, 10))
        delisting = listing + timedelta(days=random.randint(30, 365)) if maybe_null(0.3) else None
        row = {
            'product_id': product_id,
            'merchant_id': merchant_id,
            'product_name': fake.word() + ' ' + fake.word(),
            'product_category_id': f'CAT{random.randint(1,5):03d}',
            'category_name': random.choice(categories),
            'listing_time': random_date_str_variants(listing),
            'delisting_time': random_date_str_variants(delisting) if delisting else '',
            'current_stock': random.randint(10, 500),
            'product_status': random.choice(statuses),
            'price': round(random.uniform(50, 3000), 2),
            'average_rating': round(random.uniform(1, 5), 1) if maybe_null(0.2) else None,
            'rating_count': random.randint(0, 1000),
            'product_lifecycle': random.choice(lifecycles),
            # 不包含 created_time, updated_time
        }
        new_rows['dwd_dim_products'].append(row)

    updated_dims = {
        'users': list(new_user_ids),
        'merchants': list(new_merchant_ids),
        'promotions': list(new_promotion_ids),
        'product_ids': list(new_product_ids),
        'product_to_merchant': existing_dims['product_to_merchant'].copy(),
        'promotion_types': existing_dims['promotion_types'],
    }
    for p in new_rows['dwd_dim_products']:
        updated_dims['product_to_merchant'][p['product_id']] = p['merchant_id']

    return new_rows, updated_dims

# ================== 生成事实数据 ==================
def generate_facts(target_date, dims):
    rows = {
        'dwd_orders': [],                # 包含所有订单时间字段
        'dwd_user_actions': [],
        'dwd_returns': [],                # 包含所有退货时间字段
        'dwd_merchant_operations': [],
        'dwd_data_quality_alerts': [],
        'merchant_OPEN': [],               # 保留，但需适配新外键
        'merchant_return': [],              # 保留
    }

    base_order_id = int(time.time() * 1000)
    next_order_id = base_order_id

    # 1. 订单（已合并 dwd_order_time 的时间字段）
    # 在 generate_facts 函数中，替换原有的订单生成循环
        # 1. 订单（已合并 dwd_order_time 的时间字段）
    for i in range(DAILY_CONFIG['orders']):
        # 使用日期 + 毫秒 + 序号生成唯一 order_id，避免跨天冲突和长度溢出
        date_prefix = target_date.strftime('%Y%m%d')
        seq = f"{i:04d}"
        order_id = f"{date_prefix}{int(time.time() * 1000) % 1000000}{seq}"

        user_id = random.choice(dims['users'])
        product_id = random.choice(dims['product_ids'])
        promotion_id = random.choice(dims['promotions']) if dims['promotions'] and maybe_null(0.7) else None
        order_amount = round(random.uniform(20, 2000), 2)
        status = random.choices(
            ['pending', 'paid', 'shipped', 'completed', 'cancelled', 'refunded'],
            weights=[5, 10, 20, 50, 10, 5], k=1
        )[0]
        coupon_info = random_json_dict() if random.random() > 0.7 else None
        quality_flag = random.choices(['valid', 'invalid', 'suspicious'], weights=[90, 5, 5])[0]

        # 生成业务时间
        order_dt = random_datetime_on_date(target_date)
        payment_dt = order_dt + timedelta(minutes=random.randint(5, 120)) if status in ['paid', 'shipped', 'completed', 'refunded'] else None
        shipping_dt = payment_dt + timedelta(hours=random.randint(1, 24)) if status in ['shipped', 'completed', 'refunded'] else None
        complete_dt = shipping_dt + timedelta(days=random.randint(1, 3)) if status in ['completed', 'refunded'] else None

        order_row = {
            'order_id': order_id,          # 现在是字符串类型
            'user_id': user_id,
            'product_id': product_id,
            'order_amount': order_amount,
            'order_status': status,
            'shipping_address': fake.address(),
            'promotion_id': promotion_id,
            'coupon_info': coupon_info,
            'data_quality_flag': quality_flag,
            'order_time': random_date_str_variants(order_dt),
            'payment_time': random_date_str_variants(payment_dt) if payment_dt else '',
            'shipping_time': random_date_str_variants(shipping_dt) if shipping_dt else '',
            'complete_time': random_date_str_variants(complete_dt) if complete_dt else '',
        }
        rows['dwd_orders'].append(order_row)

    # 2. 用户行为
    action_types = ['register', 'login', 'browse', 'favorite', 'add_cart', 'purchase', 'search', 'logout']
    for _ in range(DAILY_CONFIG['user_actions']):
        user_id = random.choice(dims['users'])
        action_dt = random_datetime_on_date(target_date)
        session_id = fake.uuid4() if maybe_null(0.2) else None
        product_id = random.choice(dims['product_ids']) if random.random() > 0.3 else None
        action_row = {
            'action_id': None,  # 自增，CSV中可留空
            'user_id': user_id,
            'session_id': session_id,
            'action_type': random.choice(action_types),
            'action_time': random_date_str_variants(action_dt),
            'product_id': product_id,
            'stay_duration_seconds': random.randint(5, 600) if maybe_null(0.2) else None,
            'device_type': random.choice(['PC', 'Mobile', 'Tablet']),
            'is_abnormal': random.choice([True, False]) if maybe_null(0.1) else False,
            'abnormal_reason': fake.sentence() if random.random() > 0.7 else None,
            # 不包含 created_time
        }
        rows['dwd_user_actions'].append(action_row)

    # 3. 退货（已合并 dwd_return_time 的时间字段）
    completed_orders = [o for o in rows['dwd_orders'] if o['order_status'] in ['completed', 'refunded']]
    return_count = min(DAILY_CONFIG['returns'], len(completed_orders))
    if return_count > 0:
        selected_orders = random.sample(completed_orders, return_count)
    else:
        selected_orders = []

    return_num_base = int(time.time() * 1000) % 10000
    return_reason_cats = ['quality_issue', 'wrong_item', 'damaged', 'size_issue', 'not_as_described', 'other']
    for idx, order in enumerate(selected_orders):
        return_id = f"RET{return_num_base + idx + 1}"
        user_id = order['user_id']
        product_id = order['product_id']
        merchant_id = dims['product_to_merchant'].get(product_id, random.choice(dims['merchants']))
        apply_dt = random_datetime_on_date(target_date)
        approved_dt = apply_dt + timedelta(days=1) if random.random() > 0.3 else None
        rejected_dt = apply_dt + timedelta(days=1) if random.random() > 0.8 else None
        shipped_back_dt = approved_dt + timedelta(days=2) if approved_dt and random.random() > 0.5 else None
        received_dt = shipped_back_dt + timedelta(days=2) if shipped_back_dt else None
        refund_dt = received_dt + timedelta(days=1) if received_dt else None

        # 根据时间存在性推断退货状态
        if refund_dt:
            ret_status = 'refunded'
        elif received_dt:
            ret_status = 'received'
        elif shipped_back_dt:
            ret_status = 'shipped_back'
        elif approved_dt:
            ret_status = 'approved'
        elif rejected_dt:
            ret_status = 'rejected'
        else:
            ret_status = 'applied'

        return_row = {
            'return_id': return_id,
            'order_id': order['order_id'],
            'user_id': user_id,
            'merchant_id': merchant_id,
            'return_reason_category': random.choice(return_reason_cats),
            'return_reason_detail': fake.text(max_nb_chars=100) if maybe_null(0.2) else '',
            'return_status': ret_status,
            # 所有退货时间字段（业务时间）
            'apply_time': random_date_str_variants(apply_dt),
            'approved_time': random_date_str_variants(approved_dt) if approved_dt else '',
            'rejected_time': random_date_str_variants(rejected_dt) if rejected_dt else '',
            'shipped_back_time': random_date_str_variants(shipped_back_dt) if shipped_back_dt else '',
            'received_time': random_date_str_variants(received_dt) if received_dt else '',
            'refund_time': random_date_str_variants(refund_dt) if refund_dt else '',
            'refund_amount': round(random.uniform(10, order['order_amount']), 2) if maybe_null(0.3) else None,
            'return_quantity': random.randint(1, 3),
            'data_consistency_flag': random.choice(['consistent', 'inconsistent']),
            # 不包含 created_time
        }
        rows['dwd_returns'].append(return_row)

    # 4. 商家操作（已移除 merchant_name，增加 product_id 外键）
    op_types = ['add_product', 'remove_product', 'update_price', 'update_stock']
    for _ in range(DAILY_CONFIG['merchant_operations']):
        merchant_id = random.choice(dims['merchants'])
        product_id = random.choice(dims['product_ids'])
        op_time = random_datetime_on_date(target_date)
        row = {
            'operation_id': None,
            'merchant_id': merchant_id,
            'operation_type': random.choice(op_types),
            'new_place': round(random.uniform(10, 500), 2) if maybe_null(0.3) else None,
            'product_id': product_id,
            'operation_time': random_date_str_variants(op_time),
            # 不包含 created_time, updated_time
        }
        rows['dwd_merchant_operations'].append(row)

    # 5. 商家开店/上架（保留，但需确保外键引用正确）
    for _ in range(DAILY_CONFIG['merchant_open']):
        merchant_id = random.choice(dims['merchants'])
        product_id = random.choice(dims['product_ids'])
        # 获取商品分类（简化，从商品维度获取可能更准确，但此处随机）
        category_name = random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports'])
        merchant_name = f"商户_{merchant_id}"  # 从商家表可获取真实名称，但这里简化
        row = {
            'open_id': None,
            'merchant_name': merchant_name,
            'merchant_id': merchant_id,
            'product_id': product_id,
            'place': round(random.uniform(10, 1000), 2) if maybe_null(0.2) else None,
            'category_name': category_name,
            'current_stock': random.randint(0, 500),
        }
        rows['merchant_OPEN'].append(row)

    # 6. 商家退货记录（保留）
    return_records = rows['dwd_returns']
    merchant_return_count = min(DAILY_CONFIG['merchant_return'], len(return_records))
    if merchant_return_count > 0:
        selected_returns = random.sample(return_records, merchant_return_count)
    else:
        selected_returns = []
    for idx, ret in enumerate(selected_returns):
        row = {
            'return_id': int(ret['return_id'].replace('RET', '')),
            'merchant_id': ret['merchant_id'],
            'return_type': random.choice(['yes', 'no']),
            'user_id': ret['user_id'],
            'order_id': ret['order_id'],
        }
        rows['merchant_return'].append(row)

    # 7. 数据质量告警
    check_types = ['missing_values', 'outliers', 'duplicates', 'format_error', 'referential_integrity']
    severities = ['low', 'medium', 'high', 'critical']
    for _ in range(DAILY_CONFIG['quality_alerts']):
        check_time = random_datetime_on_date(target_date)
        row = {
            'alert_id': None,
            'check_type': random.choice(check_types),
            'problem_description': fake.text(max_nb_chars=200),
            'problem_count': random.randint(1, 100),
            'severity': random.choice(severities),
            'check_time': random_date_str_variants(check_time),
            'data_date': target_date.strftime('%Y-%m-%d'),
            'resolved': random.choice([True, False]) if maybe_null(0.3) else False,
            'resolution_notes': fake.text(max_nb_chars=100) if random.random() > 0.7 else None,
            'resolved_time': random_date_str_variants(check_time + timedelta(days=1)) if random.random() > 0.5 else '',
            # 不包含 created_time
        }
        rows['dwd_data_quality_alerts'].append(row)

    return rows

# ================== 上传 CSV 到 MinIO（使用 ods 前缀）==================
def upload_to_minio(all_rows, target_date):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    date_str = target_date.strftime('%Y-%m-%d')
    for table_name, rows in all_rows.items():
        if not rows:
            continue

        # 将表名转换为 ODS 层前缀
        if table_name.startswith('dwd_'):
            ods_table_name = f"ods_{table_name[4:]}"
        else:
            ods_table_name = f"ods_{table_name}"

        with tempfile.NamedTemporaryFile(mode='w', newline='', encoding='utf-8', suffix='.csv', delete=False) as tmpf:
            writer = csv.DictWriter(tmpf, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
            tmp_path = tmpf.name

        object_name = f"{ods_table_name}/{date_str}/{ods_table_name}_{date_str}.csv"
        try:
            client.fput_object(MINIO_BUCKET, object_name, tmp_path, content_type='text/csv')
            print(f"上传成功: {object_name}")
        except S3Error as e:
            print(f"上传失败 {object_name}: {e}")
        finally:
            os.unlink(tmp_path)

# ================== 主函数 ==================
def main(execution_date):
    target_date = execution_date
    print(f"开始生成 {target_date} 的原始数据")

    conn = get_mysql_conn()
    try:
        existing_dims = load_existing_dimensions(conn)
        print(f"从MySQL读取到 {len(existing_dims['users'])} 个用户, {len(existing_dims['merchants'])} 个商家, "
              f"{len(existing_dims['promotions'])} 个促销, {len(existing_dims['product_ids'])} 个商品")

        new_dim_rows, updated_dims = generate_new_dimensions(target_date, existing_dims)
        print(f"新生成维度：{len(new_dim_rows['dwd_users'])} 用户, {len(new_dim_rows['dwd_dim_merchants'])} 商家, "
              f"{len(new_dim_rows['dwd_dim_promotions'])} 促销, {len(new_dim_rows['dwd_dim_products'])} 商品")

        fact_rows = generate_facts(target_date, updated_dims)
        all_rows = {**new_dim_rows, **fact_rows}

        upload_to_minio(all_rows, target_date)

        total = sum(len(v) for v in all_rows.values())
        print(f"{target_date} 数据生成完成，共生成 {total} 条记录")
    except Exception as e:
        print(f"出错: {e}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        target = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    else:
        target = (datetime.now() - timedelta(days=1)).date()
    main(target)