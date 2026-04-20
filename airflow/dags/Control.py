from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import requests
import json
default_args = {
    'owner': 'kucy',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 10),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['nytgzy@outlook.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ecommerce_etl_pipeline',
    default_args=default_args,
    description='电商监控ELT完整管道',
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['ecommerce', 'dbt'],
) as dag:
    task_generate_data = BashOperator(
        task_id='daily_date_generate',
        bash_command='python /opt/airflow/dags/scripts/generate_daliy_data.py {{ ds }}',
    )
    task_user_clean = BashOperator(
        task_id='clean_user_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/user_clean.py {{ ds }}' ,
    )
    task_merchant_clean = BashOperator(
        task_id='clean_merchant_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/merchant_clean.py {{ ds }}',
    )
    task_merchant_operations_clean = BashOperator(
        task_id='clean_merchant_operations_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/merchant_operations_clean.py {{ ds }}',
    )
    task_product_clean = BashOperator(
        task_id='clean_product_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/product_clean.py {{ ds }}',
    )
    task_promotions_clean = BashOperator(
        task_id='clean_promotions_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/promotions_clean.py {{ ds }}',
    )
    task_user_action_clean = BashOperator(
        task_id='clean_user_action_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/user_action_clean.py {{ ds }}',
    )
    task_orders_clean = BashOperator(
        task_id='clean_orders_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/orders_clean.py {{ ds }}',
    )
    task_return_clean = BashOperator(
        task_id='clean_return_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/return_clean.py {{ ds }}',
    )
    task_data_quality_clean = BashOperator(
        task_id='clean_data_quality_clean',
        bash_command='python /opt/airflow/dags/scripts/clean/data_quality_clean.py {{ ds }}',
    )

    # dbt 任务dws层
    task_dws_business_daily_health = BashOperator(
        task_id='dws_business_daily_health',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select dws_business_daily_health --vars \'{"target_date": "{{ ds }}"}\'',
    )
    task_dws_daily_sales = BashOperator(
        task_id='dws_daily_sales',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select dws_daily_sales --vars \'{"target_date": "{{ ds }}"}\'',
    )
    task_dws_merchant_daily = BashOperator(
        task_id='dws_merchant_daily',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select dws_merchant_daily --vars \'{"target_date": "{{ ds }}"}\'',
    )
    task_dws_merchant_profile = BashOperator(
        task_id='dws_merchant_profile',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select dws_merchant_profile --vars \'{"target_date": "{{ ds }}"}\'',
    )
    task_dws_product_summary = BashOperator(
        task_id='dws_product_summary',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select dws_product_summary --vars \'{"target_date": "{{ ds }}"}\'',
    )
    task_dws_user_profile = BashOperator(
        task_id='dws_user_profile',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select dws_user_profile --vars \'{"target_date": "{{ ds }}"}\'',
    )
    
    #dbt 任务ads层
    task_ads_merchant_daily_snapshot = BashOperator(
        task_id='ads_merchant_daily_snapshot',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_merchant_daily_snapshot --vars \'{"target_date": "{{ ds }}"}\'',
    )
    task_ads_platform_daily_snapshot = BashOperator(
        task_id='ads_platform_daily_snapshot',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_platform_daily_snapshot --vars \'{"target_date": "{{ ds }}"}\'',
    )
    task_ads_user_monthly_snapshot = BashOperator(
        task_id='ads_user_monthly_snapshot',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_user_monthly_snapshot',
    )
    task_ads_merchant_monthly_snapshot = BashOperator(
        task_id='ads_merchant_monthly_snapshot',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_merchant_monthly_snapshot',
    )
    task_ads_platform_monthly_summary = BashOperator(
        task_id='ads_platform_monthly_summary',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_platform_monthly_summary',
    )
    task_ads_platform_promotion_detail = BashOperator(
        task_id='ads_platform_promotion_detail',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_platform_promotion_detail',
    )
    task_ads_user_yearly_snapshot = BashOperator(
        task_id='ads_user_yearly_snapshot',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_user_yearly_snapshot',
    )
    task_ads_merchant_yearly_snapshot = BashOperator(
        task_id='ads_merchant_yearly_snapshot',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_merchant_yearly_snapshot',
    )
    task_ads_platform_yearly_summary = BashOperator(
        task_id='ads_platform_yearly_summary',
        bash_command='cd /opt/airflow/dags/etl_dbt/etl && dbt run --select ads_platform_yearly_summary',
    )
    
    # 时间判断
    def branch_monthly(**context):
        logical_date = context.get('logical_date', context.get('data_interval_start'))
        if logical_date is None:
            return 'skip_monthly'
        if logical_date.day == 1:
            return ['ads_user_monthly_snapshot', 'ads_merchant_monthly_snapshot', 'ads_platform_promotion_detail']
        else:
            return 'skip_monthly'


    def branch_yearly(**context):
        logical_date = context.get('logical_date', context.get('data_interval_start'))
        if logical_date is None:
            return 'skip_yearly'
        if logical_date.month == 1 and logical_date.day == 1:
            return ['ads_user_yearly_snapshot', 'ads_merchant_yearly_snapshot', 'ads_platform_yearly_summary']
        else:
            return 'skip_yearly'
            
    # 调度
    branch_monthly = BranchPythonOperator(
        task_id='branch_monthly',
        python_callable=branch_monthly,
    )
    branch_yearly = BranchPythonOperator(
        task_id='branch_yearly',
        python_callable=branch_yearly,
    )
    
    #跳过
    skip_monthly = EmptyOperator(task_id='skip_monthly')
    skip_yearly = EmptyOperator(task_id='skip_yearly')
    # 对matabase进行调度 注意时间元素
    # ========== 仪表盘配置 ==========
    DASHBOARD_CONFIGS = {
        "user_monthly": {
            "dashboard_id": 2,
            "param_mapping": {"id": "909e8b04", "date": "78dd0fa4"},
            "frequency": "monthly",
            "id_source": "user"
        },
        "user_yearly": {
            "dashboard_id": 3,
            "param_mapping": {"id": "3b6ba703", "date": "15beb77a"},
            "frequency": "yearly",
            "id_source": "user"
        },
        "merchant_daily": {
            "dashboard_id": 4,
            "param_mapping": {"id": "f856044a", "date": "c37fc7d1"},
            "frequency": "daily",
            "id_source": "merchant"
        },
        "merchant_monthly": {
            "dashboard_id": 5,
            "param_mapping": {"id": "d3328db", "date": "b5781aba"},
            "frequency": "monthly",
            "id_source": "merchant"
        },
        "merchant_yearly": {
            "dashboard_id": 6,
            "param_mapping": {"id": "ab4c0010", "date": "c0160d55"},
            "frequency": "yearly",
            "id_source": "merchant"
        },
        "platform_daily": {
            "dashboard_id": 7,
            "param_mapping": {"date": "924a20a3"},
            "frequency": "daily",
            "id_source": "platform"
        },
        "platform_monthly": {
            "dashboard_id": 8,
            "param_mapping": {"date": "3a4d7b4"},
            "frequency": "monthly",
            "id_source": "platform"
        },
        "platform_yearly": {
            "dashboard_id": 9,
            "param_mapping": {"date": "ac9c24bf"},
            "frequency": "yearly",
            "id_source": "platform"
        }
    }
    
    # 根据执行日期判断是否需要生成对应频率的报表，日每天，月一号，年一月一号
    def should_run(frequency: str, logical_date: datetime) -> bool:
        if frequency == "daily":
            return True
        elif frequency == "monthly":
            return logical_date.day == 1
        elif frequency == "yearly":
            return logical_date.month == 1 and logical_date.day == 1
        else:
            return False


    @task
    def generate_all_public_links(**context): # 生成表报的公共链接
        logical_date = context['logical_date']
        date_str = logical_date.strftime('%Y-%m-%d')
        year_str = logical_date.strftime('%Y')

        mysql_hook = MySqlHook(mysql_conn_id='etl_mysql')
        metabase_url = Variable.get("metabase_url")

        link_records = []

        # 获取用户和商家ID
        user_ids = [r[0] for r in mysql_hook.get_records("SELECT user_id FROM dwd_users")]
        merchant_ids = [r[0] for r in mysql_hook.get_records("SELECT merchant_id FROM dwd_dim_merchants")]

        for report_type, config in DASHBOARD_CONFIGS.items():
            if not should_run(config["frequency"], logical_date):
                continue

            dashboard_id = config["dashboard_id"]
            # 从 Airflow Variable 读取该仪表盘的公开 UUID
            try:
                uuid = Variable.get(f"public_uuid_dashboard_{dashboard_id}")
            except:
                print(
                    f"错误：未找到仪表盘 {dashboard_id} 的公开 UUID，请检查 Variable public_uuid_dashboard_{dashboard_id}")
                continue

            base_url = f"{metabase_url}/public/dashboard/{uuid}"
            id_source = config["id_source"]
            param_mapping = config["param_mapping"]

            if id_source == "platform":
                # 平台报表：只需日期参数
                params = {}
                if "date" in param_mapping:
                    params["date"] = date_str
                query_string = "&".join(f"{k}={v}" for k, v in params.items())
                full_url = f"{base_url}?{query_string}" if query_string else base_url

                link_records.append({
                    "table": f"{report_type}_link",
                    "id_value": None,
                    "create_date": date_str,
                    "link": full_url
                })
                continue

            # 用户/商家报表：为每个ID生成链接
            ids = user_ids if id_source == "user" else merchant_ids
            id_column = "user_id" if id_source == "user" else "merchant_id"

            for obj_id in ids:
                params = {}
                # 根据 param_mapping 构造筛选参数
                for param_key, param_id in param_mapping.items():
                    if param_key == "id":
                        params["id"] = obj_id
                    elif param_key == "date":
                        params["date"] = year_str if config["frequency"] == "yearly" else date_str
                query_string = "&".join(f"{k}={v}" for k, v in params.items())
                full_url = f"{base_url}?{query_string}"

                link_records.append({
                    "table": f"{report_type}_link",
                    "id_value": obj_id,
                    "create_date": date_str,
                    "link": full_url,
                    "id_column": id_column
                })

        print(f"共生成 {len(link_records)} 个公开链接")
        return link_records


    @task
    def save_links_to_mysql(link_records: list):
        """将生成的公开链接写入对应的 MySQL 表中"""
        if not link_records:
            print("没有需要保存的链接")
            return

        mysql_hook = MySqlHook(mysql_conn_id='etl_mysql')

        for record in link_records:
            table = record["table"]
            id_val = record.get("id_value")
            create_date = record["create_date"]
            link = record["link"]

            if id_val is not None:
                id_column = record.get("id_column", "user_id")
                sql = f"""
                    INSERT INTO {table} ({id_column}, create_date, link)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE link = VALUES(link)
                """
                mysql_hook.run(sql, parameters=(id_val, create_date, link))
            else:
                sql = f"""
                    INSERT INTO {table} (create_date, link)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE link = VALUES(link)
                """
                mysql_hook.run(sql, parameters=(create_date, link))

        print(f"已成功将 {len(link_records)} 条链接写入数据库")  
    # 依赖关系
    task_generate_data >> [
        task_user_clean,
        task_merchant_clean,
        task_promotions_clean,
        task_data_quality_clean
    ]
    for task in [task_user_clean, task_merchant_clean, task_promotions_clean, task_data_quality_clean]:
        task >> task_product_clean
    task_product_clean >> [
        task_merchant_operations_clean,
        task_user_action_clean,
        task_orders_clean
    ]
    for task in [task_merchant_operations_clean, task_user_action_clean, task_orders_clean]:
        task >> task_return_clean

    dws_tasks = [
        task_dws_daily_sales,
        task_dws_merchant_daily,
        task_dws_product_summary,
        task_dws_user_profile,
        task_dws_business_daily_health,
        task_dws_merchant_profile
    ]
    task_return_clean >> dws_tasks

    daily_ads_tasks = [
        task_ads_merchant_daily_snapshot,
        task_ads_platform_daily_snapshot
    ]
    for dws in dws_tasks:
        for ads in daily_ads_tasks:
            dws >> ads

    task_ads_merchant_monthly_snapshot >> task_ads_platform_monthly_summary

    for ads in daily_ads_tasks:
        ads >> branch_monthly

    branch_monthly >> [
        task_ads_user_monthly_snapshot,
        task_ads_merchant_monthly_snapshot,
        task_ads_platform_promotion_detail
    ]
    branch_monthly >> skip_monthly

    for task in [branch_monthly, skip_monthly]:
        task >> branch_yearly

    branch_yearly >> [
        task_ads_user_yearly_snapshot,
        task_ads_merchant_yearly_snapshot,
        task_ads_platform_yearly_summary
    ]
    branch_yearly >> skip_yearly
    # 生成所有公开链接记录
    public_links = generate_all_public_links()
    # 设置上游依赖：等待所有 ADS 分支结束（包括跳过分支）
    [skip_monthly, branch_monthly, skip_yearly, branch_yearly] >> public_links
    # 将链接存入 MySQL
    save_links = save_links_to_mysql(public_links)