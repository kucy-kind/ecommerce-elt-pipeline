

# 技术栈

Apache Airflow 3.x+ | dbt 1.7+ | Python 3.10 | MySQL 8.0 | Docker Compose

# 项目简介

这是一个端到端的**现代数据栈（Modern Data Stack）** 实战项目，完整模拟了电商场景下从原始数据生成到 BI 报表推送的全链路。

- **数据生成**：基于 Python Faker 按天生成仿真订单、用户行为、退货等数据，以 JSON/CSV 格式写入 MinIO 对象存储（ODS 层）。
- **分层建模**：采用 **ODS → DWD → DWS → ADS** 四层架构，通过 Python 脚本清洗数据，并利用 dbt 完成维度建模与指标聚合。
- **数据质量闭环**：在 dbt 中嵌入外键一致性、业务逻辑等校验规则，测试失败将阻断 Airflow DAG 并触发告警，确保下游数据可靠。
- **可视化呈现**：ADS 层直接对接 Metabase，构建商家日报、用户月报、平台健康度看板，形成完整的业务监控能力，无论是用户或者是商家，最终会获得一个公共连接，这个连接可以直接查看表报。

**技术关键词**：Airflow 调度 · dbt 转换 · 维度建模 · 增量刷新 · 数据质量监控 · Docker 一键部署

# 系统架构

```mermaid
graph LR
 subgraph 数据生成
 A[Faker 模拟脚本]
 end
 subgraph 对象存储 ODS
 B[MinIO]
 end
 subgraph 计算与调度
 C[Airflow DAG]
 end
 subgraph 数据仓库分层
 D[MySQL DWD<br/>清洗后事实/维度表]
 E[MySQL DWS<br/>主题宽表]
 F[MySQL ADS<br/>应用指标表]
 H[MySQL LINK<br/>连接存储]
 end
 subgraph 数据质量
 G[dbt test<br/>一致性校验]
 end
 subgraph 可视化
 P[Metabase<br/>BI 看板]
 end
 A -->|每日 JSON/CSV| B
 C -->|触发 Python 清洗| B
 B -->|读取并清洗| D
 C -->|运行 dbt| D
 D -->|dbt 转换| E
 E -->|dbt 聚合| F
 C -->|执行测试| G
 G -->|失败阻断| C
 F -->|SQL 查询| P
 P -->|调用API| H
 ```

## 技术栈

| 组件 | 技术选型 | 作用 |
| :--- | :--- | :--- |
| **工作流调度** | Apache Airflow 3.x+ | 编排所有 ETL 任务，管理依赖与告警 |
| **数据转换** | dbt-core 1.7+ (MySQL 适配器) | 实现 SQL 模型、测试、文档一体化 |
| **数据清洗** | Python 3.10 (Pandas) | 处理 ODS 层 JSON/CSV，标准化写入 DWD |
| **数据仓库** | MySQL 8.0 | 存储 DWD、DWS、ADS、LINK 层表 |
| **对象存储** | MinIO | 存放原始 ODS 层文件（S3 兼容） |
| **数据可视化** | Metabase | 创建商家、用户、平台报表看板 |
| **容器化** | Docker & Docker Compose | 一键启动所有服务，环境隔离 |

## 快速开始策略

### 1. 环境要求

-   Docker 20.10+
    
-   Docker Compose 2.0+
    
-   至少 8GB 可用内存（给 MySQL 和 Airflow）
    

### 2. 克隆仓库

```bash
git clone git@github.com:[你的用户名]/[仓库名].git
cd [仓库名]
```
### 3. 准备挂载目录（防止 Docker 启动报错）
```bash
mkdir -p mysql/data minio/data airflow/logs
```
### 4. 配置环境变量
```bash
cp .env.example .env
编辑 .env，将密码项替换为你自己的强密码
```
### 5. 启动所有服务
```bash
docker-compose up -d
首次启动会拉取镜像并构建 Airflow 镜像，大约需要 3-5 分钟。
```
### 6. 访问各组件
| 服务 | 地址 | 默认账号 / 密码 |
| :--- | :--- | :--- |
| **Airflow** | http://localhost:8080 | `admin` / `.env`中的  `AIRFLOW_ADMIN_PASSWORD`需要在`logs`查找 |
| **MinIO Console** | http://localhost:9090 | `minioadmin` / 你设置的 `.env` 中 `MINIO_ROOT_PASSWORD` |
| **Metabase** | http://localhost:3000 | 首次访问需自行配置（数据库连接信息见 `.env`） |
| **MySQL** | localhost:3306 | `root` / 你设置的 `.env` 中 `MYSQL_ROOT_PASSWORD` |

### 7. 触发数据管道
1.  登录 Airflow。
2.  找到 DAG：`ecommerce_elt_pipeline`。
3.  点击播放按钮手动触发一次。
4.  等待 DAG 运行结束（所有任务变绿）。

##  数据分层设计

| 层级 | 存储位置 | 数据形态 | 关键技术 |
| :--- | :--- | :--- | :--- |
| **ODS** | MinIO (`/data`) | 原始 JSON/CSV（不可变） | Python Faker 生成 |
| **DWD** | MySQL `dwd_*` | 清洗后的事实表与维度表 | Python 清洗 + dbt test |
| **DWS** | MySQL `dws_*` | 主题宽表（轻度汇总） | dbt 模型（增量更新） |
| **ADS** | MySQL `ads_*` | 报表指标（应用层） | dbt 聚合，直接对接 BI |
| **LINK** | MySQL `*_link` | 表报连接 | 调用API，任务并行生成 |

**核心表举例**：
- `dwd_orders`：订单事实表，关联用户/商家/商品维度。
- `dws_user_profile`：用户主题宽表，含近30天行为指标。
- `dws_business_daily_health`：平台健康日报，含健康分计算。
##  数据质量监控
项目中实现了以下自动化质量检查：
| 检查类型 | 具体逻辑 | 失败处理 |
| :--- | :--- | :--- |
| **外键一致性** | 确保 `dwd_returns.order_id` 在 `dwd_orders` 中存在 | 阻断 DAG，发送邮件告警 |
| **非空约束** | 订单金额、用户ID 等关键字段不能为空 | 同上 |
| **业务逻辑** | 退款金额 ≤ 订单金额，下单时间 < 退货时间 | 同上 |
## 报表预览（Metabase）

### 用户

**月表：**[用户日报](http://192.168.160.129:3000/public/dashboard/06dfa859-6356-4a3c-884d-837eaff32555?date=2026-04-01&id=86)
**年表:** [用户年表](http://192.168.160.129:3000/public/dashboard/3610bae2-9ebd-47a6-b03b-36d9c1063c5b?date=2026-01-01&id=10086)

### 商家
**日表：**[商家日报](http://192.168.160.129:3000/public/dashboard/0ac2efec-f43c-43f0-b09e-0ad6d39203cd?date=2026-04-20&id=20002)
**月表：**[商家月表](http://192.168.160.129:3000/public/dashboard/a8ca0447-f469-498f-825a-a1787b2990d2?date=2026-04-01&id=20001)
**年表：**[商家年表](http://192.168.160.129:3000/public/dashboard/a2ab1e11-2f21-49da-81a1-16bc81e6ea9c?date=2026-01-01&id=20001)

### 平台
**日表：**[平台日报](http://192.168.160.129:3000/public/dashboard/c10e84d4-83f2-4b74-bd65-493e95862cfc?date=2026-04-1)
**月表：**[平台月表](http://192.168.160.129:3000/public/dashboard/12270b67-87f8-4ebc-9a21-5059dd4bd804?date=2026-04-01)
**年表：**[平台年表](http://192.168.160.129:3000/public/dashboard/21884283-292a-4b18-aba2-59de3e2c7794?date=2026-01-01)
# 项目结构
```bash
├── airflow/
│ ├── dags/ # Airflow DAG 定义文件
│ ├── logs/ # 运行时日志（不上传 Git）
│ └── plugins/ # 自定义 Airflow 插件
├── dbt/ # dbt 项目根目录（对应 etl_dbt/etl）
│ ├── models/ # SQL 模型（分层：dwd/dws/ads）
│ └── dbt_project.yml # dbt 项目配置文件
├── data_generator/ # 基础数据初始化模块
│ └── data_init.py # 生成少量用户、商家等静态维度数据
├── scripts/ # Python ETL 脚本集合
│ ├── data_generator/ # Faker 仿真数据生成器（按天生成 ODS 文件）
│ └── clean/ # ODS → DWD 层数据清洗脚本
├── docker-compose.yml # Docker 服务编排定义（MySQL、MinIO、Airflow、Metabase 等）
├── Dockerfile_airflow # Airflow 镜像定制文件
├── Dockerfile # Python 运行环境镜像定制
├── requirements.txt # Python 项目依赖库清单
├── .env.example # 环境变量配置模板（需复制为 .env 并填写真实值）
├── .gitignore # Git 忽略规则（排除敏感文件与运行时目录）
└── README.md # 项目说明文档（本文件）
```
注意项目数据流开始生成时，需要先运行一次data_init.py脚本，保证数据库中存有最基础的一些如（少量用户、少量商家、少量活动等）之后就不再需要运行这个脚本了
