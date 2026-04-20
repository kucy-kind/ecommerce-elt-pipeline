import pandas as pd
from sqlalchemy import create_engine,text
import os
import json
from datetime import datetime
def data_export(df, mysql_conn, json_columns, dtype, log, t):
    """
    将 DataFrame 数据导出到 MySQL 表
    :param df: pandas DataFrame，包含要插入的数据
    :param mysql_conn: SQLAlchemy 连接字符串（例如 'mysql+pymysql://user:pass@host/db'）
    :param json_columns: 需要作为 JSON 类型处理的列名列表
    :param dtype: SQLAlchemy 数据类型字典（用于指定列类型）
    :param log: 日志记录器
    :param t: 目标 MySQL 表名
    :return: bool 成功返回 True，失败返回 False
    """
    log.info("开始数据导出流程")

    try:
        log.info(f"接收到 DataFrame，包含 {len(df)} 条记录")
        

        # 确保使用 utf8mb4 字符集
        if 'charset=' not in mysql_conn:
            if '?' in mysql_conn:
                mysql_conn += '&charset=utf8mb4'
            else:
                mysql_conn += '?charset=utf8mb4'

        # 自定义 JSON 序列化器，确保中文不被转义
        def json_serializer(obj):
            return json.dumps(obj, ensure_ascii=False)

        engine = create_engine(mysql_conn, json_serializer=json_serializer)

        # 处理 JSON 列：将字符串转换为字典/列表（如果尚未转换）
        for col in json_columns:
            if col in df.columns:
                def ensure_dict_list(val):
                    if isinstance(val, str):
                        try:
                            return json.loads(val)
                        except Exception:
                            return val
                    return val
                df[col] = df[col].apply(ensure_dict_list)

        # 分批插入数据，避免一次性插入过大
        chunk_size = 1000
        total_inserted = 0
        for i in range(0, len(df), chunk_size):
            chunk_df = df.iloc[i:i + chunk_size]
            chunk_df.to_sql(
                name=t,
                con=engine,
                if_exists='append',
                index=False,
                dtype=dtype
            )
            total_inserted += len(chunk_df)
            log.info(f"已插入 {len(chunk_df)} 条记录，总计 {total_inserted}")

        log.info(f"数据已导入MySQL表: {t}")
        
        log.info("数据导出完成")
        return True

    except Exception as e:
        log.error(f"数据导出失败: {e}", exc_info=True)
        return False


def return_name_array(df, log, valid_names):
    """
    检查 DataFrame 的列名是否与给定的有效名称列表匹配
    :param df: pandas DataFrame，待检查的数据
    :param log: 日志记录器
    :param valid_names: 包含有效列名的集合或列表（如数据库表的列名）
    :return: 成功返回元组 (unmark_name, all_columns)，其中 unmark_name 是不在 valid_names 中的列名列表，
             all_columns 是 DataFrame 的所有列名列表；若失败返回 False
    """
    log.info("开始检查 DataFrame 的列名是否匹配！")
    unmark_name = []

    try:
        # 确认 df 是 DataFrame 类型
        if not isinstance(df, pd.DataFrame):
            log.error("输入数据不是 pandas DataFrame 类型！")
            return False

        # 获取所有列名
        all_columns = list(df.columns)
        log.info(f"DataFrame 包含列: {all_columns}")

        # 检查每个列名是否在有效名称集合中
        for col in all_columns:
            if col not in valid_names:
                log.warning(f"列名 '{col}' 不在有效名称集合中！")
                unmark_name.append(col)

        return unmark_name, all_columns

    except Exception as e:
        log.error(f"检查列名时发生错误: {str(e)}")
        return False


def rename_df_columns(df, log, old_name, new_name):
    """
    在 pandas DataFrame 中重命名列名，并递归处理所有字典类型列的内部键名。
    :param df: pandas DataFrame，待处理的 DataFrame
    :param log: 日志记录器
    :param old_name: 需要被替换的旧键名
    :param new_name: 新键名
    :return: 修改后的 DataFrame；若出错则记录错误并返回原 DataFrame
    """
    log.info(f"开始处理 DataFrame，重命名键名: '{old_name}' -> '{new_name}'")

    if df.empty:
        log.warning("DataFrame 为空，无需处理")
        return df

    # 1. 处理顶层列名
    if old_name in df.columns:
        df.rename(columns={old_name: new_name}, inplace=True)
        log.info(f"顶层列名已修改: '{old_name}' -> '{new_name}'")

    # 2. 递归处理嵌套字典的函数
    def rename_in_dict(obj):
        if isinstance(obj, dict):
            # 如果当前字典包含旧键，则重命名
            if old_name in obj:
                obj[new_name] = obj.pop(old_name)
            # 递归处理字典的所有值
            for key, value in obj.items():
                rename_in_dict(value)
        elif isinstance(obj, list):
            # 如果是列表，递归处理每个元素
            for item in obj:
                rename_in_dict(item)
        # 其他类型无需处理

    # 3. 遍历所有列，对可能包含字典/列表的单元格应用递归重命名
    for col in df.columns:
        # 尝试检测列中是否包含字典或列表（常见于 JSON 列）
        # 为了避免对每一行进行昂贵的类型检查，可以先抽样判断
        sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
        if sample is not None and isinstance(sample, (dict, list)):
            # 应用递归函数
            df[col] = df[col].apply(lambda x: rename_in_dict(x) if isinstance(x, (dict, list)) else x)
            log.info(f"列 '{col}' 中的嵌套键名已递归处理")

    log.info("键名重命名完成")
    return df



def clean_df(df, name, not_null_column_name, log):
    """
    清洗 pandas DataFrame 数据：
    1. 删除不需要的列（保留 name 中指定的列）
    2. 检查必需列（not_null_column_name）是否存在
    3. 删除必需列中包含空值的行
    :param df: pandas DataFrame，待清洗的数据
    :param name: 需要保留的列名列表/集合
    :param not_null_column_name: 必须非空的列名列表
    :param log: 日志记录器
    :return: 清洗后的 DataFrame
    """
    log.info("clean程序开始")

    try:
        # 检查输入类型
        if not isinstance(df, pd.DataFrame):
            log.error("输入不是 pandas DataFrame")
            raise TypeError("输入必须是 pandas DataFrame")

        log.info(f"原始数据形状：{df.shape}")

        # 删除不需要的列
        existing_columns = set(df.columns)
        needed_columns = set(name)
        not_need_columns = list(existing_columns - needed_columns)

        if not_need_columns:
            log.warning(f"文件中存在{len(not_need_columns)}个不需要的列：{not_need_columns}")
            df = df.drop(columns=not_need_columns)
            log.info(f"已删除不需要的列，剩余列：{list(df.columns)}")

        # 确保必需列存在
        missing_columns = [col for col in not_null_column_name if col not in df.columns]
        if missing_columns:
            log.error(f"必需的非空列缺失：{missing_columns}")
            raise ValueError(f"缺少必需的列：{missing_columns}")

        # 删除包含空值的行
        original_rows = len(df)
        df_clean = df.dropna(subset=not_null_column_name)
        removed_rows = original_rows - len(df_clean)

        if removed_rows > 0:
            log.info(f"已删除{removed_rows}行包含空值的记录")
        else:
            log.info("所有指定列均已为非空值")

        log.info(f"数据清洗完成，清洗后数据形状：{df_clean.shape}")
        return df_clean

    except Exception as e:
        log.error(f"数据清洗过程中发生错误：{str(e)}")
        raise


def clack(df, columns_name, allowed_values, log):
    log.info(f"clack程序开始，检查列: {columns_name}")

    if columns_name not in df.columns:
        log.error(f"列 {columns_name} 不存在于DataFrame中")
        return df

    # 记录原始情况
    original_count = len(df)
    unique_before = set(df[columns_name].astype(str).str.strip().str.lower())
    log.info(f"原始数据 {columns_name} 的不同值: {sorted(unique_before)}")

    # 将允许值转为小写用于匹配
    allowed_lower = {str(v).lower().strip() for v in allowed_values}

    # 标准化列值：去除空格，转为小写
    df_clean = df.copy()
    df_clean[columns_name] = df_clean[columns_name].astype(str).str.strip().str.lower()

    # 过滤不在允许列表中的记录
    mask = df_clean[columns_name].isin(allowed_lower)
    removed_count = (~mask).sum()

    if removed_count > 0:
        log.warning(f"移除了 {removed_count} 条 {columns_name} 不在允许列表中的记录")
        # 显示被移除的值
        removed_values = df_clean.loc[~mask, columns_name].unique()
        log.warning(f"被移除的值: {removed_values}")
        df_clean = df_clean[mask]

    log.info(f"清理完成: 原始 {original_count} 条 -> 清理后 {len(df_clean)} 条")
    log.info(f"清理后 {columns_name} 的不同值: {sorted(df_clean[columns_name].unique())}")

    return df_clean
def convert_specific_dict_column(df, column_name, log):
    """
    将指定列中的字符串转换为字典/列表对象（用于清洗过程中的预处理）
    :param df: pandas DataFrame
    :param column_name: 需要转换的列名
    :param log: 日志记录器
    :return: 转换后的 DataFrame
    """
    if column_name in df.columns:
        def parse_json(val):
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except Exception:
                    return val
            return val
        df[column_name] = df[column_name].apply(parse_json)
        log.info(f"列 '{column_name}' 已转换为 JSON 对象")
    else:
        log.warning(f"列 '{column_name}' 不存在，跳过转换")
    return df
    
def clean_foreign_key(df, fk_column, ref_table, ref_column, mysql_conn, log):
    """
    根据外键约束清洗 DataFrame：删除那些外键值在参考表中不存在的行。
    不会修改任何列的值，只会删除整行。
    :param df: pandas DataFrame，待清洗的数据
    :param fk_column: 外键列名（在 df 中）
    :param ref_table: 参考表名（MySQL 中的表）
    :param ref_column: 参考表中的对应列名
    :param mysql_conn: SQLAlchemy 连接字符串
    :param log: 日志记录器
    :return: 清洗后的 DataFrame（无效外键行已被删除）
    """
    log.info(f"开始根据外键约束清洗数据：外键列 '{fk_column}' 参考表 '{ref_table}.{ref_column}'")

    # 检查外键列是否存在
    if fk_column not in df.columns:
        log.error(f"外键列 '{fk_column}' 不存在于 DataFrame 中")
        return df

    # 从 MySQL 读取参考表的所有有效值（去重）
    try:
        engine = create_engine(mysql_conn)
        with engine.connect() as conn:
            query = text(f"SELECT DISTINCT `{ref_column}` FROM {ref_table}")
            result = conn.execute(query)
            # 将所有有效值转为字符串并去空格，用于匹配
            valid_values = set(str(row[0]).strip() for row in result.fetchall())
        log.info(f"从参考表 {ref_table} 中读取到 {len(valid_values)} 个有效值")
    except Exception as e:
        log.error(f"读取参考表失败: {e}", exc_info=True)
        # 如果读取失败，为安全起见返回原 DataFrame（或可选择抛出异常）
        return df

    original_rows = len(df)

    # 处理外键列：先去除 NaN，然后转为字符串、去除两端空格，再检查是否在有效值集合中
    # 注意：如果外键列本身有 NaN，这些行会被自动过滤（因为 NaN 不在有效值中）
    df_str = df.copy()
    df_str[fk_column] = df_str[fk_column].astype(str).str.strip()
    mask = df_str[fk_column].isin(valid_values)

    df_clean = df[mask].copy()
    removed_rows = original_rows - len(df_clean)

    if removed_rows > 0:
        log.warning(f"因外键约束移除了 {removed_rows} 行（外键值不在参考表中）")
        # 可选：记录被移除的外键值（最多显示前10个）
        removed_fk_values = df[~mask][fk_column].unique()
        log.warning(f"被移除的外键值示例: {removed_fk_values[:10]}")
    else:
        log.info("所有行的外键值均在参考表中")

    log.info(f"外键清洗完成，剩余 {len(df_clean)} 行")
    return df_clean
    
def default_id_generator(orig_value, seq, existing_set, suffix="_dup"):
    """
    默认的 ID 生成器：在原值后添加后缀和序号，并确保不与现有集合冲突。
    :param orig_value: 原始 ID 值
    :param seq: 序号（从1开始）
    :param existing_set: 现有所有 ID 的集合（用于检查新值是否冲突）
    :param suffix: 要添加的后缀，默认为 "_dup"
    :return: 生成的新 ID
    """
    new_val = f"{orig_value}{suffix}{seq}"
    while new_val in existing_set:
        seq += 1
        new_val = f"{orig_value}{suffix}{seq}"
    return new_val
    
def fix_duplicate_primary_key(df, pk_column, log, id_generator=None):
    """
    检测 DataFrame 中主键列是否有重复值，若有重复则对重复行的主键值进行修改，确保唯一性。

    :param df: pandas DataFrame，待处理的数据
    :param pk_column: 主键列名
    :param log: 日志记录器
    :param id_generator: 可选，用于生成新 ID 的函数，接受参数 (原值, 重复序号, 现有ID集合)
                         若未提供，则使用默认规则：在原值后添加 "_dup{序号}"
    :return: 修改后的 DataFrame（重复主键已被修正）
    """
    log.info(f"开始检查主键列 '{pk_column}' 的重复值")

    if pk_column not in df.columns:
        log.error(f"主键列 '{pk_column}' 不存在于 DataFrame 中")
        return df

    # 找出重复的键值
    duplicate_mask = df[pk_column].duplicated(keep=False)  # 标记所有重复行（包括第一次出现）
    if not duplicate_mask.any():
        log.info(f"主键列 '{pk_column}' 无重复值，无需处理")
        return df

    # 获取重复值列表
    duplicate_values = df.loc[duplicate_mask, pk_column].unique()
    log.warning(f"发现重复主键值: {list(duplicate_values)}")

    # 如果没有提供自定义生成器，使用默认生成器（后缀 _dup）
    if id_generator is None:
        def generator(orig_val, seq, existing_set):
            return default_id_generator(orig_val, seq, existing_set, suffix="_dup")
        id_generator = generator

    # 用于存储已修改的行数和记录
    modified_count = 0
    # 现有所有主键值的集合（用于检查生成的新值是否冲突）
    existing_ids = set(df[pk_column].astype(str))

    # 对每个重复值进行处理
    for val in duplicate_values:
        # 获取该值的所有索引
        indices = df[df[pk_column] == val].index.tolist()
        if len(indices) <= 1:
            continue  # 不可能，因为 duplicate_values 只包含重复的

        log.info(f"处理主键值 '{val}'，共有 {len(indices)} 行重复")
        # 保留第一个出现的行为原始值，从第二个开始修改
        for seq, idx in enumerate(indices[1:], start=1):
            orig_val = df.at[idx, pk_column]
            # 生成新值
            new_val = id_generator(orig_val, seq, existing_ids)
            # 更新 DataFrame
            df.at[idx, pk_column] = new_val
            existing_ids.add(new_val)  # 更新现有集合
            modified_count += 1
            log.debug(f"行 {idx} 主键 '{orig_val}' 已修改为 '{new_val}'")

    log.info(f"主键重复处理完成，共修改 {modified_count} 行")
    return df
    
def resolve_pk_conflict_with_db(df, pk_column, table_name, mysql_conn, log, id_generator=None):
    """
    检查 DataFrame 中的主键是否与数据库现有主键冲突，并对冲突行重新生成主键。
    
    :param df: 待处理的 DataFrame
    :param pk_column: 主键列名
    :param table_name: 目标数据库表名（用于查询现有主键）
    :param mysql_conn: SQLAlchemy 连接字符串
    :param log: 日志记录器
    :param id_generator: 生成新 ID 的函数，默认为在原值后加 "_new{seq}"
    :return: 处理后的 DataFrame（冲突主键已被替换）
    """
    log.info(f"开始检查主键列 '{pk_column}' 与数据库表 {table_name} 的冲突")
    
    if pk_column not in df.columns:
        log.error(f"主键列 '{pk_column}' 不存在")
        return df

    # 1. 从数据库读取所有已存在的主键
    try:
        engine = create_engine(mysql_conn)
        with engine.connect() as conn:
            query = text(f"SELECT `{pk_column}` FROM {table_name}")
            result = conn.execute(query)
            existing_ids = set(str(row[0]) for row in result.fetchall())
        log.info(f"数据库中已存在 {len(existing_ids)} 个主键")
    except Exception as e:
        log.error(f"读取数据库主键失败: {e}")
        return df

    # 2. 找出当前 DataFrame 中与数据库冲突的行
    df_str = df.copy()
    df_str[pk_column] = df_str[pk_column].astype(str)
    conflict_mask = df_str[pk_column].isin(existing_ids)
    
    if not conflict_mask.any():
        log.info("无主键冲突，无需处理")
        return df

    # 3. 对冲突行重新生成 ID
    conflict_rows = df[conflict_mask].copy()
    log.warning(f"发现 {len(conflict_rows)} 行与数据库主键冲突")

    # 如果没有提供自定义生成器，使用默认生成器（后缀 _new）
    if id_generator is None:
        def generator(orig_val, seq, existing_set):
            return default_id_generator(orig_val, seq, existing_set, suffix="_new")
        id_generator = generator

    # 用于跟踪新生成的 ID（防止生成的新 ID 在本次处理中重复）
    current_existing = existing_ids.union(set(df_str.loc[~conflict_mask, pk_column]))
    modified_count = 0

    for idx, row in conflict_rows.iterrows():
        orig_val = str(row[pk_column])
        seq = 1
        new_val = id_generator(orig_val, seq, current_existing)
        df.at[idx, pk_column] = new_val
        current_existing.add(new_val)
        modified_count += 1
        log.debug(f"行 {idx} 主键 '{orig_val}' 已修改为 '{new_val}'")

    log.info(f"主键冲突处理完成，共修改 {modified_count} 行")
    return df

def standardize_datetime_columns(df, datetime_columns, log, not_null_cols=None):
    """
    将 DataFrame 中指定的日期时间列标准化为 '%Y-%m-%d %H:%M:%S' 格式。
    对于在 not_null_cols 中的列，如果解析失败则删除整行；
    对于其他列，解析失败则设为 None 并保留。

    :param df: 原始 DataFrame
    :param datetime_columns: 需要处理的日期时间列名列表
    :param log: 日志记录器
    :param not_null_cols: 必须非空的列名列表（默认为空列表）
    :return: 清洗后的 DataFrame
    """
    log.info(f"开始标准化日期时间列: {datetime_columns}")
    df_clean = df.copy()
    not_null_cols = not_null_cols or []

    # 扩充支持的所有日期格式
    ALLOWED_DATE_FORMATS = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d-%m-%Y %H:%M",
        "%Y%m%d%H%M%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M",
        "%Y%m%d",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for col in datetime_columns:
        if col not in df_clean.columns:
            log.warning(f"列 '{col}' 不存在，跳过")
            continue

        def parse_date(val):
            if pd.isna(val) or str(val).strip() in ['', 'null', 'None', 'not_a_date']:
                return None
            val_str = str(val).strip()
            for fmt in ALLOWED_DATE_FORMATS:
                try:
                    return datetime.strptime(val_str, fmt).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
            log.warning(f"无法解析日期值 '{val}'，该行将被{'删除' if col in not_null_cols else '置空'}")
            return None

        df_clean[col] = df_clean[col].apply(parse_date)

        # 如果该列必须非空，删除解析后为空的行
        if col in not_null_cols:
            before = len(df_clean)
            # 删除 None 和 pandas 的 NA 值
            df_clean = df_clean[df_clean[col].notna()]
            after = len(df_clean)
            if before > after:
                log.info(f"列 '{col}' 因解析失败删除了 {before - after} 行")
        else:
            # 对于允许为空的列，保留 None，无需删除
            pass

    # 最后再次确保所有 not_null_cols 列没有缺失值（防止后续步骤意外引入）
    for col in not_null_cols:
        if col in df_clean.columns:
            before = len(df_clean)
            df_clean = df_clean[df_clean[col].notna()]
            after = len(df_clean)
            if before > after:
                log.warning(f"列 '{col}' 在最终检查中发现缺失值，已删除 {before - after} 行")

    log.info(f"日期标准化完成，剩余 {len(df_clean)} 行")
    return df_clean
    
def normalize_boolean_column(df, col_name, log, default_false=True):
    """
    将DataFrame中指定列的布尔值标准化为 0/1 整数。
    支持多种常见表示：True/False、true/false、yes/no、1/0、t/f等。
    无法识别的值将根据 default_false 设置为 0 或 1。
    :param df: pandas DataFrame
    :param col_name: 列名
    :param log: 日志记录器
    :param default_false: 对于无法识别的值，是否默认为 False (0)。True则默认为0，否则为1。
    :return: 修改后的 DataFrame（直接修改原df，但为链式调用返回df）
    """
    if col_name not in df.columns:
        log.warning(f"列 '{col_name}' 不存在，跳过布尔值标准化")
        return df

    def _normalize(val):
        if pd.isna(val):
            return 0 if default_false else 1
        if isinstance(val, bool):
            return 1 if val else 0
        if isinstance(val, (int, float)):
            return 1 if val else 0
        if isinstance(val, str):
            v = val.strip().lower()
            if v in ('true', 't', 'yes', 'y', '1'):
                return 1
            elif v in ('false', 'f', 'no', 'n', '0'):
                return 0
            else:
                # 无法识别，按默认值处理
                log.warning(f"无法识别的布尔值 '{val}'，设置为 {0 if default_false else 1}")
                return 0 if default_false else 1
        # 其他类型，按默认值处理
        log.warning(f"无法处理的类型 {type(val)} 的值 '{val}'，设置为 {0 if default_false else 1}")
        return 0 if default_false else 1

    df[col_name] = df[col_name].apply(_normalize)
    log.info(f"列 '{col_name}' 已标准化为 0/1 布尔值")
    return df