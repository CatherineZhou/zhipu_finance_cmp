#coding=utf-8

from  utils.utils_tools import find_json,exec_sql_s
from utils.llm_model import create_chat_completion
import json



# 查询问题中对应的code 都是什么？
def process_code(value):
    """
    Given a code (e.g., a stock code), search the three tables and return matches.

    Parameters:
        value (str): The code to search for.

    Returns:
        list: A list of tuples (result, table) if found, else empty.
    """
    res_lst = []
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    columns_to_select = ['InnerCode', 'CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                         'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']

    value = value.replace("'", "''")  # Escape single quotes

    for table in tables:
        local_select_cols = columns_to_select.copy()
        if 'US' in table:
            if 'ChiNameAbbr' in local_select_cols:
                local_select_cols.remove('ChiNameAbbr')
            if 'EngNameAbbr' in local_select_cols:
                local_select_cols.remove('EngNameAbbr')

        sql = f"""
        SELECT {', '.join(local_select_cols)}
        FROM {table}
        WHERE SecuCode = '{value}' # 证券代码
        """
        result = exec_sql_s(sql)
        if result:
            res_lst.append((result, table)) # 字段+表名
    else:
        if not res_lst:
            print(f"未在任何表中找到代码为 {value} 的信息。")

    return res_lst


## 在对应表中找到问题中的公司名。
def process_company_name(value):
    """
    Given a company name (or related keyword), search in three tables:
    ConstantDB.SecuMain, ConstantDB.HK_SecuMain, ConstantDB.US_SecuMain.

    Attempts to match various company-related fields (e.g., ChiName, EngName, etc.)
    and returns all matching results along with the table where they were found.

    Parameters:
        value (str): The company name or related string to match.

    Returns:
        list: A list of tuples (result, table) where result is the matched data and table is the table name.
              If no matches found, prints a message and returns an empty list.
    """
    res_lst = []
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']

    # where 后面的查询条件
    columns_to_match = ['CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                        'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']

    # select 的字段。
    columns_to_select = ['InnerCode', 'CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                         'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']

    # Escape single quotes to prevent SQL injection
    value = value.replace("'", "''")

    for table in tables:
        # For the US table, remove columns that may not be available
        local_match_cols = columns_to_match.copy()
        local_select_cols = columns_to_select.copy()
        if 'US' in table:
            if 'ChiNameAbbr' in local_match_cols:
                local_match_cols.remove('ChiNameAbbr')
            if 'ChiNameAbbr' in local_select_cols:
                local_select_cols.remove('ChiNameAbbr')
            if 'EngNameAbbr' in local_match_cols:
                local_match_cols.remove('EngNameAbbr')
            if 'EngNameAbbr' in local_select_cols:
                local_select_cols.remove('EngNameAbbr')

        # Build the WHERE clause with OR conditions for each column
        match_conditions = [f"{col} = '{value}'" for col in local_match_cols]
        where_clause = ' OR '.join(match_conditions)

        sql = f"""
        SELECT {', '.join(local_select_cols)}
        FROM {table}
        WHERE {where_clause}
        """
        result = exec_sql_s(sql)
        if result:
            res_lst.append((result, table))
    else:
        # The 'else' clause in a for loop runs only if no 'break' was encountered.
        # Here it just prints if no results were found.
        if not res_lst:
            print(f"未在任何表中找到公司名称为 {value} 的信息。")

    return res_lst


# 问题中提取的实体，进行处理，拿到标准的名字，或者对应的code
def process_items(item_list):
    """
    问题中涉及的基金名，公司名，或者code。 这些可能包含在多张表， 或者表示多个不同的含义。 可以基于or条件查询，查到那个就是那个？

    Given a list of items (dictionaries) from JSON extraction, attempt to process each based on its key:
    - If key is '基金名称' or '公司名称', use process_company_name.
    - If key is '代码', use process_code.
    - Otherwise, print an unrecognized key message.

    Parameters:
        item_list (list): A list of dictionaries like [{"公司名称": "XX公司"}, {"代码":"600872"}].

    Returns:
        tuple: (res, tables)
               res (str): A formatted string showing what was found.
               tables (list): A list of table names where matches were found.
    """
    res_list = []
    for item in item_list:
        key, value = list(item.items())[0]
        if key in ["基金名称", "公司名称"]:
            res_list.extend(process_company_name(value)) # 问题中相关公司名在表中找到。
        elif key == "代码":
            res_list.extend(process_code(value)) # 问题中相关code 在表中找到
        else:
            print(f"无法识别的键：{key}")

    # Filter out empty results
    res_list = [i for i in res_list if i]
    res = ''
    tables = []
    for result_data, table_name in res_list:
        tables.append(table_name)
        res += f"预处理程序通过表格：{table_name} 查询到以下内容：\n {json.dumps(result_data, ensure_ascii=False, indent=1)} \n"

    return res, tables

##问题中实体提取
def process_question(question):
    """
    Given a question, run it through a prompt to perform Named Entity Recognition (NER),
    extract entities (公司名称, 代码, 基金名称), parse the assistant's JSON response,
    and process the items to retrieve relevant information from the database.

    Parameters:
        question (str): The user question.

    Returns:
        tuple: (res, tables) where
               res (str) - Processed result details as a string.
               tables (list) - List of tables involved in the final result.
    """
    prompt = '''
    你将会进行命名实体识别任务，并输出实体json，主要识别以下几种实体：
    公司名称，代码，基金名称。

    其中，公司名称可以是全称，简称，拼音缩写，代码包含股票代码和基金代码，基金名称包含债券型基金，
    以下是几个示例：
    user:唐山港集团股份有限公司是什么时间上市的（回答XXXX-XX-XX）
    当年一共上市了多少家企业？
    这些企业有多少是在北京注册的？
    assistant:```json
    [{"公司名称":"唐山港集团股份有限公司"}]
    ```
    user:JD的职工总数有多少人？
    该公司披露的硕士或研究生学历（及以上）的有多少人？
    20201月1日至年底退休了多少人？
    assistant:```json
    [{"公司名称":"JD"}]
    ```
    user:600872的全称、A股简称、法人、法律顾问、会计师事务所及董秘是？
    该公司实控人是否发生改变？如果发生变化，什么时候变成了谁？是哪国人？是否有永久境外居留权？（回答时间用XXXX-XX-XX）
    assistant:```json
    [{"代码":"600872"}]
    ```
    user:华夏鼎康债券A在2019年的分红次数是多少？每次分红的派现比例是多少？
    基于上述分红数据，在2019年最后一次分红时，如果一位投资者持有1000份该基金，税后可以获得多少分红收益？
    assistant:```json
    [{"基金名称":"华夏鼎康债券A"}]
    ```
    user:化工纳入过多少个子类概念？
    assistant:```json
    []
    ```
    '''

    messages = [{'role': 'system', 'content': prompt}, {'role': 'user', 'content': question}]
    aa = create_chat_completion(messages)
    bb = find_json(aa)
    return process_items(bb)  # 根据问题中涉及到的实体，或者代码，提取相关核心信息。例如，innercode， 公司名， 等等。