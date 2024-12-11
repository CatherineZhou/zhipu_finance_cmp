# 问题改写
# 问题拆解
# 文本生成sql
# sql 执行
import pandas as pd

from template_prompt import REWRITE_QUERY_PROMPT,TEXT2SQL,MRC
from  llm_model import call_qwen72b,call_glm,call_qwen72b_npu,minmax_chat
from utils import select_data

def chat_model(messages):
    result = call_glm(messages)
    return result

def assembly_prompt(template, query="", system="", context="", history="",rel_table_desc=""):
    prompt = template.replace('[SYSTEM]', system)
    prompt = prompt.replace('[CONTEXT]', context)
    prompt = prompt.replace('[QUERY]', query)
    prompt = prompt.replace('[CON_HISTORY]', history)
    prompt = prompt.replace('[CUR_QUERY]', query)
    prompt = prompt.replace('[TABLES_DESC]', rel_table_desc)

    return prompt


def get_table_intro(db_desc,table_name):
    """
    提取表重要信息
    :param db_desc:
    :param table_name:
    :return: str
    """
    table_info =[]
    for json_str in db_desc:
        table_=json_str['表英文'].lower()
        if table_== table_name:
            table_info.append( f"表中文名:{json_str['库名中文']}.{json_str['表中文']}")
            table_info.append(f"表英文名:{json_str['库名英文'].lower()}.{json_str['表英文'].lower()}")
            table_info.append( f"表描述:{json_str['表描述']}")
            break
    table_info = '\n'.join(table_info)
    return table_info

def get_columns_foreign_keys(colums_desc,table_name):
    """
    获取表中列对应的外键信息备注
    :param colums_desc:
    :param table_name:
    :return:str
    """
    foreign_keys=[]
    for json_str in colums_desc:
        table_eng=json_str['table_name'].lower()
        if table_eng==table_name:
            columns=json_str['columns']
            for col in columns:
                if col['annotation'] is not None:
                    annotation=col['annotation'].strip()
                    foreign_keys.append(f"  -{annotation}")
            break
    foreign_keys = '\n'.join(foreign_keys)
    foreign_keys=f'表外键(Foreign_keys):\n{foreign_keys}'
    return foreign_keys


def get_table_create(db_table,table_name):
    """
    获取创建表语句
    :param colums_desc:
    :param table_name:
    :return: string
    """
    sql = f"SHOW CREATE TABLE {db_table}.{table_name};"
    result = select_data(sql)
    create_table_sql = result[0]['Create Table']
    return create_table_sql


def get_rel_table_desc(db_desc,colums_desc,rel_table):
    rel_table=rel_table.split(',')
    if 'ConstantDB.SecuMain' not in rel_table:
        rel_table.append('ConstantDB.SecuMain')

    rel_table_desc=[]# 表描述

    for table_name in rel_table:
        table_schema=[]
        # 得到表名，表名注意统一都大写
        db_table = table_name.split('.')[0].lower()
        table_name = table_name.split('.')[-1].lower()

        # 获取表描述:
        """
        表中文名:上市公司基本资料.公司概况
        表英文名：AStockBasicInfoDB.LC_StockArchives
        表描述: 收录上市公司的基本情况，包括：联系方式、注册信息、中介机构、行业和产品、公司证券品种及背景资料等内容。
        """
        table_intro=get_table_intro(db_desc, table_name)

        # 获取外键说明，多个外键，用'-'开始并换行
        foreign_keys=get_columns_foreign_keys(colums_desc,table_name)

        # 获取create table 表结构
        tb_create_desc=get_table_create(db_table,table_name)

        # 将表基本介绍，建表语句， 表外键进行拼接
        table_schema.append(table_intro)
        table_schema.append(tb_create_desc)
        table_schema.append(foreign_keys)
        table_schema='\n'.join(table_schema)
        table_schema=f"<TABLE>\n{table_schema}\n</TABLE>"
        rel_table_desc.append(table_schema)

    rel_table_desc='\n\n'.join(rel_table_desc)
    return rel_table_desc

def formate_history(history):
    history_chat = ''
    if len(history)>0:
        for hs in history:
            history_chat = history_chat + f"{hs['role']}:{hs['content']}\n"
    return history_chat


def answer_question():
    import json
    with open('../data/process/query_table_select_single_result.json', 'r') as f:
        content = f.read()
    query_json = json.loads(content) # query

    with open('../data/org_data/db_desc.json') as f:
        db_content = f.read()
    db_desc = json.loads(db_content) # 数据名

    with open('../data/process/table_columns.json') as f:
        db_content = f.read()
    columns_desc = json.loads(db_content)  # 列名


    for json_string in query_json[:1]:
        tid = json_string['tid'] # 当前会话id
        team = json_string['team']
        history=[{}]
        for i, t in enumerate(team):

            id = t['id'] # 问题id
            query = t['question']
            rel_table = t['db_table']
            rel_tables_desc = get_rel_table_desc(db_desc,columns_desc,rel_table)

            # 1.query rewrite
            rewrite_query_prompt = assembly_prompt(REWRITE_QUERY_PROMPT,query,history=formate_history(history))
            rewrite_query = call_qwen72b([{"role":"user","content":rewrite_query_prompt}]) # messages
            rewrite_query=json.loads(rewrite_query)['rewrite_query']
            # 2.问题拆解

            #3.sql 生成
            text2sql_prompt = assembly_prompt(TEXT2SQL,query=rewrite_query,rel_table_desc=rel_tables_desc)
            text2sql_result =chat_model([{"role":"user","content":text2sql_prompt}])
            text2sql_result=text2sql_result.replace('#','')
            print(f'-----text2sql_result:{text2sql_result}')

            #4.调用sql
            sql_result = select_data(text2sql_result)
            sql_result=json.dumps(sql_result,indent=1,ensure_ascii=False)
            print(sql_result)

            # #5.生成回复
            mrc_prompt=assembly_prompt(MRC,context=sql_result,query=rewrite_query)
            result=call_qwen72b([{"role":"user","content":mrc_prompt}])

            history.append({"role":"user","content":rewrite_query})
            history.append({"role": "assistant", "content": result})

answer_question()

