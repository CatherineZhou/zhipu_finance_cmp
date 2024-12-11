# coding=utf-8
import pandas as pd
import sqlite3
import json
from sql_opt import DatabaseManager
import time


#
def select_data(sql, num=10):
    import requests
    import json

    url = "https://comm.chatglm.cn/finglm2/api/query"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer 0609ed029da743a5aff206f83f596290"
    }
    data = {
        "sql": sql,
        "limit": num
    }

    response = requests.post(url, headers=headers, json=data)
    response = response.json()['data']
    return response

# #
tb_name = "astockindustrydb.lc_indfinindicators"

# sql = f"SHOW CREATE TABLE {tb_name};"
# sql = f"select * from {tb_name} limit 10;"
# response=select_data(sql, num=5)
# print(json.dumps(response,indent=1, ensure_ascii=False))

#
db_manager = DatabaseManager('localhost', 'root', 'Herz_3280')
db_manager.connect()


def insert_to_table(db, tb):
    """
    将远程数据库的数据查询插入到本地数据库
    :param db:
    :param tb:
    :return:
    """
    try:

        db_name=db.lower()
        tb_name = f'{db}.{tb}'.lower()
        print('----table name', tb_name)

        # 如果本地不存在db，则创建该db
        db_manager.create_database(db_name)

        # 如果本地表存在，则删除
        db_manager.drop_table(tb_name)

        # 查询远程数据库建表语句，基于该语句本地建表
        sql = f"SHOW CREATE TABLE {tb_name};"
        result = select_data(sql)
        create_table_sql = result[0]['Create Table']
        time.sleep(3)
        db_manager.create_table(tb_name, create_table_sql)
        print(f'------create table :{tb_name}')

        # 查询远程数据库当前表有多少行
        sql = f"select count(*) from {tb_name};"
        result = select_data(sql)
        table_row_nums = result[0]['count(*)']
        table_row_nums =min(table_row_nums,1000)
        time.sleep(3)
        print(f'----{tb_name} have :{table_row_nums}')

        # 查询远程数据
        sql = f"select * from {tb_name};"
        result = select_data(sql, table_row_nums)
        time.sleep(3)
        result=json.dumps(result,indent=1,ensure_ascii=False)
        # 将远程数据存储到本地, 将json转为df

        db_manager.insert_json_data(tb_name,result)  # 插入本地数据库
        print(f'----{tb_name} insert finished ')

        return '1'
    except Exception as e:
        return f'have error :{e}'


def load_table_content():
    df = pd.read_csv('../data/org_data/table_desc.csv')
    df = df.iloc[6:]
    # 库名英文	表英文
    df['flag'] = df[['库名英文', '表英文']].apply(lambda x: insert_to_table(x[0], x[1]),axis=1)


load_table_content()

db_manager.disconnect()
