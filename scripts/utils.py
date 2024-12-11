#coding=utf-8
import json

import pandas as pd
#将列描述处理为按表名存储的json数据格式。

def tabel_col_pro():
    df_columns = pd.read_csv('../data/org_data/columns_desc.csv')
    json_data = df_columns.to_json(orient='records',indent=1,force_ascii=False)
    json_data=json.loads(json_data)
    table_columns=[]
    f_table_name=''
    for i,json_str in enumerate(json_data):

        table_name=json_str['table_name']
        column_name=json_str['column_name']
        column_description=json_str['column_description']
        annotation=json_str['注释']
        if i==0:
            f_table_name=table_name
            columns = []

        if f_table_name==table_name:
            columns.append(
                {
                    "column_name": column_name,
                    "column_description": column_description,
                    "annotation": annotation,
                }
            )
        else:
            table_columns.append(
                {
                    "table_name": f_table_name,
                    "columns":columns
                }
            ) # 将同一个表的字段合并

            f_table_name=table_name
            columns = []
            columns.append(
                {
                    "column_name": column_name,
                    "column_description": column_description,
                    "annotation": annotation,
                }
            )

    table_columns=json.dumps(table_columns,indent=1, ensure_ascii=False)
    with open('../data/process/table_columns.json','w') as f:
        f.write(table_columns)

#tabel_col_pro()

def tabel_col_pro_formate():
    df_columns = pd.read_csv('../data/org_data/columns_desc.csv')
    json_data = df_columns.to_json(orient='records',indent=1,force_ascii=False)
    json_data=json.loads(json_data)
    table_columns=[]
    f_table_name=''
    for i,json_str in enumerate(json_data):

        table_name=json_str['table_name']
        column_name=json_str['column_name']
        column_description=json_str['column_description']
        annotation=json_str['注释']
        if i==0:
            f_table_name=table_name
            columns = []

        if f_table_name==table_name:
            columns.append(f'{column_name}    {column_description}    {annotation}')
        else:
            table_columns.append(
                {
                    "table_name": f_table_name,
                    "columns":columns
                }
            ) # 将同一个表的字段合并

            f_table_name=table_name
            columns = []
            columns.append(f'{column_name}    {column_description}    {annotation}')

    table_columns=json.dumps(table_columns,indent=1, ensure_ascii=False)
    with open('../data/process/table_columns_formate.json','w') as f:
        f.write(table_columns)
#tabel_col_pro_formate()


def select_data(sql):
    import requests
    import json

    url = "https://comm.chatglm.cn/finglm2/api/query"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer 86776e8aaa3547719042e51b304ec811"
    }
    data = {
        "sql": f"{sql}",
        "limit": 10
    }

    response = requests.post(url, headers=headers, json=data)
    print(f'-----sql response:{response.text}')

    return response.json()['data']


# #sql = 'SELECT ChiName, AShareAbbr, LegalRepr, LegalConsultant, AccountingFirm, SecretaryBD FROM AStockBasicInfoDB.LC_StockArchives WHERE CompanyCode = \'600872\';'
#
# sql="select CompanyCode, ChiName, AShareAbbr, LegalRepr, LegalConsultant, AccountingFirm, SecretaryBD from AStockBasicInfoDB.LC_StockArchives"
#
# sql="select CompanyCode, ChiName, AShareAbbr, LegalRepr, LegalConsultant, AccountingFirm, SecretaryBD from AStockBasicInfoDB.LC_StockArchives where CompanyCode='600872'"
# sql="select * from ConstantDB.secumain  WHERE SecuCode=600872 limit 10"
# sql="SELECT ChiName, AShareAbbr, LegalRepr, LegalConsultant, AccountingFirm, SecretaryBD FROM AStockBasicInfoDB.LC_StockArchives WHERE CompanyCode = 1805"
# sql="""SELECT A.CompanyCode, A.ChiName, A.AShareAbbr, A.LegalRepr, A.LegalConsultant, A.AccountingFirm, A.SecretaryBD
# FROM AStockBasicInfoDB.LC_StockArchives A
# JOIN ConstantDB.SecuMain S ON A.CompanyCode = S.CompanyCode
# WHERE S.SecuCode = '600872';"""
#
# sql="""sql
# SELECT
#     A.ChiName AS '全称',
#     A.AShareAbbr AS 'A股简称',
#     A.LegalRepr AS '法人代表',
#     A.LegalConsultant AS '法律顾问',
#     A.AccountingFirm AS '会计师事务所',
#     A.SecretaryBD AS '董秘'
# FROM
#     AStockBasicInfoDB.LC_StockArchives AS A
# WHERE
#     A.AStockCode = '600872';"""
#
# sql = f"SHOW CREATE TABLE AStockBasicInfoDB.LC_StockArchives;"
# result = select_data(sql)
# create_table_sql = result[0]['Create Table']
#
# result=select_data(sql)
# print(result)