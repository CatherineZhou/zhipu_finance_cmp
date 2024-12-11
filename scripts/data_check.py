import pandas as pd
import requests
import json


def select_sql():
    url = "https://comm.chatglm.cn/finglm2/api/query"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer ce484b2272284b85a2f6fd1438687f1f"
    }
    data = {
        "sql": "desc astockfinancedb.lc_incomestatementall ",
        "limit": 10
    }

    response = requests.post(url, headers=headers, json=data)
    print(json.dumps(response.json(), indent=2, ensure_ascii=False))

#select_sql()


def select_data():
    import requests
    import json

    url = "https://comm.chatglm.cn/finglm2/api/query"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer 0609ed029da743a5aff206f83f596290"
    }
    data = {
        "sql": "SELECT * FROM astockshareholderdb.lc_mshareholder LIMIT 10",
        "limit": 10
    }

    response = requests.post(url, headers=headers, json=data)
    print(json.dumps(response.json(), indent=2, ensure_ascii=False))

#select_data()

# 提取json文件中的问题

def read_query():
    with open('../data/org_data/question.json','r') as f:
        content = f.read()

    json_data=json.loads(content)

    querys = []
    for json_string in json_data:
        tid=json_string['tid']
        team=json_string['team']
        for t in team:
            id= t['id']
            question = t['question']
            querys.append([tid, id, question])


    df_data = pd.DataFrame(querys, columns=['tid','id','question'])
    df_data.to_csv('../data/process/querys.csv',encoding='utf-8')


#read_query()

def trans_db_desc():
    """
    将db 的描述信息转为json格式存储
    :return:
    """
    df_data = pd.read_csv('../data/org_data/table_desc.csv')
    print(df_data.shape, df_data.head(3))
    json_str = df_data.to_json(orient='records', force_ascii=False,indent=1)
    print(type(json_str))
    with open('../data/org_data/db_desc.json','w') as f:
        f.write(json_str)

trans_db_desc()

"""
curl -X POST "https://comm.chatglm.cn/finglm2/api/query" -H "Content-Type: application/json" -H "Authorization: Bearer ce484b2272284b85a2f6fd1438687f1f" -d '{"sql": "SELECT * FROM constantdb.secumain LIMIT 10","limit": 10}'

"""