#coding=utf-8

"""
将json数据转为csv文件，查看前30个效果。
"""

import json
import pandas as pd


"""
  {
    "tid": "tttt----1",
    "team": [
      {
        "id": "tttt----1----1-1-1",
        "question": "600872的全称、A股简称、法人、法律顾问、会计师事务所及董秘是？",
        "answer": "全称 中炬高新技术实业(集团)股份有限公司, A股简称 中炬高新, 法人 余健华, 法律顾问 广东卓建(中山)律师事务所, 会计师事务所 天职国际会计师事务所（特殊普通合伙）, 董秘 郭毅航"
      },
      {
        "id": "tttt----1----1-1-2",
        "question": "该公司实控人是否发生改变？如果发生变化，什么时候变成了谁？是哪国人？是否有永久境外居留权？（回答时间用XXXX-XX-XX）",
        "answer": "实控人是否发生改变 否, 变更时间 None, 当前实控人 姚振华, 实控人国籍 中国, 是否拥有永久境外居留权 None"
      },
      {
        "id": "tttt----1----1-1-3",
        "question": "在实控人发生变化的当年股权发生了几次转让？",
        "answer": "公司代码 600872, 公司名称 中炬高新技术实业(集团)股份有限公司, 实控人变化年份 2019, 当年股权转让次数 0"
      }
    ]
  },
"""

def read_query():
    with open('../data/process/result.json','r') as f:
        content = f.read()

    json_data=json.loads(content)

    querys = []
    for json_string in json_data:
        tid=json_string['tid']
        team=json_string['team']
        for t in team:
            id= t['id']
            question = t['question']
            answer=t['answer']
            querys.append([tid, id, question,answer])


    df_data = pd.DataFrame(querys, columns=['tid','id','question','answer'])
    df_data.to_csv('../data/process/query_eval.csv',encoding='utf-8',index=False)

read_query()