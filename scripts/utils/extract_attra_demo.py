import json
import os
import pandas as pd
import ast
from utils.utils_tools import clean_text

import pandas as pd
import jieba
import re
import requests
import json
from zhipuai import ZhipuAI
from collections import Counter
from tqdm import tqdm
import time
import os

from utils.faiss_index import create_index,get_vector_search
from utils.llm_model import create_chat_completion

root_dir='../data/'

# Preprocess the competition questions here
question_data_path = os.path.join(root_dir, 'org_data/question.json')
df1 = pd.read_excel(os.path.join(root_dir,'org_data/data_dictionary.xlsx'), sheet_name='库表关系')
df2 = pd.read_excel(os.path.join(root_dir,'org_data/data_dictionary.xlsx'), sheet_name='表字段信息')
file_path = os.path.join(root_dir,'org_data/all_tables_schema.txt')



df1['库表名英文'] = df1['库名英文'] + '.' + df1['表英文']
df1['库表名中文'] = df1['库名中文'] + '.' + df1['表中文']

database_name = list(df1['库名中文'])
table_name = list(df1['表中文'])
table_name_en = list(df1['表英文'])
database_table_ch = list(df1['库表名中文'])
database_table_en = list(df1['库表名英文'])
database_table_en_zs = {'库表名': database_table_en, '对应中文注释说明': table_name}
database_table_map = df1.set_index('库表名中文')['库表名英文'].to_dict()

database_L = []
database_L_zh = []
for i in table_name_en:
    df3 = df2[df2['table_name'] == i]
    name = df1[df1['表英文'] == i]['库表名英文'].iloc[0]
    column_name = list(df3['column_name'])
    column_name_zh = list(df3['column_description'])
    column_name_2 = list(df3['注释'])

    dict_1 = {'数据表名': name, '列名': column_name, '注释': column_name_2}
    dict_2 = {'数据表名': name, '列名': column_name, '列名中文描述': column_name_zh, '注释': column_name_2}
    database_L.append(dict_1)
    database_L_zh.append(dict_2)



col_names_dic={}
col_names_comment_dic = {}
for col_desc in database_L_zh:
    data_table = col_desc['数据表名']
    column_name_en = col_desc['列名']
    column_name_zh = col_desc['列名中文描述']
    column_name_2 = col_desc['注释']

    for i, col_name in enumerate(column_name_zh):
        col_names_dic[f'{data_table}.{column_name_en[i]}']=col_name
        col_comment=column_name_zh[i] + '' if column_name_2[i] is None else str(column_name_2[i])
        col_names_comment_dic[f'{data_table}.{column_name_en[i]}']=col_comment

col_names_zh_list = list(col_names_dic.values())

col_names_comment_list = list(col_names_comment_dic.values())
#col_names_comment_list=[f'{str(i)}##{v}' for i ,v in enumerate(col_names_comment_list)]


col_names_index=create_index(col_names_zh_list)
#col_names_comment_index=create_index(col_names_comment_list)


# 提取问题中可能包含的属性
def get_attra_entity(question):
    # 帮我提取“这支基金20年最后一次分红派现比例多少钱,保留2位小数？”涉及金融实体术语，不要包含一些太宽泛、太大粒度的实体，如果包含多个实体，则通过列表形式返回。如：['**','**',,]
    template_prompt = """帮我提取“<<question>>”涉及金融实体术语，不要包含一些太宽泛、太大粒度的实体，如果包含多个实体，则通过列表形式返回。如：['**','**'...]。为了方便你理解，如下提供了一些示例。


    <示例-START>
    Query:这只股票最近一次的股息率是多少，精确到小数点后两位？
    Answer:['股息率']

    Query:该债券的票面利率和到期收益率分别是多少？
    Answer:['票面利率', '到期收益率']

    Query这个月的沪深300指数的市盈率和市净率分别是多少？
    Answer:['市盈率', '市净率']
    <示例-END>

    注意：你只需要按规范要求输出实体，不要解释。

    """

    template_prompt = """帮我提取“<<question>>”涉及核心关键词，不要包含一些太宽泛、太大粒度的关键词，如果包含多个实体，则通过列表形式返回。如：['**','**'...]。为了方便你理解，如下提供了一些示例。


    <示例-START>
    Query:这只股票最近一次的股息率是多少，精确到小数点后两位？
    Answer:['股息率']

    Query:该债券的票面利率和到期收益率分别是多少？
    Answer:['票面利率', '到期收益率']

    Query这个月的沪深300指数的市盈率和市净率分别是多少？
    Answer:['市盈率', '市净率']
    <示例-END>

    注意：你只需要按规范要求输出实体，不要解释。

    """

    messages = []
    #question = "600872的全称、A股简称、法人、法律顾问、会计师事务所及董秘是？"
    input_text = template_prompt.replace('<<question>>', question)
    messages.append({"role": "user", "content": input_text})
    result = create_chat_completion(messages)
    return result



def find_keys_by_value(dictionary, target_value):
    keys = []
    for key, value in dictionary.items():
        if value == target_value:
            keys.append([key,target_value])
    return keys

def to_get_question_columns_embeddings(question):
    """
    基于给定的问题，从database_L_zh 表以及列名描述中筛选出可能需要使用的列名。
    这里使用的是关键词硬匹配。可能效果会不太好。
    Given a question (string) and a global variable database_L_zh (list of dicts),
    find 列名 that correspond to 列名中文描述 mentioned in the question.

    If any matching columns are found, return a message instructing the user to
    use these column names directly for data querying. If none are found, return an empty string.

    Parameters:
        question (str): The input question text.

    Returns:
        str: A message with identified column names or an empty string if none found.
    """

    """
    dict_2 = {'数据表名': name, '列名': column_name, '列名中文描述': column_name_zh, '注释': column_name_2}
    database_L.append(dict_1)
    database_L_zh.append(dict_2)
    """
    matched_columns = []
    matched_columns_pro=[]
    real_list = ast.literal_eval(get_attra_entity(question))

    if real_list:
        for a in real_list:
            col_names_cos=get_vector_search(col_names_index,col_names_zh_list, a, k=5, cut_off=0.85)
            col_names_cos = list(set(col_names_cos))
            print(f'{a} 相似字段：{col_names_cos}')
            for sim_item in col_names_cos:
                sim_col_name = sim_item.split(':')[0]
                tables_name=find_keys_by_value(col_names_dic,sim_col_name)
                matched_columns.extend(tables_name)
                print(f'{sim_col_name} ## tabel :{tables_name}\n\n')

    for table_info in matched_columns:
        col_desc=table_info[1]
        data_table=table_info[0].split('.')
        data_db='.'.join(data_table[:2])
        column_name =data_table[2]

        matched_columns_pro.append({
            '数据库表': data_db,
            '列名': column_name,
            '列名中文含义': col_desc
        })

    matched_columns_pro = json.dumps(matched_columns_pro,indent=1, ensure_ascii=False)

    print(f'#############\n\n\n{question}: ##{matched_columns_pro}##')  #

    return matched_columns_pro