import re
import requests
import json
from zhipuai import ZhipuAI
from collections import Counter
from tqdm import tqdm
import time
import os
from utils.contain_col_names import way_string_2
from utils.utils_tools import find_value_in_list_of_dicts,find_json,process_dict,to_select,clean_text
from utils.llm_model import create_chat_completion
from utils.extract_entity import process_question
from utils.faiss_index import create_index,get_vector_search
#root_dir = '/data/keraszhou/pywork/zhipu_finance_llm_2024/kears/'
root_dir='../data/'
import pandas as pd

# In[27]:


# Preprocess the competition questions here
question_data_path = os.path.join(root_dir, 'org_data/question.json')
print(question_data_path)
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
for i in table_name_en[:1]:
    df3 = df2[df2['table_name'] == i]
    name = df1[df1['表英文'] == i]['库表名英文'].iloc[0]
    column_name = list(df3['column_name'])
    column_name_zh = list(df3['column_description'])
    print('---------before ',df3['注释'].values.tolist())
    df3.fillna('missing', inplace=True)
    print('\n\n\n---------after ',df3['注释'].values.tolist())
    column_name_2 = list(df3['注释'])  #column_name_2 = list(df3['注释'].dropna())
