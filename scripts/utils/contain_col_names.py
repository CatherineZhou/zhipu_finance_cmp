#coding=utf-8
# 基于问题中属性字段，筛选出可能需要使用的列名， 同时指定不同的属性可以使用的sql语句。

from utils.utils_tools import clean_text
from utils.llm_model import create_chat_completion
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

def find_dict_by_element(dict_list, target_element):
    """
    Given a list of dictionaries, return all dictionaries where  '列名中文描述' contains the target_element.
    Parameters:
        dict_list (list): A list of dictionaries, each expected to have '列名中文描述' key.
        target_element (str): The element to search for.

    Returns:
        list: A list of dictionaries that contain target_element in '列名中文描述'.
    """
    return [d for d in dict_list if target_element in d.get('列名中文描述', [])]


def find_dict_by_element_embedding(dict_list, target_element):
    """
    Given a list of dictionaries, return all dictionaries where  '列名中文描述' contains the target_element.
    Parameters:
        dict_list (list): A list of dictionaries, each expected to have '列名中文描述' key.
        target_element (str): The element to search for.

    Returns:
        list: A list of dictionaries that contain target_element in '列名中文描述'.
    """

    return [d for d in dict_list if target_element in d.get('列名中文描述', [])]


def to_get_question_columns(question,database_L_zh):
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

    L_num = []
    for items in database_L_zh:
        L_num += items['列名中文描述']

    # Get unique column descriptions
    L_num_new = [item for item, count in Counter(L_num).items() if count == 1]

    # Drop NaN if any
    series_num = pd.Series(L_num_new)
    L_num_new = list(series_num.dropna())

    # Remove known irrelevant items
    irrelevant_items = ['年度', '占比']
    for irr in irrelevant_items:
        if irr in L_num_new:
            L_num_new.remove(irr)

    matched_columns = []
    for col_desc in L_num_new:
        # Check if the column description or its cleaned version appears in the question
        if col_desc in question or clean_text(col_desc) in question:
            L_dict = find_dict_by_element(database_L_zh, col_desc)
            if not L_dict:
                continue
            # Create a mapping from Chinese description to English column name
            dict_zip = dict(zip(L_dict[0]['列名中文描述'], L_dict[0]['列名']))
            column_name = dict_zip[col_desc]
            data_table = L_dict[0]['数据表名']

            matched_columns.append({
                '数据库表': data_table,
                '列名': column_name,
                '列名中文含义': col_desc
            })

    if matched_columns:
        return f"已获得一部分数据库列名{matched_columns}，请充分利用获得的列名直接查询数据。"
    else:
        return ''


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
    print('------提取的实体词',result)
    return result



def find_keys_by_value(dictionary, target_value):
    keys = []
    for key, value in dictionary.items():
        if value == target_value:
            keys.append([key,target_value])
    return keys


from utils.faiss_index import create_index,get_vector_search
import ast
def to_get_question_columns_embeddings(question,col_names_index,col_names_zh_list,col_names_dic):
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
            #print(f'{a} 相似字段：{col_names_cos}')
            for sim_item in col_names_cos:
                sim_col_name = sim_item.split(':')[0]
                tables_name=find_keys_by_value(col_names_dic,sim_col_name)
                matched_columns.extend(tables_name)
                #print(f'{sim_col_name} ## tabel :{tables_name}\n\n')

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

    #matched_columns_pro = json.dumps(matched_columns_pro,indent=1, ensure_ascii=False)

    if matched_columns:
        return f"已获得一部分数据库列名{matched_columns_pro}，请充分利用获得的列名直接查询数据。"
    else:
        return ''




def way_string_2(question,database_L_zh,col_names_index=None,col_names_zh_list=None,col_names_dic=None):
    """
    基于给定的问题和表的列筛选出可能要使用的列名。 同时针对一些列名进行单独处理。例如： 最新高，近一个月最高价。
    :param question:
    :return:
    """
    #way_string_2 = to_get_question_columns(question,database_L_zh)  # 筛选出可能需要使用的列名。返回的 messages，包含表，列名
    way_string_2 = to_get_question_columns_embeddings(question,col_names_index,col_names_zh_list,col_names_dic)
    way_string_2 += ">>查询参考："

    # 特殊逻辑处理，
    if "近一个月最高价" in question:
        way_string_2 += "查询近一个月最高价,你写的sql语句可以优先考虑表中已有字段HighPriceRM  近一月最高价(元)  "
    if "近一个月最低价" in question:
        way_string_2 += "查询近一月最低价(元),你写的sql语句直接调用已有字段LowPriceRM"
    if "行业" in question and ('多少只' in question or '几个' in question or '多少个' in question):
        way_string_2 += """查询某行业某年数量 示例sql语句:SELECT count(*) as 风电零部件_2021
            FROM AStockIndustryDB.LC_ExgIndustry
            where ThirdIndustryName like '%风电零部件%' and year(InfoPublDate)=2021 and IfPerformed = 1;"""
    if ('年度报告' in question and '最新更新' in question) or '比例合计' in question:
        way_string_2 += """特别重要一定注意，查询最新更新XXXX年年度报告，机构持有无限售流通A股数量合计InstitutionsHoldProp最多公司代码，优先使用查询sql语句，SELECT *
                            FROM AStockShareholderDB.LC_StockHoldingSt
                            WHERE date(EndDate) = 'XXXX-12-31'
                              AND UpdateTime = (
                                SELECT MAX(UpdateTime)
                                FROM AStockShareholderDB.LC_StockHoldingSt
                                WHERE date(EndDate) = 'XXXX-12-31'
                              ) order by InstitutionsHoldings desc limit 1 ，XXXX代表问题查询年度，sql语句禁止出现group by InnerCode;

                              查询最新更新XXXX年年度报告,公司机构持有无限售流通A股比例合计InstitutionsHoldProp是多少,优先使用查询sql语句，SELECT InstitutionsHoldProp
                            FROM AStockShareholderDB.LC_StockHoldingSt
                            WHERE date(EndDate) = 'XXXX-12-31'
                              AND UpdateTime = (
                                SELECT MAX(UpdateTime)
                                FROM AStockShareholderDB.LC_StockHoldingSt
                                WHERE date(EndDate) = 'XXXX-12-31'
                              ) order by InstitutionsHoldings desc limit 1 ，XXXX代表问题查询年度，sql语句禁止出现group by InnerCode;"""

    if '新高' in question:
        way_string_2 += """新高 要用AStockMarketQuotesDB.CS_StockPatterns现有字段

        查询今天是2021年01月01日，创近半年新高的股票有几只示。示例sql语句:SELECT count(*)  FROM AStockMarketQuotesDB.CS_StockPatterns
                where  IfHighestHPriceRMSix=1 and date(TradingDay)='2021-01-01;
                判断某日 YY-MM-DD  InnerCode XXXXXX 是否创近一周的新高，查询结果1代表是,IfHighestHPriceRW字段可以根据情况灵活调整  SELECT   InnerCode,TradingDay,IfHighestHPriceRW  FROM AStockMarketQuotesDB.CS_StockPatterns
where  date(TradingDay)='2021-12-20' and InnerCode = '311490'

                """
    if '成交额' in question and '平均' in question:
        way_string_2 += """查询这家公司5日内平均成交额是多少。示例sql语句:SELECT count(*)  FROM AStockMarketQuotesDB.CS_StockPatterns
                where  IfHighestHPriceRMSix=1 and date(TradingDay)='2021-01-01"""
    if '半年度报告' in question:
        way_string_2 += """查询XXXX年半年度报告的条件为：year(EndDate) = XXXX and InfoSource='半年度报告'"""

    return way_string_2

    if '新高' in question:
        way_string_2 += """查询今天是2021年01月01日，创近半年新高的股票有几只示。示例sql语句:SELECT count(*)  FROM AStockMarketQuotesDB.CS_StockPatterns
                where  IfHighestHPriceRMSix=1 and date(TradingDay)='2021-01-01"""
    if '成交额' in question and '平均' in question:
        way_string_2 += """查询这家公司5日内平均成交额是多少。示例sql语句:SELECT count(*)  FROM AStockMarketQuotesDB.CS_StockPatterns
                where  IfHighestHPriceRMSix=1 and date(TradingDay)='2021-01-01"""

    return way_string_2


