#coding=utf-8#!/usr/bin/env python
# coding: utf-8

# ## 初始化
#
# 在这一步，你需要导入所有必要的库，并对项目中的部分参数进行初始化。包括填写 智谱AI API Key 以及一个队伍 Token。


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
from utils.contain_col_names import way_string_2
from utils.utils_tools import find_value_in_list_of_dicts,find_json,process_dict,to_select,clean_text
from utils.llm_model import create_chat_completion
from utils.extract_entity import process_question
from utils.faiss_index import create_index,get_vector_search
#root_dir = '/data/keraszhou/pywork/zhipu_finance_llm_2024/kears/'
root_dir='../data/'

from datetime import datetime

# 获取当前时间
now = datetime.now()

# 将当前时间转换为字符串形式
time_str = now.strftime("%Y-%m-%dT%H-%M")


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
for i in table_name_en:
    df3 = df2[df2['table_name'] == i]
    name = df1[df1['表英文'] == i]['库表名英文'].iloc[0]
    column_name = list(df3['column_name'])
    column_name_zh = list(df3['column_description'])
    df3.fillna('missing', inplace=True)
    column_name_2 = list(df3['注释'])  #column_name_2 = list(df3['注释'].dropna())


    dict_1 = {'数据表名': name, '列名': column_name, '注释': column_name_2}
    dict_2 = {'数据表名': name, '列名': column_name, '列名中文描述': column_name_zh, '注释': column_name_2}
    database_L.append(dict_1)
    database_L_zh.append(dict_2)


# 创建检索索引
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



def filter_table_comments(question, table_comments):
    """
    Filter a list of table comments based on the given question.
    Uses jieba for segmentation and removes stopwords, returning only comments
    that contain at least one of the segmented keywords.

    Parameters:
        question (str): The question text.
        table_comments (list): A list of comment strings to filter.

    Returns:
        filtered_comments (list): Filtered list of comments.
    """
    stopwords = ['？', '有', '的', '多少', '人', '（', '）']
    seg_list = list(jieba.cut(question, cut_all=False))
    filtered_seg_list = [word for word in seg_list if word not in stopwords]

    filtered_comments = []
    for comment in table_comments:
        if any(keyword in comment for keyword in filtered_seg_list):
            filtered_comments.append(comment)
    return filtered_comments


with open(file_path, 'r', encoding='utf-8') as file:
    content = file.read()
input_text = content


def parse_table_structures(input_text):
    """
    Parse the input text to extract table structures.

    The format is expected as pairs: "table_name === table_structure".

    Parameters:
        input_text (str): The raw text containing table structures.

    Returns:
        tables_dict (dict): A dictionary where keys are table names and
                            values are the associated table structures.
    """
    tables_text = input_text.split('===')[1:]
    tables_dict = {tables_text[i]: tables_text[i + 1] for i in range(0, len(tables_text), 2)}
    return tables_dict


def map_chinese_to_english_tables(chinese_names, english_names):
    """
    Map Chinese table names to their corresponding English table names.
    For each Chinese name, there is a matching English name
    (case-insensitive comparison).

    Parameters:
        chinese_names (list): A list of Chinese table names.
        english_names (list): A list of English table names.

    Returns:
        name_map (dict): A dictionary mapping Chinese table names to English table names.
    """
    name_map = {}
    for cname in chinese_names:
        # Find the corresponding English name (case-insensitive match)
        english_match = [en for en in english_names if str(en).lower() == cname.lower()][0]
        name_map[cname] = english_match
    return name_map




def get_table_schema(question=''):
    """
    Retrieve table schemas along with optional filtered field comments.
    If a question is provided, the comments will be filtered based on
    question keywords.

    The function:
      1. Maps Chinese table names to English table names.
      2. For each table, retrieves its structure and finds associated comments.
      3. If a question is provided, filter the comments based on keywords extracted from the question.

    Parameters:
        question (str): The question text. If empty, no filtering is performed.

    Returns:
        table_maps (list): A list of dictionaries, each containing table schema information.
        {
            '数据表名': EnglishTableName,
            '数据表结构': TableStructure,
            '字段注释': FilteredComments (optional if question is provided)
        }
    """
    #print(f'-----input_text:{input_text}' )
    parsed_tables = parse_table_structures(input_text)

    # Clean up keys and values
    cleaned_tables = {
        k.replace(' ', '').replace('表结构', ''): v.replace('--', '')
        for k, v in parsed_tables.items()
    }

    # List of Chinese table names (keys)
    chinese_table_names = list(cleaned_tables.keys())

    name_map = map_chinese_to_english_tables(chinese_table_names, database_table_en)

    table_maps = []
    for cname, structure in cleaned_tables.items():
        english_name = name_map.get(cname)
        comments = find_value_in_list_of_dicts(database_L, '数据表名', english_name, '注释')

        if question == '':
            # No filtering, just return table name and structure
            table_map = {
                '数据表名': english_name,
                '数据表结构': structure
            }
        else:
            # Filter comments based on question
            filtered_comments = filter_table_comments(question, comments)
            table_map = {
                '数据表名': english_name,
                '数据表结构': structure,
                '字段注释': filtered_comments
            }

        table_maps.append(table_map)

    return table_maps





def run_conversation(question):
    """
    Run a conversation flow given a question by:
      1. Using run_conversation_xietong(question) to get an answer.
      2. Attempting to find and parse JSON from the answer using find_json.
      3. Converting the parsed result (or original text if parsing fails) into a descriptive sentence using process_dict.

    Parameters:
        question (str): The question to ask.

    Returns:
        str: The final processed answer as a descriptive string.
    """
    last_answer = run_conversation_xietong(question) #
    parsed_data = find_json(last_answer)
    final_string = process_dict(parsed_data)
    return final_string




# ## sql优化
#
# 本方案中需要对模型生成呢的SQL语句进行优化。我们对由模型生成的 SQL 语句进行一个小的优化步骤，以使其在查询接口中能够正确执行。主要的优化措施包括：
#
# 1. 日期字段格式转换：函数 replace_date_with_day 会将形如 TradingDate = 'YYYY-MM-DD' 的条件自动转化为 date(TradingDate) = 'YYYY-MM-DD' 的格式。这样可以确保在特定查询引擎或数据库中根据日期进行正确的查询过滤。
# 2. SQL语句提取: 函数 extract_sql 会从给定的文本中提取出被 sql ...  包围的 SQL 代码片段，从而从较复杂的文本中获得纯净的 SQL 语句。
# 3. 接口查询执行：将优化后的 SQL 语句通过 select_data 函数发送到指定的 API 接口进行查询，并以 JSON 格式返回结果。

# In[40]:

def output_result(result, info_list):
    """
    Append the formatted JSON 'data' from the result into 'info_list'.

    Parameters:
        result (dict): The query result containing 'data'.
        info_list (list): The list to which formatted data will be appended.

    Returns:
        list: The updated info_list with the new data appended, if any.
    """
    if 'data' in result and len(result['data']) > 0:
        info_list.append(json.dumps(result['data'], ensure_ascii=False, indent=1) + '\n')
    return info_list



def run_conversation_until_complete(messages, max_rounds=6):
    """
    Test function to run a conversation loop until the assistant indicates completion.
    """
    last_response = None  # 用于存储最后一次对话的响应
    round_count = 0  # 对话轮数计数器
    response = create_chat_completion(messages)
    while True:
        if round_count >= max_rounds:
            break  # 如果对话轮数超过最大值，则退出循环

        question = response
        select_result = to_select(question) # 基于sql语句进行查询
        messages.append({"role": "assistant", "content": question})
        messages.append({"role": "user", "content": str(select_result)})

        response = create_chat_completion(messages)

        last_response = response  # 存储最后一次响应
        if "全部完成" in response:
            messages.append({"role": "assistant", "content": last_response}) # 记录中间结果
            break  # 如果检测到“回答完成”，则停止循环
        round_count += 1  # 增加对话轮数计数
    return last_response,messages  # 返回最后一次对话的内容


def run_conversation_xietong(question):
    """
    整个流程：
    1. fact_1: 问题中涉及的基金名， 公司名， code 对应的完整信息，方便后面where条件查询。（这里会包含表名或者字段）
    2. way_string_2: 问题中的涉及的属性可能需要用到的表名或者字段名，以及针对一些特殊字段可能需要查询的sql进行特殊说明。
    3. 拼装提示词： 将表英文名和中文名带入提示词， 模型如何一次完成则输出答案。 如果不能则进行输出可能需要使用的表。
    4. 如果步骤4没有完成，则进行对话聊天，多次对话完成最后sql输出。
    :param question:
    :return:
    """

    content_p_1 = """我有如下数据库表{'库表名': ['AStockBasicInfoDB.LC_StockArchives',
  'AStockBasicInfoDB.LC_NameChange',
  'AStockBasicInfoDB.LC_Business',
  'AStockIndustryDB.LC_ExgIndustry',
  'AStockIndustryDB.LC_ExgIndChange',
  'AStockIndustryDB.LC_IndustryValuation',
  'AStockIndustryDB.LC_IndFinIndicators',
  'AStockIndustryDB.LC_COConcept',
  'AStockIndustryDB.LC_ConceptList',
  'AStockOperationsDB.LC_SuppCustDetail',
  'AStockShareholderDB.LC_SHTypeClassifi',
  'AStockShareholderDB.LC_MainSHListNew',
  'AStockShareholderDB.LC_SHNumber',
  'AStockShareholderDB.LC_Mshareholder',
  'AStockShareholderDB.LC_ActualController',
  'AStockShareholderDB.LC_ShareStru',
  'AStockShareholderDB.LC_StockHoldingSt',
  'AStockShareholderDB.LC_ShareTransfer',
  'AStockShareholderDB.LC_ShareFP',
  'AStockShareholderDB.LC_ShareFPSta',
  'AStockShareholderDB.LC_Buyback',
  'AStockShareholderDB.LC_BuybackAttach',
  'AStockShareholderDB.LC_LegalDistribution',
  'AStockShareholderDB.LC_NationalStockHoldSt',
  'AStockShareholderDB.CS_ForeignHoldingSt',
  'AStockFinanceDB.LC_AShareSeasonedNewIssue',
  'AStockFinanceDB.LC_ASharePlacement',
  'AStockFinanceDB.LC_Dividend',
  'AStockFinanceDB.LC_CapitalInvest',
  'AStockMarketQuotesDB.CS_StockCapFlowIndex',
  'AStockMarketQuotesDB.CS_TurnoverVolTecIndex',
  'AStockMarketQuotesDB.CS_StockPatterns',
  'AStockMarketQuotesDB.QT_DailyQuote',
  'AStockMarketQuotesDB.QT_StockPerformance',
  'AStockMarketQuotesDB.LC_SuspendResumption',
  'AStockFinanceDB.LC_BalanceSheetAll',
  'AStockFinanceDB.LC_IncomeStatementAll',
  'AStockFinanceDB.LC_CashFlowStatementAll',
  'AStockFinanceDB.LC_IntAssetsDetail',
  'AStockFinanceDB.LC_MainOperIncome',
  'AStockFinanceDB.LC_OperatingStatus',
  'AStockFinanceDB.LC_AuditOpinion',
  'AStockOperationsDB.LC_Staff',
  'AStockOperationsDB.LC_RewardStat',
  'AStockEventsDB.LC_Warrant',
  'AStockEventsDB.LC_Credit',
  'AStockEventsDB.LC_SuitArbitration',
  'AStockEventsDB.LC_EntrustInv',
  'AStockEventsDB.LC_Regroup',
  'AStockEventsDB.LC_MajorContract',
  'AStockEventsDB.LC_InvestorRa',
  'AStockEventsDB.LC_InvestorDetail',
  'AStockShareholderDB.LC_ESOP',
  'AStockShareholderDB.LC_ESOPSummary',
  'AStockShareholderDB.LC_TransferPlan',
  'AStockShareholderDB.LC_SMAttendInfo',
  'HKStockDB.HK_EmployeeChange',
  'HKStockDB.HK_StockArchives',
  'HKStockDB.CS_HKStockPerformance',
  'USStockDB.US_CompanyInfo',
  'USStockDB.US_DailyQuote',
  'PublicFundDB.MF_FundArchives',
  'PublicFundDB.MF_FundProdName',
  'PublicFundDB.MF_InvestAdvisorOutline',
  'PublicFundDB.MF_Dividend',
  'CreditDB.LC_ViolatiParty',
  'IndexDB.LC_IndexBasicInfo',
  'IndexDB.LC_IndexComponent',
  'InstitutionDB.LC_InstiArchive',
  'ConstantDB.SecuMain',
  'ConstantDB.HK_SecuMain',
  'ConstantDB.CT_SystemConst',
  'ConstantDB.QT_TradingDayNew',
  'ConstantDB.LC_AreaCode',
  'InstitutionDB.PS_EventStru',
  'ConstantDB.US_SecuMain',
  'InstitutionDB.PS_NewsSecurity'],
 '对应中文注释说明': ['公司概况',
  '公司名称更改状况',
  '公司经营范围与行业变更',
  '公司行业划分表',
  '公司行业变更表',
  '行业估值指标',
  '行业财务指标表',
  '概念所属公司表',
  '概念板块常量表',
  '公司供应商与客户',
  '股东类型分类表',
  '股东名单(新)',
  '股东户数',
  '大股东介绍',
  '公司实际控制人',
  '公司股本结构变动',
  '股东持股统计',
  '股东股权变动',
  '股东股权冻结和质押',
  '股东股权冻结和质押统计',
  '股份回购',
  '股份回购关联表',
  '法人配售与战略投资者',
  'A股国家队持股统计',
  '外资持股统计',
  'A股增发',
  'A股配股',
  '公司分红',
  '资金投向说明',
  '境内股票交易资金流向指标',
  '境内股票成交量技术指标',
  '股票技术形态表',
  '日行情表',
  '股票行情表现(新)',
  '停牌复牌表',
  '资产负债表_新会计准则',
  '利润分配表_新会计准则',
  '现金流量表_新会计准则',
  '公司研发投入与产出',
  '公司主营业务构成',
  '公司经营情况述评',
  '公司历年审计意见',
  '公司职工构成',
  '公司管理层报酬统计',
  '公司担保明细',
  '公司借贷明细',
  '公司诉讼仲裁明细',
  '重大事项委托理财',
  '公司资产重组明细',
  '公司重大经营合同明细',
  '投资者关系活动',
  '投资者关系活动调研明细',
  '员工持股计划',
  '员工持股计划概况',
  '股东增减持计划表',
  '股东大会出席信息',
  '港股公司员工数量变动表',
  '港股公司概况',
  '港股行情表现',
  '美股公司概况',
  '美股日行情',
  '公募基金概况',
  '公募基金产品名称',
  '公募基金管理人概况',
  '公募基金分红',
  '违规当事人处罚',
  '指数基本情况',
  '指数成份',
  '机构基本资料',
  '证券主表,包含字段InnerCode,CompanyCode,SecuCode,ChiName,ChiNameAbbr 代表中文名称缩写,EngName,EngNameAbbr,SecuAbbr 代表 证券简称,ListedDate',
  '港股证券主表，包含字段InnerCode,CompanyCode,SecuCode,ChiName,ChiNameAbbr 代表中文名称缩写,EngName,EngNameAbbr,SecuAbbr 代表 证券简称,ListedDate',
  '系统常量表',
  '交易日表(新)',
  '国家城市代码表',
  '事件体系指引表',
  '美股证券主表',
  '证券舆情表']}
已查询获得事实：<<fact_1>>
我想回答问题
"<<question>>"

如果已查询获得事实可以直接总结答案，需要是明确的答案数据不是需要查询数据库表，记得提示我：<全部完成，答案如下>,将答案总结以json格式给我。
如果不能直接总结答案，需要查询的数据库表,请从上面数据库表中筛选出还需要哪些数据库表，记得提示我：<需要查询的数据库表>,只返回需要数据列表,不要回答其他内容。"""

    content_p = content_p_1.replace('<<question>>', str(question)).replace('<<fact_1>>',
                                                                           str(process_question(question))) # process_question 提取问题中的包含的公司名，基金名， code完整中文名，或者code
    content_p = content_p + way_string_2(question,database_L_zh,col_names_index,col_names_zh_list,col_names_dic) # 筛选出问题中可能涉及的属性对应的列名以及表名。。
    # 执行函数部分
    messages = []
    messages.append({"role": "user", "content": "您好阿"})
    messages.append({"role": "user", "content": content_p})
    response = create_chat_completion(messages) # 开始写sql。
    print(f'------##【获取需要使用的表结构】:##【问题】:{question}\n##:content_p:{content_p}##\nresponse:{response}')
    if "全部完成" in response:
        return response  # 如果检测到“回答完成”，则是一步可以回答问题。
    messages.append({"role": "assistant", "content": response})


    content_p_2 = """获取的表结构如下<list>,表结构中列名可以引用使用,表结构中数据示例只是参考不能引用。
    我们现在开始查询当前问题，请你分步写出查询sql语句，我把查询结果告诉你，你再告诉我下一步，
    注意如果我返回的结果为空或者错误影响下一步调用，请重新告诉我sql语句。
    等你全部回答完成，不需要进行下一步调用时，记得提示我：<全部完成，答案如下>,将答案总结以json格式给我，只需要总结当前问题。
    查询技巧:sql查询年度时优先使用year()函数。sql查询语句不需要注释，不然会报错。sql中日期条件格式应参考这样date(TradingDay) = 'YYYY-MM-DD'。尽量利用表格中已有的字段。"""

    table_maps = get_table_schema(question) # 这里用来解析all_tables_schema.txt中的内容
    LL = [i for i in table_maps if i.get('数据表名') in response] # 这里获得表的结构描述
    content_p_2 = content_p_2.replace('<list>', str(LL)) + way_string_2(question,database_L_zh,col_names_index,col_names_zh_list,col_names_dic)
    messages.append({"role": "user", "content": content_p_2})  ###开始对话
    print(f'------##【生成sql的提示词】:##【问题】:{question}\n##\nmessages:{messages}')
    last_answer,mid_messages = run_conversation_until_complete(messages, max_rounds=9)
    print('##########################################################')
    print(f'----------question:{question},##message:{mid_messages} ##')
    print('##########################################################')
    return last_answer


# In[33]:



# ## 运行脚本解决问题
# 这里展现了对单个问题的完整流程。主程序将会遍历这个过程，直到完成所有问题。

# In[44]:

# 获取答案
def get_answer(question):
    """
    Attempt to answer the given question by interacting with the
    conversation model. If an error occurs, return a default error message.

    Parameters:
        question (str): The question that needs an answer.

    Returns:
        str: The answer string or an error message if an exception occurs.
    """
    try:
        print(f"Attempting to answer the question: {question}")
        last_answer = run_conversation(question)
        return last_answer
    except Exception as e:
        print(f"Error occurred while executing get_answer: {e}")
        return "An error occurred while retrieving the answer."


# 问题改写
def question_rew(context_text, original_question):
    """
    Rewrite the given question to be clearer and more specific based on the provided context,
    without altering the original meaning or omitting any information.

    Parameters:
        context_text (str): The context text that the question is based on.
        original_question (str): The question to be rewritten.

    Returns:
        str: The rewritten question.
    """
    # prompt = (
    #     f"根据这些内容'{context_text}',帮我重写这个问题”'{original_question}' ,让问题清晰明确，"
    #     "不改变原意，不要遗漏信息，特别是时间，只返回问题。\n"
    #     "以下是示例：\n"
    #     "user:根据这些内容'最新更新的2021年度报告中，机构持有无限售流通A股数量合计最多的公司简称是？  公司简称 帝尔激光',"
    #     "帮我重写这个问题'在这份报告中，该公司机构持有无限售流通A股比例合计是多少，保留2位小数？' ,让问题清晰明确，不改变原意，不要遗漏信息，特别是时间，"
    #     "只返回问题。\n"
    #     "assistant:最新更新的2021年度报告中,公司简称 帝尔激光 持有无限售流通A股比例合计是多少，保留2位小数？"
    # )

    prompt = f"""你的任务是问题改写，结合给定上文，改写用户问题。让问题清晰明确，不改变原意，不要遗漏信息，特别是时间，只返回问题。
    以下是示例，方便你理解：

    系统提供上文：最新更新的2021年度报告中，机构持有无限售流通A股数量合计最多的公司简称是？  公司简称 帝尔激光'
    用户问题：在这份报告中，该公司机构持有无限售流通A股比例合计是多少，保留2位小数？' 
    你改写后的问题:最新更新的2021年度报告中,公司简称 帝尔激光 持有无限售流通A股比例合计是多少，保留2位小数？

    --------------
    现在开始你的任务
    系统提供上文：{context_text}
    用户问题：{original_question}
    你改写后的问题：
    """

    messages = [{"role": "user", "content": prompt}]
    response = create_chat_completion(messages)
    print(f'---------改写后的问题：{response}')
    return response


def main_answer(q_json_list, start_index=0, end_index=None):
    """
    Process a portion of a list of JSON objects, each containing a 'tid' and 'team'
    where 'team' is a list of questions.

    For each JSON object in the specified range:
      1. Extract all questions from 'team'.
      2. If no previous Q&A history, use the question directly. Otherwise,
         rewrite the question based on all previously answered content.
      3. Get the answer using get_answer and store it.
      4. Update the original structure with the answers.

    Parameters:
        q_json_list (list): List of data objects, each containing keys 'tid' and 'team'.
        start_index (int): The starting index of the list subset to process.
        end_index (int): The ending index (non-inclusive) of the list subset.
                         If None, process until the end of q_json_list.

    Returns:
        list: A list of processed dictionaries with updated answers.
    """
    if end_index is None or end_index > len(q_json_list):
        end_index = len(q_json_list)

    data_list_result = []
    for i in tqdm(range(start_index, end_index), desc="Processing JSON data in range"):
        item = q_json_list[i]
        start_time = time.time()

        # Extract questions
        questions_list = [(member["id"], member["question"]) for member in item["team"]]
        answers_dict = {}
        all_previous = ''

        # Iterate over all questions in the current item
        for question_id, question_text in questions_list:
            if all_previous == '':
                rewritten_question = question_text
            else:
                rewritten_question = question_rew(all_previous, question_text) # 问题改写

            answer = get_answer(rewritten_question) # 这里开始作答
            print(f'----------answer:{answer}')
            answers_dict[question_id] = answer
            all_previous += question_text + answer

        # Update original item with answers
        for member in item["team"]:
            member["answer"] = answers_dict.get(member["id"], "无答案")

        updated_data = {"tid": item["tid"], "team": item["team"]}
        data_list_result.append(updated_data)

        elapsed_time = time.time() - start_time
        print(f"Completed processing JSON index {i} in {elapsed_time:.2f} seconds")
        with open(f'../data/summit/result_zj_{time_str}.json', 'w', encoding='utf-8') as json_file:
            json.dump(data_list_result, json_file, ensure_ascii=False, indent=4)
    return data_list_result


# ## 主代码
# 运行下列代码块，就能运行完整的整个项目，本NoteBook未启用并发，因此效率较低，运行所有问题需要使用大约3小时时间。如果你只希望运行于几道题，你可以在参数中进行设置，比如只运行前面两题。




if __name__ == "__main__":

    # Load input data
    with open(question_data_path, 'r', encoding='utf-8') as file:
        q_json_list = json.load(file)

    # Users can specify a range to process the corresponding subset of data
    # For example, from index 0 to 100 (excluding 100), processing the first 100 JSON entries
    start_idx = 0
    #end_idx = 101  # Specify processing data in the range 0-101

    results = main_answer(q_json_list, start_index=start_idx)

    # Write the processing results to a file
    with open(f'../data/summit/result_{time_str}.json', 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=4)


