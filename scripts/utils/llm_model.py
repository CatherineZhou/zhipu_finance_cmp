
import json
import requests

from zhipuai import ZhipuAI


use_model="zhipu"

def minmax_guanfang_unstream(messages):
    url = "https://api.minimax.chat/v1/text/chatcompletion_v2"
    API_KEY = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJHcm91cE5hbWUiOiLlkajmmZPkvqAiLCJVc2VyTmFtZSI6IuWRqOaZk-S-oCIsIkFjY291bnQiOiIiLCJTdWJqZWN0SUQiOiIxNzgzMDIzNzI5MDU2NDMyMTMxIiwiUGhvbmUiOiIxODU2NTY2MzM4MCIsIkdyb3VwSUQiOiIxNzgzMDIzNzI5MDQ4MDQzNTIzIiwiUGFnZU5hbWUiOiIiLCJNYWlsIjoiIiwiQ3JlYXRlVGltZSI6IjIwMjUtMDEtMTUgMTc6MDQ6NTEiLCJUb2tlblR5cGUiOjEsImlzcyI6Im1pbmltYXgifQ.DQaVhgfZ7zcBwNuj6-DAjl6fv-Qm7I7XX_qatZTo0Z-_IUY5Iwl5iXDZlkd5kU7jWByFNDISep0wzP6WhiLKg8ih-NsKb7bd83AyUPuSqtuGZQ7gBv8OtkwU7nfP_qNg_YEhKZbBgolNLGqNqV_crbBqwWku_wojZ8BL01CIGNFe54NablONALorn9dWZWqmSf6v9pYyyxlbIJW65Un2N1AIptVKwmfcMbdoPUhQS5csmxJNIyXsRUkmaIkxZC-fMRAv-Af_ecCdq-TFRKlycOLFAioJ2TIiPY1LAxYIb3ds-a8bGqVrRjLP2tXR5VUhsk9eo0raUIJoK2UvDRaQOw"
    try:
        payload = json.dumps({
        "model":"MiniMax-Text-01",
        "messages": messages

    })
        headers = {
            'Authorization': f'Bearer {API_KEY}',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)
        result = json.loads(response.text)
        result = result['choices'][0]['message']['content']
        return result

    except Exception as e:
        print(f'##trig_unstream_chat_unfinetune prompt:{input} ##Have error,prompt:\n {e}')
        return ' request error!!!'


ZHIPU_KEY="dc1e8b158e4d09d2e11c94cf7dbc5680.6p6wswTgiOOys1qV"
Access_Token = '937a9e2dd1094c558772836f0981527e'  # Competition team Token, used to access the competition database
MODEL = "glm-4-plus"  # Default large model used; this solution uses the GLM-4-PLUS model entirely
client = ZhipuAI(api_key=ZHIPU_KEY)

def zhipu_chat_completion(messages, model=MODEL):
    """
    Create a chat completion using the provided messages and model.

    Parameters:
        messages (list): A list of message dictionaries to pass to the model.
        model (str): The model name to use.

    Returns:
        response (dict): The response from the chat completion endpoint.
    """
    response = client.chat.completions.create(
        model=model,
        stream=False,
        messages=messages
    )
    result = response.choices[0].message.content
    return result



def create_chat_completion(messages):
    if use_model=="zhipu":
        result = zhipu_chat_completion(messages)
    if use_model=="minmax":
        result = minmax_guanfang_unstream(messages)

    return result


# prompt="""根据这些内容'600872的全称、A股简称、法人、法律顾问、会计师事务所及董秘是？全称 中炬高新技术实业(集团)股份有限公司, A股简称 中炬高新, 法人 余健华, 法律顾问 广东卓建(中山)律师事务所, 会计师事务所 立信会计师事务所(特殊普通合伙), 董秘 郭毅航',帮我重写这个问题”'该公司实控人是否发生改变？如果发生变化，什么时候变成了谁？是哪国人？是否有永久境外居留权？（回答时间用XXXX-XX-XX）' ,让问题清晰明确，不改变原意，不要遗漏信息，特别是时间，只返回问题。
# 以下是示例：
# user:根据这些内容'最新更新的2021年度报告中，机构持有无限售流通A股数量合计最多的公司简称是？  公司简称 帝尔激光',帮我重写这个问题'在这份报告中，该公司机构持有无限售流通A股比例合计是多少，保留2位小数？' ,让问题清晰明确，不改变原意，不要遗漏信息，特别是时间，只返回问题。
# assistant:最新更新的2021年度报告中,公司简称 帝尔激光 持有无限售流通A股比例合计是多少，保留2位小数？
# """
# prompt="""你的任务是问题改写，结合给定上文，改写用户问题。让问题清晰明确，不改变原意，不要遗漏信息，特别是时间，只返回问题。
# 以下是示例，方便你理解：
#
# 系统提供上文：最新更新的2021年度报告中，机构持有无限售流通A股数量合计最多的公司简称是？  公司简称 帝尔激光'
# 用户问题：在这份报告中，该公司机构持有无限售流通A股比例合计是多少，保留2位小数？'
# 你改写后的问题:最新更新的2021年度报告中,公司简称 帝尔激光 持有无限售流通A股比例合计是多少，保留2位小数？
#
# --------------
# 现在开始你的任务
# 系统提供上文：'600872的全称、A股简称、法人、法律顾问、会计师事务所及董秘是？全称 中炬高新技术实业(集团)股份有限公司, A股简称 中炬高新, 法人 余健华, 法律顾问 广东卓建(中山)律师事务所, 会计师事务所 立信会计师事务所(特殊普通合伙), 董秘 郭毅航
# 用户问题：该公司实控人是否发生改变？如果发生变化，什么时候变成了谁？是哪国人？是否有永久境外居留权？（回答时间用XXXX-XX-XX）
# 你改写后的问题：
# """
#
# messages = [{"role": "user", "content": prompt}]
#
# print(create_chat_completion(messages))




