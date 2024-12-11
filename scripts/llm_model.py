
api_key="dc1e8b158e4d09d2e11c94cf7dbc5680.6p6wswTgiOOys1qV"

import requests
import json
def call_glm(messages):

    from zhipuai import ZhipuAI
    try:
        client = ZhipuAI(api_key=api_key)  # 请填写您自己的APIKey
        response = client.chat.completions.create(
            model="glm-4-plus",  # 请填写您要调用的模型名称
            messages=messages,
        )
        result = response.choices[0].message.content
        #print('-------result:',response.choices[0].message.content)
        return result

    except Exception as e:
        return f'have an error {e}'


def call_qwen72b(messages):
    try:
        from openai import OpenAI
        client = OpenAI(
            base_url="http://10.233.243.163:11434/v1",
            api_key="ollama",
        )

        completion = client.chat.completions.create(
            model="qwen2.5:72b",
            messages=messages,
            max_tokens=32000,
        )
        result = completion.choices[0].message.content
        return result
    except Exception as e:
        return f' Have error Exception:{e}'



def call_qwen72b_npu(messages):
    unstream_url = 'http://10.138.2.48:10025/v1/chat/completions'
    headers = {"Content-Type": "application/json"}
    try:

        messages = messages
        data = {"model": "qwen2",
                'messages': messages,
                "max_tokens": 512,
                "presence_penalty": 1.03,
                "frequency_penalty": 1.0,
                "temperature": 0.5,
                "top_p": 0.95,
                "stream": False}  # POST请求的数据

        response = requests.post(unstream_url, json=data, headers=headers)
        result = response.text
        result = json.loads(result)
        result = result["choices"][0]["message"]["content"]

        return result
    except Exception as e:
        print(f'##trig_unstream_chat_unfinetune prompt:{messages} ##Have error,prompt:\n {e}')
        return ''

# messages = [{"role": "user", "content": "中国首都是哪里？"}]
# print(call_qwen72b_npu(messages))


def minmax_chat(input):

    unstream_url = "http://10.183.65.16:20001/minmax_chat_unstream/"
    #unstream_url = "http://localhost:20001/minmax_chat_unstream/"
    #unstream_url="http://10.183.65.16:11112/minmax_chat_unstream_proxcy/"
    headers = {"Content-Type": "application/json"}

    try:
        data = {'query': input}  # POST请求的数据
        response = requests.post(unstream_url, json=data, headers=headers)
        response = json.loads(response.text)
        return response
    except Exception as e:
        print(f'##trig_unstream_chat_unfinetune prompt:{input} ##Have error,prompt:\n {e}')
        return ''

#minmax_chat("中国首都是哪里？")

