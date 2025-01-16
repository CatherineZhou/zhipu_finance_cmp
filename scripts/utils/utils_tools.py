
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



def find_json(text):
    """
    Attempt to extract and parse a JSON object from the provided text.
    The function tries up to three attempts using two patterns:
      1. A Markdown code block with ```json ... ```
      2. A more general JSON-like pattern using { and }

    If successful, returns the parsed JSON data.
    If parsing fails after all attempts, returns the original text.

    Parameters:
        text (str): The input text from which to extract JSON.

    Returns:
        dict or str: Parsed JSON dictionary if successful, else the original text.
    """
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        json_pattern = r"```json\n(.*?)\n```"
        match = re.search(json_pattern, text, re.DOTALL)
        if not match:
            json_pattern2 = r"({.*?})"
            match = re.search(json_pattern2, text, re.DOTALL)

        if match:
            json_string = match.group(1) if match.lastindex == 1 else match.group(0)
            # Remove Markdown formatting if present
            json_string = json_string.replace("```json\n", "").replace("\n```", "")
            try:
                data = json.loads(json_string)
                return data
            except json.JSONDecodeError as e:
                if attempt < max_attempts:
                    print(f"Attempt {attempt}: Failed to parse JSON, reason: {e}. Retrying...")
                else:
                    print(f"All {max_attempts} attempts to parse JSON failed. Returning original text.")
        else:
            if attempt < max_attempts:
                print(f"Attempt {attempt}: No JSON string found in the text. Retrying...")
            else:
                print("No matching JSON string found. Returning original text.")

        # If no match or no success in this attempt, return the original text
        return text

Access_Token = '937a9e2dd1094c558772836f0981527e'
def exec_sql_s(sql):
    """
    Execute a given SQL query on a remote endpoint and return the result.
    Uses 'Access_Token' for authorization and limits the result to 10 rows.

    Parameters:
        sql (str): The SQL query to be executed.

    Returns:
        list: The query result as a list of rows (dictionaries), or None if not found.
    """
    headers = {
        "Authorization": f'Bearer {Access_Token}',
        "Accept": "application/json"
    }
    url = "https://comm.chatglm.cn/finglm2/api/query"

    response = requests.post(url, headers=headers, json={
        "sql": sql,
        "limit": 10
    })
    response_json = response.json()

    # If there's no 'data' field, print the full response for debugging
    if 'data' not in response_json:
        print(response_json)

    # Return 'data' if present
    return response_json.get('data', None)


def clean_text(text):
    """
    Remove any parenthetical segments (including Chinese parentheses) and trim whitespace.
    For example, "This is a sentence(remark)" -> "This is a sentence"

    Parameters:
        text (str): The text to clean.

    Returns:
        str: The cleaned text.
    """
    pattern = r'[\(（][^\)）]*[\)）]'  # Pattern to match parentheses and their contents
    cleaned_text = re.sub(pattern, '', text).strip()
    return cleaned_text


def dict_to_sentence(data):
    """
    Convert a dictionary into a descriptive sentence by enumerating key-value pairs.
    For example: {"name": "John", "age": 30} -> "name 是 John, age 是 30"

    Parameters:
        data (dict): The dictionary to convert.

    Returns:
        str: A sentence describing the dictionary keys and values.
    """
    try:
        if not isinstance(data, dict):
            raise ValueError("Input is not a dictionary")

        return ", ".join(f"{key} 是 {value}" for key, value in data.items())
    except Exception as e:
        print(f"Error in dict_to_sentence: {e}")
        return str(data)


def process_dict(d):
    """
    Recursively process a nested dictionary to produce a comma-separated description.
    For nested dictionaries, it processes them recursively and returns a descriptive string.

    For example:
        {
            "company": {
                "name": "ABC Corp",
                "location": "New York"
            },
            "year": 2021
        }
    might be processed into a string like:
        "company company 是 name 是 ABC Corp, location 是 New York, year 2021"

    Parameters:
        d (dict): A dictionary or another object to describe.

    Returns:
        str: A descriptive string.
    """

    def recursive_process(sub_dict):
        sentences = []
        for key, value in sub_dict.items():
            if isinstance(value, dict):
                # Process nested dictionary and wrap result in dict_to_sentence for formatting
                nested_result = recursive_process(value)
                sentences.append(dict_to_sentence({key: nested_result}))
            else:
                # Non-dict values are directly appended
                sentences.append(f"{key} {value}")
        return ", ".join(sentences)

    if not isinstance(d, dict):
        # If it's not a dictionary, just return its string representation
        return str(d)

    return recursive_process(d)


def select_data(sql_text):
    """
    Sends the given SQL query to a specified endpoint and returns the JSON response.

    Parameters:
        sql_text (str): The SQL query to be executed.

    Returns:
        str: The JSON response from the API, formatted with indentation.
    """
    url = "https://comm.chatglm.cn/finglm2/api/query"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f'Bearer {Access_Token}'
    }
    data = {
        "sql": sql_text,  # e.g. SELECT * FROM constantdb.secumain LIMIT 10
        "limit": 15
    }
    response = requests.post(url, headers=headers, json=data)
    try:
        return json.dumps(response.json(), indent=2, ensure_ascii=False)
    except:
        return str(response.json())


def to_select(text):
    """
    High-level function that:
      1. Extracts SQL from the given text.
      2. Optimizes the extracted SQL by converting date columns to 'date(...)'.
      3. Executes the optimized SQL through select_data and returns the result.

    Parameters:
        text (str): The input text containing an SQL statement.

    Returns:
        str: The JSON response from the SQL query.
    """
    sql_statement = extract_sql(text)
    #print('***********Extracted SQL****************')
    print(sql_statement)
    #print('***********Extracted SQL****************')
    optimized_sql = replace_date_with_day(sql_statement)
    result = select_data(optimized_sql)
    return result


def extract_sql(text):
    """
    Extracts an SQL statement from a block of text enclosed in triple backticks:
        ```sql
        SELECT ...
        ```

    Parameters:
        text (str): The full text containing an SQL statement.

    Returns:
        str: The extracted SQL statement, or a message if not found.
    """
    sql_pattern = re.compile(r'```sql(.*?)```', re.DOTALL)
    match = sql_pattern.search(text)
    if match:
        # Strip leading and trailing whitespace from the matched SQL
        return match.group(1).strip()
    else:
        return "No SQL statement found."


def replace_date_with_day(sql):
    """
    This function replaces instances of exact date conditions in a SQL
    statement from a format like:
        TradingDate = 'YYYY-MM-DD'
    to:
        date(TradingDate) = 'YYYY-MM-DD'

    Parameters:
        sql (str): The original SQL statement.

    Returns:
        str: The modified SQL statement, or the original if no match is found.
    """
    # Regex pattern to match patterns like: ColumnName = 'YYYY-MM-DD'
    pattern = r"([.\w]+)\s*=\s*'(\d{4}-\d{2}-\d{2})'"

    def replace_func(match):
        column_name = match.group(1)
        date_value = match.group(2)
        return f"date({column_name}) = '{date_value}'"

    new_sql = re.sub(pattern, replace_func, sql)

    # If no change was made, return the original SQL
    return new_sql if new_sql != sql else sql


def extract_list_from_json(json_string):
    """
    Attempt to decode a JSON string representing a list.

    Parameters:
        json_string (str): The JSON string to decode.

    Returns:
        list or None: The decoded list, or None if decoding fails or not a list.
    """
    try:
        data = json.loads(json_string)
        if isinstance(data, list):
            return data
        else:
            print("解码的JSON数据不是一个列表")
            return None
    except json.JSONDecodeError as e:
        print(f"JSON解码错误: {e}")
        return None


def find_value_in_list_of_dicts(dict_list, key_to_match, value_to_match, key_to_return):
    """
    Search through a list of dictionaries and find the first dictionary where
    the value of key_to_match equals value_to_match, then return the value
    associated with key_to_return.

    Parameters:
        dict_list (list): A list of dictionaries to search through.
        key_to_match (str): The key whose value we want to match.
        value_to_match (str): The value we are looking for.
        key_to_return (str): The key whose value we want to return.

    Returns:
        (str): The value associated with key_to_return in the matching dictionary,
               or an empty string if no match is found.
    """
    for dictionary in dict_list:
        if dictionary.get(key_to_match) == value_to_match:
            return dictionary.get(key_to_return)
    return ''


def generate_embeddings(text, model="bge-m3:latest"):
    url = "http://10.233.243.163:11434/api/embeddings"
    headers = {'Authorization': 'Bearer ollama'}  # 确保使用正确的API密钥
    data = {
        "model": model,
        "prompt": text
    }
    response = requests.post(url, headers=headers, json=data)
    return response.json()


def get_sim_score(text_a, text_b):

    from scipy import spatial

    # 定义目标向量
    vector_text_a = generate_embeddings(text_a)['embedding']
    vector_text_b = generate_embeddings(text_b)['embedding']

    # 定义一组向量列表
    #vector_text_b_list=[minmax_embedding(b) for b in text_b_list]

    # 计算余弦相似度
    cosine_similarities = 1 - spatial.distance.cosine(vector_text_a, vector_text_b)

    print("余弦相似度：", cosine_similarities)
    return cosine_similarities