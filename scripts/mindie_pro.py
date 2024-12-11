# coding=utf-8
import copy
import datetime
import json
import pandas as pd
from template_prompt import SELECT_TABLE_sys, SELECT_TABLE_SINGLE, SELECT_TABLE_SINGLE_FORMATE
from llm_model import call_glm, call_qwen72b,call_qwen72b_npu
import datetime

def chat_model(messages):
    result =call_qwen72b(messages)
    return result


## 一个表描述与问题进行判断。
def select_table_single():

    start_time = datetime.datetime.now()
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # 创建线程池
    executor = ThreadPoolExecutor(max_workers=3)

    with open('db_desc.json', 'r') as f:
        content = f.read()
    json_tb = json.loads(content)

    with open('question.json', 'r') as f:
        content = f.read()
    query_json = json.loads(content)

    print('---query_json',len(query_json))

    new_query_json = []
    for json_string in query_json:
        tid = json_string['tid']
        team = json_string['team']
        new_team = []
        for i, t in enumerate(team):
            id = t['id']
            query = t['question']
            rel_table = []

            # 提交任务到线程池，将参数打包成元组
            futures = {executor.submit(process_query, (query,tb)): tb for tb in json_tb}

            # 等待所有任务完成，并收集结果
            for future in as_completed(futures):
                try:
                    tb = futures[future]
                    result = future.result()
                    print(f"Data:{tb}#####,Result: {result}")

                    # 如果table 与query相关，即输出的是"是"，则加入到rel_table中
                    if result == "是":
                        rel_table.append(f"{tb['库名英文']}.{tb['表英文']}")
                except Exception as exc:
                    print(f"Generated an exception: {exc}")


            new_team.append(
                {
                    "id": id,
                    "question": query,
                    "db_table": ','.join(rel_table)
                }
            )

        new_query_json.append(
            {
                "tid": tid,
                "team": new_team
            }
        )

    # 所有任务完成后，关闭线程池
    executor.shutdown(wait=True)

    new_query_json = json.dumps(new_query_json, ensure_ascii=False, indent=1)

    with open('query_table_select_single_result.json', 'w') as f:
        f.write(new_query_json)

    print('last finished time', str(datetime.datetime.now() - start_time))


select_table_single()
