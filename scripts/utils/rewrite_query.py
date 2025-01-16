#coding=utf-8
# 问题改写
from  utils.utils_tools import create_chat_completion
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
    prompt = (
        f"根据这些内容'{context_text}',帮我重写这个问题”'{original_question}' ,让问题清晰明确，"
        "不改变原意，不要遗漏信息，特别是时间，只返回问题。\n"
        "以下是示例：\n"
        "user:根据这些内容'最新更新的2021年度报告中，机构持有无限售流通A股数量合计最多的公司简称是？  公司简称 帝尔激光',"
        "帮我重写这个问题'在这份报告中，该公司机构持有无限售流通A股比例合计是多少，保留2位小数？' ,让问题清晰明确，不改变原意，不要遗漏信息，特别是时间，"
        "只返回问题。\n"
        "assistant:最新更新的2021年度报告中,公司简称 帝尔激光 持有无限售流通A股比例合计是多少，保留2位小数？"
    )

    messages = [{"role": "user", "content": prompt}]
    response = create_chat_completion(messages)
    return response.choices[0].message.content