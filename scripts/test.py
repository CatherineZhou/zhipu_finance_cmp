import json


def test1():
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def process_data(data):
        # 假设这里是一些数据处理的代码，并返回结果
        result = data * data  # 示例：返回数据的平方
        return result

    data_list = [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5]

    # 使用 ThreadPoolExecutor 创建线程池
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 使用字典推导式创建一个 future 到数据的映射
        future_to_data = {executor.submit(process_data, data): data for data in data_list}

        # 等待每个 future 完成，并获取结果
        for future in as_completed(future_to_data):
            data = future_to_data[future]
            try:
                result = future.result()
                print(f"Data: {data}, Result: {result}")
            except Exception as exc:
                print(f"Data {data} generated an exception: {exc}")


def test2():
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def process_data(data):
        # 假设这里是一些数据处理的代码，并返回结果
        result = data * data  # 示例：返回数据的平方
        return result

    # 创建一个 ThreadPoolExecutor 实例
    executor = ThreadPoolExecutor(max_workers=5)

    # 假设我们有多个数据列表需要处理
    data_lists = [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9]
    ]

    # 遍历每个数据列表，并使用同一个线程池来处理
    for data_list in data_lists:
        # 提交任务到线程池
        futures = {executor.submit(process_data, data):data for data in data_list}
        # 等待所有任务完成，并收集结果
        for future in as_completed(futures):
            try:
                data = futures[future]
                result = future.result()
                print(f"Data:{data},Result: {result}")
            except Exception as exc:
                print(f"Generated an exception: {exc}")

    # 所有任务完成后，关闭线程池
    executor.shutdown(wait=True)


def test3():
    import json
    import pandas as pd

    json_str="""[
 {
  "ID": 601773045143,
  "BuybackID": 601072770846,
  "InfoPublDate": "2019-01-26 12:00:00.000",
  "EffectiveDate": "2019-01-25 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 239200.0,
  "Percentage": 0.0002,
  "CumulativeSum": 239200.0,
  "CumulativeSumToTS": 0.0002,
  "HighPrice": 14.46,
  "LowPrice": 14.32,
  "BuybackFunds": 3440489.0,
  "CumulativeValueSum": 3440489.0,
  "UpdateTime": "2023-05-20 10:03:17.927",
  "JSID": 737964898723,
  "InsertTime": "2019-01-25 11:10:45.180"
 },
 {
  "ID": 602102389072,
  "BuybackID": 599870778337,
  "InfoPublDate": "2019-01-30 12:00:00.000",
  "EffectiveDate": "2019-01-29 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 2500000.0,
  "Percentage": 0.0008,
  "CumulativeSum": 2500000.0,
  "CumulativeSumToTS": 0.0008,
  "HighPrice": 8.45,
  "LowPrice": 8.09,
  "BuybackFunds": 20762514.94,
  "CumulativeValueSum": 20762514.94,
  "UpdateTime": "2023-05-20 03:01:39.220",
  "JSID": 737938350958,
  "InsertTime": "2019-01-29 06:39:10.060"
 },
 {
  "ID": 602280911849,
  "BuybackID": 601588077168,
  "InfoPublDate": "2019-02-01 12:00:00.000",
  "EffectiveDate": "2019-01-31 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 1135000.0,
  "Percentage": 0.000365,
  "CumulativeSum": 1135000.0,
  "CumulativeSumToTS": 0.000365,
  "HighPrice": 4.41,
  "LowPrice": 4.38,
  "BuybackFunds": 4997600.0,
  "CumulativeValueSum": 4997600.0,
  "UpdateTime": "2023-05-20 10:03:17.927",
  "JSID": 737964898753,
  "InsertTime": "2019-01-31 08:15:11.907"
 },
 {
  "ID": 602380943598,
  "BuybackID": 601072770846,
  "InfoPublDate": "2019-02-02 12:00:00.000",
  "EffectiveDate": "2019-01-31 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 2016353.0,
  "Percentage": 0.0017,
  "CumulativeSum": 2255553.0,
  "CumulativeSumToTS": 0.0019,
  "HighPrice": 14.7,
  "LowPrice": 14.14,
  "BuybackFunds": 29129415.0,
  "CumulativeValueSum": 32569904.0,
  "UpdateTime": "2023-05-20 10:03:17.927",
  "JSID": 737964898724,
  "InsertTime": "2019-02-02 12:02:23.633"
 },
 {
  "ID": 602382003358,
  "BuybackID": 599870778337,
  "InfoPublDate": "2019-02-02 12:00:00.000",
  "EffectiveDate": "2019-01-31 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 5050000.0,
  "Percentage": 0.0017,
  "CumulativeSum": 7550000.0,
  "CumulativeSumToTS": 0.0025,
  "HighPrice": 8.52,
  "LowPrice": null,
  "BuybackFunds": 42466885.06,
  "CumulativeValueSum": 63229400.0,
  "UpdateTime": "2023-05-20 03:01:39.220",
  "JSID": 737938350959,
  "InsertTime": "2019-02-02 12:20:03.407"
 },
 {
  "ID": 603835214455,
  "BuybackID": 600813886734,
  "InfoPublDate": "2019-02-19 12:00:00.000",
  "EffectiveDate": "2019-02-18 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 1927800.0,
  "Percentage": 0.00065,
  "CumulativeSum": 1927800.0,
  "CumulativeSumToTS": 0.00065,
  "HighPrice": 4.49,
  "LowPrice": 4.41,
  "BuybackFunds": 8587782.0,
  "CumulativeValueSum": 8587782.0,
  "UpdateTime": "2023-05-20 10:03:17.927",
  "JSID": 737964898700,
  "InsertTime": "2019-02-18 08:00:14.517"
 },
 {
  "ID": 604007023838,
  "BuybackID": 599956683403,
  "InfoPublDate": "2019-02-21 12:00:00.000",
  "EffectiveDate": "2019-02-20 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 80400.0,
  "Percentage": 0.00025,
  "CumulativeSum": 80400.0,
  "CumulativeSumToTS": 0.00025,
  "HighPrice": 11.67,
  "LowPrice": 11.61,
  "BuybackFunds": 937062.0,
  "CumulativeValueSum": 937062.0,
  "UpdateTime": "2023-05-20 10:03:17.927",
  "JSID": 737964898569,
  "InsertTime": "2019-02-20 07:43:31.130"
 },
 {
  "ID": 604617340828,
  "BuybackID": 601692447534,
  "InfoPublDate": "2019-02-28 12:00:00.000",
  "EffectiveDate": "2019-02-27 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 30007437.0,
  "Percentage": 0.00257,
  "CumulativeSum": 30007437.0,
  "CumulativeSumToTS": 0.00257,
  "HighPrice": 3.05,
  "LowPrice": 2.86,
  "BuybackFunds": 89030879.48,
  "CumulativeValueSum": 89030879.48,
  "UpdateTime": "2023-05-20 10:03:17.927",
  "JSID": 737964898765,
  "InsertTime": "2019-02-27 09:15:40.880"
 },
 {
  "ID": 604700250709,
  "BuybackID": 599870778337,
  "InfoPublDate": "2019-03-01 12:00:00.000",
  "EffectiveDate": "2019-02-28 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 22149500.0,
  "Percentage": 0.00734912,
  "CumulativeSum": 29699500.0,
  "CumulativeSumToTS": 0.0099,
  "HighPrice": 9.12,
  "LowPrice": null,
  "BuybackFunds": 197392600.0,
  "CumulativeValueSum": 260622000.0,
  "UpdateTime": "2023-05-20 03:01:39.220",
  "JSID": 737938350960,
  "InsertTime": "2019-02-28 08:17:30.777"
 },
 {
  "ID": 604780396039,
  "BuybackID": 600813886734,
  "InfoPublDate": "2019-03-02 12:00:00.000",
  "EffectiveDate": "2019-02-28 12:00:00.000",
  "CurrencyUnit": 1420,
  "BuybackSum": 251900.0,
  "Percentage": 8e-05,
  "CumulativeSum": 2179700.0,
  "CumulativeSumToTS": 0.00073,
  "HighPrice": null,
  "LowPrice": null,
  "BuybackFunds": 1120955.0,
  "CumulativeValueSum": 9708737.0,
  "UpdateTime": "2023-05-20 10:03:17.927",
  "JSID": 737964898701,
  "InsertTime": "2019-03-01 06:32:30.403"
 }
]"""

    json_str = json.loads(json_str)
    df = pd.json_normalize(json_str)

    print(df)

#test3()

# def test4():
#     import json
#
#     # 定义表结构数据
#     table_structure = """
#     === astockbasicinfodb.lc_business 表结构 ===
#     列名                   注释                             数据示例
#     ----------------------------------------------------------------------------------------------------
#     ID                   ID                             599777135022
#     CompanyCode          公司代码                           224275
#     InfoPublDate         信息发布日期                         2019-01-03 12:00:00.000
#     InfoSource           信息来源                           第二届董事会第八次会议决议公告
#     SMDeciPublDate       股东大会决议公告日期                     2019-01-18 12:00:00.000
#     IfPassed             是否否决                           0
#     BusinessMajor        经营范围-主营                        生产:半导体设备(测试机、分选机)。服务:半导体设备、光机电一体化技术、计算机软件的技术开发、...
#     BusinessMinor        经营范围-兼营                        NULL
#     MainBusiness         主要业务                           集成电路专用设备的研发、生产和销售,主要产品包括集成电路测试机和分选机。
#     MainName             主要产品与业务名称                      测试机和分选机等
#     CSRCInduCategory     行业代码                           13035
#     InduEngaged          涉足行业                           NULL
#     ChangeReason         简称变更原因                         NULL
#     XGRQ                 修改日期                           2024-05-17 01:43:13.797
#     JSID                 JSID                           769290274663
#     IndustryType         行业类别                           22
#     """
#
#     # 解析表结构数据
#     def parse_table_structure(text):
#         lines = text.strip().split('\n')
#         table_name = lines[1].split(' ')[1]
#         columns = []
#         for line in lines[3:]:
#             if line.strip() == '----------------------------------------------------------------------------------------------------':
#                 break
#             columns.append(line.split(' ')[0])
#         return {
#             'table_name': table_name,
#             'columns': columns
#         }
#
#     # 解析表数据
#     def parse_table_data(text):
#         lines = text.strip().split('\n')
#         data = {}
#         for line in lines[3:]:
#             columns = line.split(' ')
#             data[columns[0].strip()] = columns[2].strip()
#         return data
#
#     # 将表结构和数据转换为JSON
#     def convert_to_json(table_structure_text, table_data_text):
#         table_structure = parse_table_structure(table_structure_text)
#         table_data = parse_table_data(table_data_text)
#         return json.dumps({
#             'table_structure': table_structure,
#             'table_data': table_data,
#         }, indent=4, ensure_ascii=False)
#
#     # 示例数据
#     table_data = """
#     === astockbasicinfodb.lc_business 表数据 ===
#     ID                   599777135022
#     CompanyCode          224275
#     InfoPublDate         2019-01-03 12:00:00.000
#     InfoSource           第二届董事会第八次会议决议公告
#     SMDeciPublDate       2019-01-18 12:00:00.000
#     IfPassed             0
#     BusinessMajor        生产:半导体设备(测试机、分选机)。服务:半导体设备、光机电一体化技术、计算机软件的技术开发、...
#     BusinessMinor        NULL
#     MainBusiness         集成电路专用设备的研发、生产和销售,主要产品包括集成电路测试机和分选机。
#     MainName             测试机和分选机等
#     CSRCInduCategory     13035
#     InduEngaged          NULL
#     ChangeReason         NULL
#     XGRQ                 2024-05-17 01:43:13.797
#     JSID                 769290274663
#     IndustryType         22
#     """
#
#     """
#     === astockeventsdb.lc_investordetail 表结构 ===
#     列名                   注释                             数据示例
#     ----------------------------------------------------------------------------------------------------
#     ID                   ID                             599916268685
#     RID                  投资者关系活动ID                      599911000861
#     Participant          调研机构                           华创证券
#     ParticipantID        调研机构编码                         41714.0
#     PersonalName         调研人员                           冯自力
#     PersonalID           调研人员编码                         NULL
#     PostName             职位名称                           NULL
#     InsertTime           发布时间                           2019-01-04 11:25:37.483
#     UpdateTime           更新时间                           2024-07-25 11:05:31.333
#     JSID                 JSID                           775524788399
#     SerialNumber         序号                             1
#
#     """
#     如何将以上内容转为如下json形式：
#     {"表名": " astockeventsdb.lc_investordetail",
#      "列名":"ID",
#      "注释":"ID",
#      "数据示例":"599916268685",
#      },
#     {"表名": " astockeventsdb.lc_investordetail",
#      "列名":"ID",
#      "注释":"ID",
#      "数据示例":"599916268685",
#      },
#      ....
#
#
#     # 转换并打印JSON
#     json_output = convert_to_json(table_structure, table_data)
#     print(json_output)
#
# test4()

def test5(table_structure):
    import json

    # 原始的表结构文本
    # table_structure = """
    #     === astockeventsdb.lc_investordetail 表结构 ===
    #     列名                   注释                             数据示例
    #     ----------------------------------------------------------------------------------------------------
    #     ID                   ID                             599916268685
    #     RID                  投资者关系活动ID                      599911000861
    #     Participant          调研机构                           华创证券
    #     ParticipantID        调研机构编码                         41714.0
    #     PersonalName         调研人员                           冯自力
    #     PersonalID           调研人员编码                         NULL
    #     PostName             职位名称                           NULL
    #     InsertTime           发布时间                           2019-01-04 11:25:37.483
    #     UpdateTime           更新时间                           2024-07-25 11:05:31.333
    #     JSID                 JSID                           775524788399
    #     SerialNumber         序号                             1
    # """

    # 将原始文本分割成行
    lines = table_structure.strip().split('\n')

    # 初始化表名变量
    table_name=lines[0].split(' ')[1]

    # 初始化列信息列表
    columns_info = []

    # 从第三行开始是列的具体信息
    for line in lines[3:]:
        # 使用制表符分割每一行的内容
        parts = line.split('                   ')
        parts=[p.strip(' ') for p in parts if len(p)>=1 ]
        if len(parts) == 3:
            column_name, comment, example = parts
            # 将列信息添加到列表中
            columns_info.append({
                "列名": column_name,
                "注释": comment,
                "数据示例": example
            })

    return {
        "table_name":table_name,
        "columns_info":columns_info
    }

    #
    # # 将列信息列表转换为JSON格式
    # json_output = json.dumps(columns_info, ensure_ascii=False, indent=4)
    # print(json_output)

#test5()

def test6():
    with open('../data/org_data/all_tables_schema.txt') as f:
        content =f.read()

    data = content.split('\n\n')

    pro_data=[]
    for table in data:
        print(f'-----table:\n{table}\n\n')
    #     table_formate = test5(table)
    #     pro_data.append(table_formate)
    #
    #
    # json_data = json.dumps(pro_data,indent=1, ensure_ascii=False)
    # with open('../data/process/schema_pro.json','w') as f:
    #     f.write(json_data)


#test6()

prompt_t="""##扮演一位数据分析师，根据系统系统的数据表，生成用户问题相关的SQL，你只需要生成SQL，不需要解释。
## 相关表结构描述：
<TABLE>
表中文名:上市公司基本资料.公司概况
表英文名：AStockBasicInfoDB.LC_StockArchives
表描述: 收录上市公司的基本情况，包括：联系方式、注册信息、中介机构、行业和产品、公司证券品种及背景资料等内容。
CREATE TABLE `AStockBasicInfoDB.LC_StockArchives` (
  `ID` bigint NOT NULL COMMENT 'ID',
  `CompanyCode` bigint DEFAULT NULL COMMENT '公司代码',
  `State` bigint DEFAULT NULL COMMENT '国别',
  `SecretaryBD` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '董事会秘书',
  `SecuAffairsRepr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '证券/股证事务代表',
  `AuthReprSBD` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '董秘授权代表',
  `ContactTel` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '联系人电话',
  `ContactFax` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '联系人传真',
  `ContactEmail` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '联系人电子邮箱',
  `RegAddr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司注册地址',
  `RegZipCode` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司注册地址邮编',
  `OfficeAddr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司办公地址',
  `OfficeZipCode` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司办公地址邮编',
  `ContactAddr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司联系地址',
  `ConatactZipCode` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司联系地址邮编',
  `Email` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '邮箱',
  `Website` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司网址',
  `DisclosureWebsites` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '信息披露网址',
  `DisclosurePapers` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '信息披露报纸',
  `EstablishmentDate` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司成立日期',
  `IRegPlace` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '首次注册登记地点',
  `LegalRepr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '法人代表',
  `GeneralManager` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '总经理',
  `LegalConsultant` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '法律顾问',
  `AccountingFirm` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '会计师事务所',
  `InduCSRC` bigint DEFAULT NULL COMMENT '公司所属证监会行业(聚源)',
  `BusinessMajor` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '经营范围-主营',
  `BusinessMinor` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '经营范围-兼营',
  `AShareAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'A股证券简称',
  `AStockCode` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'A股证券代码',
  `BShareAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'B股证券简称',
  `BStockCode` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'B股证券代码',
  `HShareAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'H股证券简称',
  `HStockCode` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'H股证券代码',
  `BriefIntroText` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '公司简介',
  `XGRQ` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '修改日期',
  `JSID` bigint DEFAULT NULL COMMENT 'JSID',
  `ChiName` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '中文名称',
  `BusinessRegNumber` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '企业法人营业执照注册号',
  `SecretaryBDTel` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '董秘电话',
  `SecretaryBDFax` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '董秘传真',
  `SecretaryBDEmail` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '董秘电子邮件',
  `SecuAffairsReprTel` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '证券事务代表电话',
  `SecuAffairsReprFax` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '证券事务代表传真',
  `SecuAffairsReprEmail` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '证券事务代表电子邮件',
  `CityCode` bigint DEFAULT NULL COMMENT '地区代码',
  `CDRShareAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'CDR证券简称',
  `CDRStockCode` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'CDR证券代码',
  `ExtendedAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '扩位简称',
  `UnprofitableMark` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '尚未盈利标识',
  `SpecialVoteMark` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '特殊表决权标识',
  `VIEMark` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '协议控制架构标识',
  `RedChipMark` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '红筹企业标识',
  `RegArea` bigint DEFAULT NULL COMMENT '所属区县',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='公司概况'；
表外键(Foreign_keys):
-公司代码（CompanyCode）：与“证券主表（SecuMain）”中的“公司代码（CompanyCode）”关联，得到上市公司的交易代码、简称等。
-公司所属证监会行业(聚源)(InduCSRC):与(CT_IndustryType)表中的"行业内部编码(IndustryNum)"字段关联,当Standard=1时,LB=1；当Standard=22时,LB=22；当Standard=25时,LB=25；当Standard=26时,LB=26。
-国别省份（State）：与“国家城市代码表（LC_AreaCode）”中的“地区内部编码（AreaInnerCode）”关联，得到省份具体信息。
-地区代码(CityCode)：与“国家城市代码表（LC_AreaCode）”中的“地区内部编码（AreaInnerCode）”关联，得到城市具体信息。
-尚未盈利标识（UnprofitableMark）：在上市时发行人尚未盈利的，其股票或存托凭证的特别标识为“U”；发行人首次实现盈利的，该特别标识取消，数据值为空。
-特殊表决权标识（SpecialVoteMark）：在上市时发行人具有表决权差异安排的，其股票或存托凭证的特别标识为“W”；上市后不再具有表决权差异安排的，该特别标识取消，数据值为空。
-协议控制架构标识（VIEMark）：在上市时发行人具有协议控制架构或者类似特殊安排的，其股票或存托凭证的特别标识为“V”；上市后不再具有相关安排的，该特别标识取消，数据值为空。
-红筹企业标识（RedChipMark）：发行人属于红筹企业，则数据值=”是“；空值则指无此标识。
-所属区县（RegArea）：与“国家城市代码表（LC_AreaCode）”中的“地区内部编码（AreaInnerCode）”关联，得到所属区县具体信息。
</TABLE>

<TABLE>
表中文名:常量库.证券主表
表英文名: ConstantDB.SecuMain
表描述: 本表收录单个证券品种（股票、基金、债券）的代码、简称、上市交易所等基础信息。
CREATE TABLE `ConstantDB.SecuMain` (
  `ID` bigint NOT NULL COMMENT 'ID',
  `InnerCode` bigint DEFAULT NULL COMMENT '证券内部编码',
  `CompanyCode` bigint DEFAULT NULL COMMENT '公司代码',
  `SecuCode` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '证券代码',
  `ChiName` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '中文名称',
  `ChiNameAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '中文名称缩写',
  `EngName` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '英文名称',
  `EngNameAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '英文名称缩写',
  `SecuAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '证券简称',
  `ChiSpelling` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '拼音证券简称',
  `SecuMarket` bigint DEFAULT NULL COMMENT '证券市场',
  `SecuCategory` bigint DEFAULT NULL COMMENT '证券类别',
  `ListedDate` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '上市日期',
  `ListedSector` bigint DEFAULT NULL COMMENT '上市板块',
  `ListedState` bigint DEFAULT NULL COMMENT '上市状态',
  `XGRQ` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '修改日期',
  `JSID` bigint DEFAULT NULL COMMENT 'JSID',
  `ISIN` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'ISIN代码',
  `ExtendedAbbr` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'No description available',
  `ExtendedSpelling` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT 'No description available',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='证券主表' 
表外键(Foreign_keys):
-公司代码（CompanyCode）：与“证券主表（SecuMain）”中的“公司代码（CompanyCode）”关联，得到上市公司的交易代码、简称等。
-证券市场(SecuMarket)与(CT_SystemConst)表中的DM字段关联，令LB = 201 AND DM IN (10,12,13,14,15,16,18,40,49,50,52,54,55,56,65,66,67,68,69,70,71,72,73,75,76,77,78,79,80,81,83,84,85,86,87,88,89,90,93,94,95,96,99,100,101,102,103,104,105,106,107,110,161,162,180,200,202,210,230,240,260,280,310,320,390,400,620,630,631,640,641,650,653,654,655,657,658,659,660,661,662,663,664,666,667,66302,66303,66305)，得到证券市场的具体描述：10-上海期货交易所，12-中国银行间外汇市场，13-大连商品交易所，14-上海黄金交易所，15-郑州商品交易所，16-上海票据交易所，18-北京证券交易所，40-芝加哥商业交易所，49-澳大利亚证券交易所，50-新西兰证券交易所，52-埃及开罗及亚历山大证券交易所，54-阿根廷布宜诺斯艾利斯证券交易所，55-巴西圣保罗证券交易所，56-墨西哥证券交易所，65-印度尼西亚证券交易所，66-泰国证券交易所，67-韩国首尔证券交易所，68-东京证券交易所，69-新加坡证券交易所，70-台湾证券交易所，71-柜台交易市场，72-香港联交所，73-一级市场，75-亚洲其他交易所，76-美国证券交易所，77-美国纳斯达克证券交易所，78-纽约证券交易所，79-美国其他交易市场，80-加拿大多伦多证券交易所，81-三板市场，83-上海证券交易所，84-其他市场，85-伦敦证券交易所，86-法国巴黎证券交易所，87-德国法兰克福证券交易所，88-欧洲其他交易所，89-银行间债券市场，90-深圳证券交易所，93-上海银行间同业拆借市场，94-瑞士证券交易所，95-荷兰阿姆斯特丹证券交易所，96-约翰内斯堡证券交易所，99-东京同业拆借市场，100-美国国债回购市场，101-伦敦银行同业拆借市场，102-香港银行同业拆借市场，103-新加坡银行同业拆借市场，104-中国银行同业拆借市场，105-欧元银行同业拆借市场，106-布鲁塞尔证券交易所，107-雅加达证券交易所，110-以色列特拉维夫证券交易所，161-意大利证券交易所，162-哥本哈根证券交易所，180-挪威奥斯陆证券交易所，200-斯德哥尔摩证券交易所，202-伊斯坦布尔证券交易所，210-印度国家证券交易所，230-奥地利维也纳证券交易所，240-西班牙马德里证券交易所，260-爱尔兰证券交易所，280-菲律宾证券交易所，310-机构间私募产品报价与服务系统，320-俄罗斯莫斯科证券交易所，390-里斯本证券交易所，400-芝加哥期权交易所，620-胡志明市证券交易所，630-沪市代理深市市场，631-沪市代理港交所市场，640-深市代理沪市市场，641-深市代理港交所市场，650-国际外汇市场(晨星)，653-上海环境能源交易所，654-北京绿色交易所，655-天津碳排放权交易中心，657-湖北碳排放权交易中心，658-重庆碳排放权交易中心，659-四川联合环境交易所，660-广州碳排放权交易所，661-海峡股权交易中心，662-深圳排放权交易所，663-欧洲能源交易所，664-全国碳排放权交易，666-布达佩斯证券交易所，667-全国温室气体自愿减排交易市场，66302-韩国ETS，66303-加拿大魁北克Cap-and-Trade(CaT)，66305-美国区域温室气体倡议（RGGI）。
-证券类别(SecuCategory)与(CT_SystemConst)表中的DM字段关联，令LB = 1177 AND DM IN (1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,23,26,27,28,29,30,31,32,33,35,36,37,38,39,40,41,42,43,44,45,46,47,55,79,80,211)，得到证券类别的具体描述：1-A股，2-B股，4-大盘，5-国债回购，6-国债现货，7-金融债券，8-开放式基金，9-可转换债券，10-其他，11-企业债券，12-企业债券回购，13-投资基金，14-央行票据，15-深市代理沪市股票，16-沪市代理深市股票，17-资产支持证券，18-资产证券化产品，19-买断式回购，20-衍生权证，21-股本权证，23-商业银行定期存款，26-收益增长线，27-新质押式回购，28-地方政府债，29-可交换公司债，30-拆借，31-信用风险缓释工具，32-浮息债计息基准利率，33-定期存款凭证，35-大额存款凭证，36-债券借贷，37-存款类机构质押式回购，38-存款类机构信用拆借，39-现货，40-货币对，41-中国存托凭证，42-协议回购，43-三方回购，44-利率互换品种，45-标准利率互换合约，46-报价回购，47-标准化票据，55-优先股，79-深市代理港交所股票，80-沪市代理港交所股票，211-自贸区债。
-上市板块(ListedSector)与(CT_SystemConst)表中的DM字段关联，令LB = 207 AND DM IN (1,2,3,4,5,6,7,8)，得到上市板块的具体描述：1-主板，2-中小企业板，3-三板，4-其他，5-大宗交易系统，6-创业板，7-科创板，8-北交所股票。
-上市状态(ListedState)与(CT_SystemConst)表中的DM字段关联，令LB = 1176 AND DM IN (1,3,5,9)，得到上市状态的具体描述：1-上市，3-暂停，5-终止，9-其他。
</TABLE>

--------------------------------
现在请基于如上提供的表，生成与用户问题相关的sql语句。sql以#开始，以#结束，你只需要输出sql语句，不需要解释。
Query：我想问的是，600872股票的全称、A股简称、法人、法律顾问、会计师事务所及董秘的具体信息？
Answer:
"""

mrc_propmpt ="""请基于如下系统提供的参考内容，回答用户问题。如果系统提供的内容无法回答用户问题，请回复："根据上下文无法回答问题。"。注意，你只能基于系统系统的参考内容回答用户问题，请不要编造内容

## 系统提供的参考内容：
[
 {
  "股票全称": "中炬高新技术实业(集团)股份有限公司",
  "A股简称": "中炬高新",
  "法人": "余健华",
  "法律顾问": "广东卓建(中山)律师事务所",
  "会计师事务所": "天职国际会计师事务所（特殊普通合伙）",
  "董秘": "郭毅航"
 }
]

Query:我想问的是，600872这个股票的全称、A股简称、法人、法律顾问、会计师事务所及董秘的信息。
Answer:
"""
from llm_model import call_glm,call_qwen72b
from utils import select_data

# messages=[{"role":"user","content":mrc_propmpt}]
#
# result=call_glm(messages)
# print('----result:\n', result)
#result=call_qwen72b(messages)
# print('----sql:',result)
# result=result.replace('#','')

result="""SELECT a.EndDate, a.ControllerName, a.NationalityDesc, a.PermanentResidency 
 FROM astockshareholderdb.lc_actualcontroller AS a 
 JOIN constantdb.secumain AS s ON a.CompanyCode = s.CompanyCode 
 WHERE s.SecuCode = '600872' 
 ORDER BY a.EndDate DESC 
 LIMIT 2 """
print(select_data(result))


