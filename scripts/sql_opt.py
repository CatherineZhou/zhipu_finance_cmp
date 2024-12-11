import mysql.connector
from mysql.connector import Error
import logging
import json
class DatabaseManager:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        #self.cursor = None # 初始化时创建好游标，直到与db的交互结束后。

    def connect(self):
        """连接到数据库"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            #self.cursor = self.connection.cursor()
            if self.connection.is_connected():
                db_Info = self.connection.get_server_info()
                print("Successfully connected to the database.", db_Info)
        except Error as e:
            print(f"Error connecting to MySQL: {e}")

    def create_database(self,db_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            self.connection.database = db_name  # 指定当前连接的默认数据库
            print("Database selected successfully")
        except Error as e:
            logging.error("Error while create database", e)

    def disconnect(self):
        """断开数据库连接"""
        if self.connection is not None:
            self.connection.close()
            print("MySQL connection is closed.")

    def create_table(self, table_name, create_sql):
        """创建表"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_sql)
            cursor.close()
            self.connection.commit()
            print(f"Table {table_name} created successfully.")
        except Error as e:
            print(f"Failed to create table {table_name}: {e}")

    def drop_table(self, table_name):
        """删除表"""
        query = f"DROP TABLE IF EXISTS {table_name};"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
            self.connection.commit()
            print(f"Table {table_name} dropped successfully.")
        except Error as e:
            print(f"Failed to drop table {table_name}: {e}")

    def insert_data(self, table_name, data):
        """插入数据 data 时字典形式的数据
        """
        columns = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, tuple(data.values()))
            cursor.close()
            self.connection.commit()
            print(f"Data inserted into table {table_name} successfully.")
        except Error as e:
            print(f"Failed to insert data into table {table_name}: {e}")


    def insert_from_dataframe(self, df, table_name):
        """从DataFrame插入数据到表"""
        if df.empty:
            print("DataFrame is empty.")
            return

        columns = ', '.join(df.columns)
        values = ', '.join(['%s'] * len(df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, df.values)
            cursor.close()
            self.connection.commit()
            print(f"{len(df)} rows inserted into table {table_name} successfully.")
        except Error as e:
            print(f"Failed to insert data into table {table_name}: {e}")



    def delete_data(self, table_name, conditions):
        """删除数据"""
        condition_query = ' AND '.join([f"{key} = %s" for key in conditions.keys()])
        query = f"DELETE FROM {table_name} WHERE {condition_query}"
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, tuple.conditions.values())
            cursor.close()
            self.connection.commit()
            print(f"Data deleted from table {table_name} successfully.")
        except Error as e:
            print(f"Failed to delete data from table {table_name}: {e}")

    def select_data(self, query):
        """查询数据"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"Failed to select data from table {query}: {e}")

    def get_column_names(self, table_name):
        """从表中获取列名,table_name 名字必须带数据库名
        """

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = [column[0] for column in cursor.fetchall()]
            return columns
        except Error as e:
            print(f"Error getting column names: {e}")
            return None

    def query_to_json(self, query):
        """执行查询并将结果转换为JSON"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            json_result = []
            for row in result:
                json_result.append(dict(zip(column_names, row)))
            return json.dumps(json_result, indent=4)
        except Error as e:
            print(f"Error executing query: {e}")
            return None

    def insert_json_data(self,  table_name, data):
        data = json.loads(data)
        cursor = self.connection.cursor()
        for item in data:

            columns = ','.join(item.keys())
            values = [v if v is not None else 'NULL' for v in item.values()]
            values = ','.join([f"'{v}'" if isinstance(v, str) else str(v) for v in values])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(query)

        self.connection.commit()
        cursor.close()


#1054 (42S22): Unknown column 'None' in 'field list'
# 使用DatabaseManager类的示例
if __name__ == "__main__":
    db_manager = DatabaseManager('localhost', 'root', 'Herz_3280')
    db_manager.connect()

    # db_name='mytest'
    # table_name = f'{db_name}.users'
#
    db_manager.create_database('astockshareholderdb') # sql语句带数据库名就可以。
#
#     # 如果表存在则删除表
#     db_manager.drop_table(table_name )
#
#     # 创建表
#     create_sql = f""" CREATE TABLE {table_name } (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     username VARCHAR(255),
#     password VARCHAR(255),
#     email VARCHAR(255)
# );
#     """
#     db_manager.create_table(table_name,create_sql)
#
#     # 插入数据
#     data = {
#         'username': 'user1',
#         'password': 'password1',
#         'email': 'user1@example.com'
#     }
#     db_manager.insert_data(table_name, data)
#
#     # 更新数据
#     db_manager.update_data(table_name, {'email': 'user1@example.com'}, {'password': 'newpassword1'})
#
#
#     # 查询数据
#     result = db_manager.select_data('users')
#     print('----result', result)
#
#     # 查询列名
#     columns=db_manager.get_column_names(table_name)
#     print(f"columns results in JSON format:\n{columns}")
#
    # # 查询结果处理为json
    # select_sql = f"select * from {table_name} ;"
    # json_result = db_manager.query_to_json(select_sql)
    # print(f"Query results in JSON format:\n{json_result}")
#
#
#     # #
#     import pandas as pd
#     json_str="""[
#  {
#   "ID": 717016908277,
#   "CompanyCode": 1428,
#   "State": 144200000,
#   "SecretaryBD": "杨力康",
#   "SecuAffairsRepr": "凌亦奇",
#   "AuthReprSBD": null,
#   "ContactTel": "0510-86632358",
#   "ContactFax": "0510-86630191-481",
#   "ContactEmail": "600481@shuangliang.com",
#   "RegAddr": "江苏省无锡市江阴市利港镇",
#   "RegZipCode": "214444",
#   "OfficeAddr": "江苏省江阴市利港镇西利路88号",
#   "OfficeZipCode": "214444",
#   "ContactAddr": "江苏省江阴市利港镇西利路88号",
#   "ConatactZipCode": "214444",
#   "Email": "600481@shuangliang.com",
#   "Website": "http://www.shuangliang.com",
#   "DisclosureWebsites": "http://www.sse.com.cn",
#   "DisclosurePapers": "《证券时报》《中国证券报》",
#   "EstablishmentDate": "1995-10-05 12:00:00.000",
#   "IRegPlace": null,
#   "LegalRepr": "刘正宇",
#   "GeneralManager": "刘正宇",
#   "LegalConsultant": "上海市通力律师事务所",
#   "AccountingFirm": "天衡会计师事务所（特殊普通合伙）",
#   "InduCSRC": 13038,
#   "BusinessMajor": "冷热水机组、热泵、空气冷却设备、海水淡化节能设备、污水处理设备、压力容器、环境保护专用设备的研究、开发、制造、安装、销售;合同能源管理;自营和代理各类商品及技术的进出口业务(国家限定企业经营或禁止进出口的商品及技术除外);对外承包工程项目。(依法须经批准的项目,经相关部门批准后方可开展经营活动)许可项目:特种设备制造;特种设备设计;特种设备安装改造修理(依法须经批准的项目,经相关部门批准后方可开展经营活动,具体经营项目以审批结果为准)",
#   "BusinessMinor": null,
#   "AShareAbbr": "双良节能",
#   "AStockCode": "600481",
#   "BShareAbbr": null,
#   "BStockCode": null,
#   "HShareAbbr": null,
#   "HStockCode": null,
#   "BriefIntroText": "公司以绿色环保为己任,不断开拓创新,致力于成为数字化驱动的全生命周期碳中和解决方案服务商,在“节能节水、清洁能源”等领域形成核心竞争力;以数字化智造、服务型智造助推清洁能源革命,已形成多晶硅核心装备、单晶硅材料、电池组件光伏产业链,并深耕地热、氢能、绿电、储能等清洁能源技术研发及装备生产,以数字化驱动的碳中和综合服务助力“双碳”目标实现。公司主营业务包括节能节水、新能源装备以及光伏三大块业务板块。2023年,公司荣得“智能制造试点示范、“省长质量奖”、“两业融合发展标杆引领典型”、“省级绿色工厂”、“省级质量标杆”、溴冷机整机装配智能化制造车间获评“省级智能制造示范车间”;江苏省重点培育和发展的国际知名品牌。同时,凭借多年合作基础和优质的产品及服务水平,茅台授予双良战略合作伙伴、授予双良节能2023年度优秀供应商荣誉称号,不断深化产业链生态构建。2023公司荣获“分布式能源优秀项目特等奖”、中国节能减排企业贡献一等奖、碳中和领域突出贡献企业奖、获评“ESG金茉莉奖”等多项殊荣。",
#   "XGRQ": "2024-08-30 09:29:07.017",
#   "JSID": 778877872162,
#   "ChiName": "双良节能系统股份有限公司",
#   "BusinessRegNumber": "320000400001692",
#   "SecretaryBDTel": "0510-86632358",
#   "SecretaryBDFax": "0510-86630191-481",
#   "SecretaryBDEmail": "600481@shuangliang.com",
#   "SecuAffairsReprTel": "0510-86632358",
#   "SecuAffairsReprFax": "0510-86630191-481",
#   "SecuAffairsReprEmail": "lingyq@shuangliang.com",
#   "CityCode": 144200113,
#   "CDRShareAbbr": null,
#   "CDRStockCode": null,
#   "ExtendedAbbr": null,
#   "UnprofitableMark": null,
#   "SpecialVoteMark": null,
#   "VIEMark": null,
#   "RedChipMark": null,
#   "RegArea": 144200120
#  }
# ]
#     """
#
#     db_manager.insert_json_data('astockbasicinfodb.lc_stockarchives',json_str)
#
#     # 断开连接
#     db_manager.disconnect()
