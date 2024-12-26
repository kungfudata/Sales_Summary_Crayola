# 下面是数据库的用户名和密码
# 这是我们的百度数据库
import pymysql
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
user = 'miki'
passwd = 'Yangr0uchuan%997'

class DataBase_baidu:
    def __init__(self, db_name="WDT_integration", host="180.76.60.179", port=3306):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db_name
        self.conn = None
        self.cur = None
        self.charset = "utf8"
        self.connect_mysql()

    def connect_mysql(self):
        try:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.passwd,
                db=self.db,
                charset=self.charset
            )
            cur = conn.cursor()
            self.conn = conn
            self.cur = cur
        except pymysql.MySQLError as e:
            print(f"Error connecting to database: {e}")

    def execute_query(self, query, params=None):
        try:
            self.cur.execute(query, params)
            self.conn.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing query: {e}")
            self.conn.rollback()

    def commit(self):
        try:
            self.conn.commit()
        except pymysql.MySQLError as e:
            print(f"Error committing transaction: {e}")
            self.conn.rollback()

    def fetchall(self):
        return self.cur.fetchall()

    def fetchone(self):
        return self.cur.fetchone()

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed")
class ExcelSaver:
    def __init__(self, file_name):
        """
        初始化ExcelSaver类

        :param file_name: 要保存的Excel文件名
        """
        self.file_name = file_name
        self.workbook = Workbook()
        self.sheet = self.workbook.active
        self.sheet.title = "Sheet1"  # 设置默认工作表名称

    def save_dataframe(self, df, start_row, start_col):
        """
        将DataFrame保存到Excel的指定位置

        :param df: 要保存的DataFrame
        :param start_row: 数据框的起始行
        :param start_col: 数据框的起始列
        """
        # 将DataFrame转化为Excel行
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), start=start_row):
            for c_idx, value in enumerate(row, start=start_col):
                self.sheet.cell(row=r_idx, column=c_idx, value=value)

    def save(self):
        """
        保存Excel文件
        """
        self.workbook.save(self.file_name)