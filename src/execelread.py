#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2024
# @generate at 2024/10/28 13:22
# comment: excel数据读取

import pandas as pd
import mysql.connector
import configparser
import uuid

excel_file_path = "../file/车务分类分级模板_2024.xlsx"

# 数据库连接定义
config = configparser.ConfigParser()
config.read("../conf/db.cnf")

my_host = config.get("dcg_task_test", "host")
my_database = config.get("dcg_task_test", "database")
my_user = config.get("dcg_task_test", "user")
my_password = config.get("dcg_task_test", "password")

con = mysql.connector.connect(
    host=my_host, user=my_user, password=my_password, database=my_database
)

try:
    ins_sql = "INSERT INTO zr_dcg_task.tb_template_imports (ImportStamp, AssetId, AssetName, Class1, Class2, Class3, Class4, DataTypeName, DataColumns, ColumnComment, DataGrade, IsSens, IsEncrypt, IsCommon) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    temp_id = str(uuid.uuid4())
    asset_id = 'AI9999999959'
    asset_name = '车务系统'
    df = pd.read_excel(excel_file_path)
    cur = con.cursor(dictionary=True)

    for index, row in df.iterrows():
        # df.insert(loc=index, column="ImportStamp", value=temp_id)
        # df.insert(loc=index, column="AssetId", value=asset_id)
        # df.insert(loc=index, column="AssetName", value=asset_name)
        # print(df.head(10))
        data = (
            temp_id,
            asset_id,
            asset_name,
            row['Class1'],
            row['Class2'],
            row['Class3'],
            row['Class4'],
            row['DataTypeName'],
            row['DataColumns'],
            row['ColumnComment'],
            row['DataGrade'],
            row['IsSens'],
            row['IsEncrypt'],
            row['IsCommon']
        )
        print(data)
        cur.execute(ins_sql, data)
    con.commit()

except Exception as e:
    print(e)
finally:
    cur.close()
    con.close()


