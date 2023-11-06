#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw 
# @generate at 2023/10/30 09:57

from datetime import datetime, timedelta
from random import randint
import json
import mysql.connector
import configparser

# 数据库连接定义
config = configparser.ConfigParser()
config.read("db.cnf")

my_host = config.get("dcg_task_test", "host")
my_database = config.get("dcg_task_test", "database")
my_user = config.get("dcg_task_test", "user")
my_password = config.get("dcg_task_test", "password")

con = mysql.connector.connect(
    host=my_host, user=my_user, password=my_password, database=my_database
)

cur = con.cursor(dictionary=True)

# 常量值定义
time_now = datetime.now()

rp_no_prifix = 'FLFJ'
rp_no_date = datetime.strftime(time_now, '%y%m%d')
rp_no_rand = str(randint(1000, 9999))
rp_no = rp_no_prifix + '000' + rp_no_date + rp_no_rand  # 报告编号

str_now = datetime.strftime(time_now, '%Y-%m-%d %H:%M:%S')  # 报告日期

rp_name = '消息中心'  # 报告涉及系统

rp_dura = 10  # 报告生成  总时长
rp_start = datetime.strftime(time_now - timedelta(seconds=rp_dura), '%Y-%m-%d %H:%M:%S')  # 报告导出 开始时间
rp_end = str_now  # 报告导出 结束时间
rp_author = '彭垣昊'  # 操作人员

# 分类分级任务运行结果中获取值

rp_assets = 1  # 数据资产总数
rp_asset_sens = 0  # 数据资产敏感字段数量
rp_databases = 10  # 数据库/schema数量
rp_database_sens = 10  # 数据库/schema敏感数量
rp_columns = 100  # 表数量
rp_column_sens = 1  # 表敏感数量
rp_datatype = 100  # 数据类型数量
rp_datatype_tag = 80  # 数据类型标签敏感数量
rp_datatype_sens = 5  # 数据类型字段敏感数量
rp_tag = 100  # 分类分级标签数量
rp_cata = 5  # 分类分级目录数量
rp_class = 'B'  # 分类分级最高分级
rp_rate = 1  # 数据类型标签占比

rp_densenssec = 10  # 安全风险，应脱敏或加密数量

# 任务信息获取
# 实例信息
sql_2_1 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.AssetName, 'key2', S.DBAddress, 'key3', '未分组', "
           "'key4', '未分组', 'key5', '未分组', 'key6', NULL, "
           "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
           "FROM (SELECT DISTINCT A.AssetName,B.DBAddress "
           "FROM tb_template_imports A "
           "LEFT JOIN zr_dcg_asset.tb_asset_instance B "
           "ON B.AssetId=A.AssetId "
           "AND B.Deleted=0 WHERE A.AssetId='AI9999999938') AS S;"
           )

try:
    str_2_1 = []
    cur.execute(sql_2_1)
    res_2_1 = cur.fetchall()
    if res_2_1:
        str_2_1 = res_2_1[0].get('data')
except Exception as e:
    print(e)

# 各分组
str_group = [
    {
        "key1": "未分组",
        "key2": "1",
        "key3": "100%",
        "key4": "",
        "key5": "",
        "key6": "",
        "key7": "",
        "key8": "",
        "key9": "",
        "key_list": None
    },
    {
        "key1": "1",
        "key2": "1",
        "key3": "100%",
        "key4": "",
        "key5": "",
        "key6": "",
        "key7": "",
        "key8": "",
        "key9": "",
        "key_list": None
    }
]

# 数据资产类型
str_2_5 = [
    {
        "key1": "MySQL",
        "key2": "1",
        "key3": "100%",
        "key4": "",
        "key5": "",
        "key6": "",
        "key7": "",
        "key8": "",
        "key9": "",
        "key_list": None
    },
    {
        "key1": "1",
        "key2": "1",
        "key3": "100%",
        "key4": "",
        "key5": "",
        "key6": "",
        "key7": "",
        "key8": "",
        "key9": "",
        "key_list": None
    }
]

# 资产库表 统计
sql_2_6 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.AssetName, 'key2', S.InstanceType, 'key3', S.DBCnt, "
           "'key4', S.SensRate, 'key5', S.SensTotal, 'key6', S.DataGrade,  "
           "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
           "FROM (SELECT A.AssetName,B.InstanceType,COUNT(DISTINCT SUBSTRING_INDEX(DataColumns, '.', 1)) AS DBCnt, "
           "CONCAT(COS.SensCnt,'/',COS.TBCnt,'--',ROUND(((COS.SensCnt / COS.TBCnt) * 100), 2),'%') AS SensRate, "
           "CO.SensTotal,CONCAT(MIN(IF(A.DataGrade NOT IN ('A','B','C','D'),'D',A.DataGrade)),'级') AS DataGrade "
           "FROM tb_template_imports A "
           "LEFT JOIN zr_dcg_asset.tb_asset_instance B "
           "ON B.AssetId=A.AssetId "
           "AND B.Deleted=0 "
           "LEFT JOIN zr_dcg_asset.tb_asset_database C "
           "ON C.AssetInsId=B.Id "
           "AND C.DBName COLLATE utf8mb4_general_ci= "
           "SUBSTRING_INDEX(DataColumns, '.', 1) COLLATE utf8mb4_general_ci "
           "AND C.Deleted=0 "
           "LEFT JOIN (SELECT SUBSTRING_INDEX(DataColumns, '.', 1) AS DBName, COUNT(*) SensTotal "
           "FROM tb_template_imports WHERE AssetId='AI9999999938' "
           "AND IsSens = '是' "
           "GROUP BY SUBSTRING_INDEX(DataColumns, '.', 1)) CO "
           "ON CO.DBName=SUBSTRING_INDEX(A.DataColumns, '.', 1) "
           "LEFT JOIN (SELECT SUBSTRING_INDEX(DataColumns, '.', 1) AS DBName, "
           "COUNT(DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1)) AS TBCnt, "
           "COUNT(DISTINCT (IF(IsSens='是',SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1),NULL))) "
           "AS SensCnt FROM tb_template_imports WHERE AssetId='AI9999999938' "
           "GROUP BY SUBSTRING_INDEX(DataColumns, '.', 1)) AS COS "
           "ON COS.DBName=SUBSTRING_INDEX(A.DataColumns, '.', 1) "
           "WHERE A.AssetId='AI9999999938' GROUP BY SUBSTRING_INDEX(A.DataColumns, '.', 1)) AS S;"
           )

sql_all_2_6 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.AssetCnt, 'key2', NULL, 'key3', S.DBCnt, "
               "'key4', S.SensRate, 'key5', S.SensTotal, 'key6', NULL, "
               "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
               "FROM (SELECT COUNT(DISTINCT A.AssetId) AS AssetCnt, "
               "COUNT(DISTINCT SUBSTRING_INDEX(DataColumns, '.', 1)) AS DBCnt, "
               "(SELECT CONCAT(COS.SensCnt,'/',COS.TBCnt,'--',ROUND(((COS.SensCnt / COS.TBCnt) * 100), 2),'%') "
               "FROM (SELECT COUNT(DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1)) AS TBCnt, "
               "COUNT(DISTINCT (IF(IsSens='是',SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1),NULL))) "
               "AS SensCnt FROM tb_template_imports WHERE AssetId='AI9999999938') AS COS) AS SensRate, "
               "(SELECT COUNT(*) SensTotal "
               "FROM tb_template_imports WHERE AssetId='AI9999999938' "
               "AND IsSens = '是') AS SensTotal "
               "FROM tb_template_imports A "
               "LEFT JOIN zr_dcg_asset.tb_asset_instance B "
               "ON B.AssetId=A.AssetId "
               "AND B.Deleted=0 "
               "LEFT JOIN zr_dcg_asset.tb_asset_database C "
               "ON C.AssetInsId=B.Id "
               "AND C.DBName COLLATE utf8mb4_general_ci= "
               "SUBSTRING_INDEX(DataColumns, '.', 1) COLLATE utf8mb4_general_ci "
               "AND C.Deleted=0 WHERE A.AssetId='AI9999999938') AS S"
               )

try:
    str_2_6 = []
    str_all_2_6 = []
    cur.execute(sql_2_6)
    res_2_6 = cur.fetchall()
    if res_2_6:
        str_2_6 = json.loads(res_2_6[0]['data'])
    cur.execute(sql_all_2_6)
    res_all_2_6 = cur.fetchall()
    if res_all_2_6:
        str_all_2_6 = json.loads(res_all_2_6[0]['data'])
    str_2_6.append(str_all_2_6[0])
except Exception as e:
    print(e)

# 数据类型标签统计
sql_2_7 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.DataTypeName, 'key2', S.ClassName, "
           "'key3', S.DataGrade, 'key4', S.IsSens, 'key5', NULL, 'key6', S.DataTypeCnt,'key7', "
           "S.TotalRate, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
           "FROM (SELECT DataTypeName,MAX(Class4) AS ClassName,MIN(DataGrade) AS DataGrade, "
           "CASE IsSens WHEN '是' THEN '敏感' WHEN '否' THEN '非敏感' ELSE '非敏感' END AS IsSens, "
           "COUNT(*) AS DataTypeCnt,CONCAT(ROUND((( COUNT(*) / (SELECT COUNT(DISTINCT DataTypeName) "
           "FROM tb_template_imports "
           "WHERE AssetId='AI9999999938') ) * 100),2),'%') AS TotalRate "
           "FROM tb_template_imports "
           "WHERE AssetId='AI9999999938' "
           "GROUP BY DataTypeName) AS S;"
           )

sql_all_2_7 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.DataTypeCnt, 'key2', NULL, 'key3', NULL, "
               "'key4', NULL, 'key5', NULL, 'key6', S.TotalCnt, "
               "'key7', '100%', 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
               "FROM (SELECT COUNT(DISTINCT DataTypeName) AS DataTypeCnt, COUNT(*) AS TotalCnt "
               "FROM tb_template_imports WHERE AssetId='AI9999999938') AS S;"
               )

try:
    str_2_7 = []
    str_all_2_7 = []
    cur.execute(sql_2_7)
    res_2_7 = cur.fetchall()
    if res_2_7:
        str_2_7 = json.loads(res_2_7[0]['data'])
    cur.execute(sql_all_2_7)
    res_all_2_7 = cur.fetchall()
    if res_all_2_7:
        str_all_2_7 = json.loads(res_all_2_7[0]['data'])
    str_2_7.append(str_all_2_7[0])
except Exception as e:
    print(e)

# 2_8 分类分级标签统计
sql_2_8 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.Class4, 'key2', S.DataGrade, "
           "'key3', S.IsSens, 'key4', S.TagCnt, 'key5', TotalRate, 'key6', NULL, "
           "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
           "FROM (SELECT Class4,MIN(IF(DataGrade NOT IN ('A','B','C','D'),'D',DataGrade)) AS DataGrade, "
           "CASE IsSens WHEN '是' THEN '敏感' WHEN '否' THEN '非敏感' ELSE '非敏感' END AS IsSens, "
           "COUNT(*) AS TagCnt,CONCAT(ROUND((( COUNT(*) / (SELECT COUNT(DISTINCT Class4) "
           "FROM tb_template_imports "
           "WHERE AssetId='AI9999999938') ) * 100),2),'%') AS TotalRate "
           "FROM tb_template_imports "
           "WHERE AssetId='AI9999999938' "
           "GROUP BY Class4) AS S;"
           )

sql_all_2_8 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.Class4Cnt, 'key2', NULL, 'key3', NULL, "
               "'key4', NULL, 'key5', NULL, 'key6', S.TotalCnt, "
               "'key7', '100%', 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
               "FROM (SELECT COUNT(DISTINCT Class4) AS Class4Cnt, COUNT(*) AS TotalCnt "
               "FROM tb_template_imports WHERE AssetId='AI9999999938') AS S;"
               )

try:
    str_2_8 = []
    str_all_2_8 = []
    cur.execute(sql_2_8)
    res_2_8 = cur.fetchall()
    if res_2_8:
        str_2_8 = json.loads(res_2_8[0]['data'])
    cur.execute(sql_all_2_8)
    res_all_2_8 = cur.fetchall()
    if res_all_2_8:
        str_all_2_8 = json.loads(res_all_2_8[0]['data'])
    str_2_8.append(str_all_2_8[0])
except Exception as e:
    print(e)

# 分级标签
sql_2_9 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.DataGrade, 'key2', S.IsSens, "
           "'key3', S.GradeCnt, 'key4', S.TotalRate, 'key5', NULL, 'key6', NULL, "
           "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
           "FROM (SELECT DataGrade,CASE IsSens WHEN '是' THEN '敏感' WHEN '否' THEN '非敏感' ELSE '非敏感' END AS IsSens, "
           "COUNT(*) AS GradeCnt,CONCAT(ROUND((( COUNT(*) / (SELECT COUNT(*) "
           "FROM tb_template_imports "
           "WHERE AssetId='AI9999999938') ) * 100),2),'%') AS TotalRate "
           "FROM tb_template_imports "
           "WHERE AssetId='AI9999999938' "
           "GROUP BY DataGrade) AS S; "
           )

sql_all_2_9 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.GreadeCnt, 'key2', NULL, 'key3', TotalCnt, "
               "'key4', '100%', 'key5', NULL, 'key6', NULL, "
               "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
               "FROM (SELECT COUNT(DISTINCT DataGrade) AS GreadeCnt, COUNT(*) AS TotalCnt "
               "FROM tb_template_imports WHERE AssetId='AI9999999938') AS S;"
               )

try:
    str_2_9 = []
    str_all_2_9 = []
    cur.execute(sql_2_9)
    res_2_9 = cur.fetchall()
    if res_2_9:
        str_2_9 = json.loads(res_2_9[0]['data'])
    cur.execute(sql_all_2_9)
    res_all_2_9 = cur.fetchall()
    if res_all_2_9:
        str_all_2_9 = json.loads(res_all_2_9[0]['data'])
    str_2_9.append(str_all_2_9[0])
except Exception as e:
    print(e)

# 报告生成
def get_rp_value():
    # 文本拼接
    rp_value = {}
    # 第一章部分
    # 0_1 报告编码
    rp_value["0_1"] = rp_no
    # 0_2 报告生成时间
    rp_value["0_2"] = str_now
    # 0_3 报告名称
    rp_value["0_3"] = rp_name
    # 0_4 涉及系统
    rp_value["0_4"] = '涉及系统：未分组'
    # 1_10 开始时间
    rp_value["1_10"] = rp_start
    # 1_11 结束时间
    rp_value["1_11"] = rp_end
    # 1_12 耗时
    rp_value["1_12"] = str(rp_dura) + '秒'
    # 1_13 耗时
    rp_value["1_13"] = rp_author
    # 1_2 涉及部门
    rp_value["1_2"] = '涉及部门：未分组'
    # 1_3 涉及区域
    rp_value["1_3"] = '涉及区域：未分组'
    # 1_4 涉及资产
    str_1_4 = ("数据资产：本次任务共涉及 <span style='color: #0079fe'>{0}</span>个数据资产，"
               "其中包含敏感字段的资产数量为<span style='color: #0079fe'>{1}</span>个")
    rp_value["1_4"] = str_1_4.format(rp_assets, rp_asset_sens)
    # 1_5 数据库/schema数量
    str_1_5 = ("数据库/schema数量：本次任务共涉及<span style='color: #0079fe'>{0}</span>个数据库/schema，"
               "其中包含敏感字段的数据库/schema的数量为<span style='color: #0079fe'>{1}</span>个")
    rp_value["1_5"] = str_1_5.format(rp_databases, rp_database_sens)
    # 1_6 表数量
    str_1_6 = ("表数量：本次任务共涉及<span style='color: #0079fe'>{0}</span>张数据表/文件数量，"
               "其中包含敏感字段的表数量为<span style='color: #0079fe'>{1}</span>个")
    rp_value["1_6"] = str_1_6.format(rp_columns, rp_column_sens)
    # 1_7 数据类型
    str_1_7 = ("数据类型：本次任务共涉及<span style='color: #0079fe'>{0}</span>个数据字段，"
               "有<span style='color: #0079fe'>{1}</span>个字段命中数据类型标签，"
               "其中敏感字段为<span style='color: #0079fe'>{2}</span>个")
    rp_value["1_7"] = str_1_7.format(rp_datatype, rp_datatype_tag, rp_datatype_sens)
    # 1_8 分类分级标签
    str_1_8 = ("分类分级标签：本次任务共发现<span style='color: #0079fe'>{0}</span>个数据类型标签，"
               "涉及<span style='color: #0079fe'>{1}</span>个数据分类目录（最末级），"
               "其中最高分级为{2}级，在数据类型标签总数中占比{3}%")
    rp_value["1_8"] = str_1_8.format(rp_tag, rp_cata, rp_class, rp_rate)
    # 1_9 安全风险
    str_1_9 = "安全风险：本次任务发现<span style='color: #0079fe'>{0}</span>个应脱敏或应加密字段，未脱敏或未加密。存在安全风险！"
    rp_value["1_9"] = str_1_9.format(rp_densenssec)
    # 2_1 资产基本信息
    rp_value["2_1"] = str_2_1
    # 2_2 系统分组统计(固定值)
    rp_value["2_2"] = str_group
    # 2_3 部门分组统计(固定值)
    rp_value["2_3"] = str_group
    # 2_4 区域分组统计(固定值)
    rp_value["2_4"] = str_group
    # 2_5 数据资产类型统计
    rp_value["2_5"] = str_2_5
    # 2_6 资产库表统计
    rp_value["str_2_6"] = str_2_6

    # 2_7 数据类型标签统计
    rp_value["2_7"] = str_2_7

    # 2_8 分类分级标签统计
    rp_value["2_8"] = str_2_8

    # 2_9 分级标签统计
    rp_value["str_2_9"] = str_2_9

    return json.dumps(rp_value, ensure_ascii=False)


rp_value = get_rp_value()
# 打印报告原始文本
print(rp_value)
