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

src_host = config.get("src_dcg_db", "host")
src_database = config.get("src_dcg_db", "database")
src_user = config.get("src_dcg_db", "user")
src_password = config.get("src_dcg_db", "password")

con = mysql.connector.connect(
    host=my_host, user=my_user, password=my_password, database=my_database
)


# 报告生成
def get_rp_value(import_stamp: str):
    cur = con.cursor(dictionary=True)

    # 常量值定义
    time_now = datetime.now()

    rp_no_prifix = 'FLFJ'
    rp_no_date = datetime.strftime(time_now, '%y%m%d')
    rp_no_rand = str(randint(1000, 9999))
    rp_no = rp_no_prifix + '000' + rp_no_date + rp_no_rand  # 报告编号

    str_now = datetime.strftime(time_now, '%Y-%m-%d %H:%M:%S')  # 报告日期

    rp_name = '壹好车服'  # 报告涉及系统

    rp_dura = 10  # 报告生成  总时长
    rp_start = datetime.strftime(time_now - timedelta(seconds=rp_dura), '%Y-%m-%d %H:%M:%S')  # 报告导出 开始时间
    rp_end = str_now  # 报告导出 结束时间
    rp_author = '王存伟'  # 操作人员

    # 分类分级任务运行结果中获取值
    rp_densenssec = 0  # 安全风险，应脱敏或加密数量

    sql_itc_1 = ("SELECT COUNT(DISTINCT AssetId) AS AssenCnt,IF(MAX(S.TableSens)>0,1,0) AS AssetSensCnt, "
                 "COUNT(DISTINCT S.TableName) AS TableCnt, "
                 "SUM(S.TableSens) AS TableSensCnt,SUM(S.ColumnCnt) AS ColumnCnt,"
                 "SUM(S.ColumnSensCnt) AS ColumnSensCnt FROM (SELECT AssetId,"
                 "SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1) AS TableName, "
                 "IF(SUM(IF(IsSens='是',1,0))>0,1,0) AS TableSens, COUNT(*) AS ColumnCnt, "
                 "SUM(IF(IsSens='是',1,0)) AS ColumnSensCnt FROM tb_template_imports "
                 "WHERE ImportStamp=%s "
                 "GROUP BY SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1)) AS S;"
                 )

    sql_tag_1 = ("SELECT COUNT(DISTINCT S.TagName) AS TagCnt, "
                 "ROUND(((SUM(S.TagGrade)/COUNT(DISTINCT S.TagName)) * 100), 2) AS TagGradeRate, "
                 "MIN(S.DataGrade) AS TopGrade,COUNT(S.DataTypeName) AS TypeCnt "
                 "FROM (SELECT Class4 AS TagName,DataTypeName,SUM(IF(DataGrade='A',1,0)) AS TagGrade, "
                 "MIN(IF(DataGrade NOT IN ('A','B','C','D'),'D',DataGrade)) AS DataGrade "
                 "FROM tb_template_imports "
                 "WHERE ImportStamp=%s GROUP BY DataTypeName) AS S;"
                 )

    try:
        cur.execute(sql_itc_1,[import_stamp])
        res_itc_1 = cur.fetchall()
        if res_itc_1:
            rp_assets = res_itc_1[0]['AssenCnt']  # 数据资产总数
            rp_asset_sens = res_itc_1[0]['AssetSensCnt']  # 数据资产敏感字段数量
            rp_databases = res_itc_1[0]['AssetSensCnt']  # 数据库/schema数量
            rp_database_sens = res_itc_1[0]['AssetSensCnt']  # 数据库/schema敏感数量
            rp_tables = res_itc_1[0]['TableCnt']  # 表数量数量
            rp_table_sens = res_itc_1[0]['TableSensCnt']  # 表敏感数量
            rp_columns = res_itc_1[0]['ColumnCnt']  # 分类分级标签数量
            rp_column_sens = res_itc_1[0]['ColumnSensCnt']  # 数据类型标签敏感数量
        cur.execute(sql_tag_1,[import_stamp])
        res_tag_1 = cur.fetchall()
        if res_tag_1:
            rp_type = res_tag_1[0]['TypeCnt']  # 数据类型数量
            rp_tag = res_tag_1[0]['TagCnt']  # 分类分级目录数量
            rp_class = res_tag_1[0]['TopGrade']  # 分类分级最高分级
            rp_rate = res_tag_1[0]['TagGradeRate']  # 数据类型标签占比
    except Exception as e:
        print(e)

    # 任务信息获取
    # 实例信息
    sql_2_1 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.AssetName, 'key2', S.DBAddress, 'key3', '未分组', "
               "'key4', '未分组', 'key5', '未分组', 'key6', NULL, "
               "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
               "FROM (SELECT DISTINCT A.AssetName,B.DBAddress "
               "FROM tb_template_imports A "
               "LEFT JOIN zr_dcg_asset.tb_asset_instance B "
               "ON B.AssetId=A.AssetId "
               "AND B.Deleted=0 WHERE A.ImportStamp=%s) AS S;"
               )

    try:
        str_2_1 = []
        cur.execute(sql_2_1,[import_stamp])
        res_2_1 = cur.fetchall()
        if res_2_1:
            str_2_1 = json.loads(res_2_1[0]['data'])
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
               "FROM tb_template_imports WHERE ImportStamp=%s "
               "AND IsSens = '是' "
               "GROUP BY SUBSTRING_INDEX(DataColumns, '.', 1)) CO "
               "ON CO.DBName=SUBSTRING_INDEX(A.DataColumns, '.', 1) "
               "LEFT JOIN (SELECT SUBSTRING_INDEX(DataColumns, '.', 1) AS DBName, "
               "COUNT(DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1)) AS TBCnt, "
               "COUNT(DISTINCT (IF(IsSens='是',SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1),NULL))) "
               "AS SensCnt FROM tb_template_imports WHERE ImportStamp=%s "
               "GROUP BY SUBSTRING_INDEX(DataColumns, '.', 1)) AS COS "
               "ON COS.DBName=SUBSTRING_INDEX(A.DataColumns, '.', 1) "
               "WHERE A.ImportStamp=%s GROUP BY SUBSTRING_INDEX(A.DataColumns, '.', 1)) AS S;"
               )

    sql_all_2_6 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.AssetCnt, 'key2', NULL, 'key3', S.DBCnt, "
                   "'key4', S.SensRate, 'key5', S.SensTotal, 'key6', NULL, "
                   "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
                   "FROM (SELECT COUNT(DISTINCT A.AssetId) AS AssetCnt, "
                   "COUNT(DISTINCT SUBSTRING_INDEX(DataColumns, '.', 1)) AS DBCnt, "
                   "(SELECT CONCAT(COS.SensCnt,'/',COS.TBCnt,'--',ROUND(((COS.SensCnt / COS.TBCnt) * 100), 2),'%') "
                   "FROM (SELECT COUNT(DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1)) AS TBCnt, "
                   "COUNT(DISTINCT (IF(IsSens='是',SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1),NULL))) "
                   "AS SensCnt FROM tb_template_imports WHERE ImportStamp=%s) "
                   "AS COS) AS SensRate, (SELECT COUNT(*) SensTotal "
                   "FROM tb_template_imports WHERE ImportStamp=%s "
                   "AND IsSens = '是') AS SensTotal "
                   "FROM tb_template_imports A "
                   "LEFT JOIN zr_dcg_asset.tb_asset_instance B "
                   "ON B.AssetId=A.AssetId "
                   "AND B.Deleted=0 "
                   "LEFT JOIN zr_dcg_asset.tb_asset_database C "
                   "ON C.AssetInsId=B.Id "
                   "AND C.DBName COLLATE utf8mb4_general_ci= "
                   "SUBSTRING_INDEX(DataColumns, '.', 1) COLLATE utf8mb4_general_ci "
                   "AND C.Deleted=0 WHERE A.ImportStamp=%s) AS S"
                   )

    try:
        str_2_6 = []
        str_all_2_6 = []
        cur.execute(sql_2_6,[import_stamp, import_stamp, import_stamp])
        res_2_6 = cur.fetchall()
        if res_2_6:
            str_2_6 = json.loads(res_2_6[0]['data'])
        cur.execute(sql_all_2_6,[import_stamp, import_stamp, import_stamp])
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
               "WHERE ImportStamp=%s) ) * 100),2),'%') AS TotalRate "
               "FROM tb_template_imports "
               "WHERE ImportStamp=%s "
               "GROUP BY DataTypeName) AS S;"
               )

    sql_all_2_7 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.DataTypeCnt, 'key2', NULL, 'key3', NULL, "
                   "'key4', NULL, 'key5', NULL, 'key6', S.TotalCnt, "
                   "'key7', '100%', 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
                   "FROM (SELECT COUNT(DISTINCT DataTypeName) AS DataTypeCnt, COUNT(*) AS TotalCnt "
                   "FROM tb_template_imports WHERE ImportStamp=%s) AS S;"
                   )

    try:
        str_2_7 = []
        str_all_2_7 = []
        cur.execute(sql_2_7,[import_stamp, import_stamp])
        res_2_7 = cur.fetchall()
        if res_2_7:
            str_2_7 = json.loads(res_2_7[0]['data'])
        cur.execute(sql_all_2_7,[import_stamp])
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
               "WHERE ImportStamp=%s) ) * 100),2),'%') AS TotalRate "
               "FROM tb_template_imports "
               "WHERE ImportStamp=%s "
               "GROUP BY Class4) AS S;"
               )

    sql_all_2_8 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.Class4Cnt, 'key2', NULL, 'key3', NULL, "
                   "'key4', NULL, 'key5', NULL, 'key6', S.TotalCnt, "
                   "'key7', '100%', 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
                   "FROM (SELECT COUNT(DISTINCT Class4) AS Class4Cnt, COUNT(*) AS TotalCnt "
                   "FROM tb_template_imports WHERE ImportStamp=%s) AS S;"
                   )

    try:
        str_2_8 = []
        str_all_2_8 = []
        cur.execute(sql_2_8,[import_stamp, import_stamp])
        res_2_8 = cur.fetchall()
        if res_2_8:
            str_2_8 = json.loads(res_2_8[0]['data'])
        cur.execute(sql_all_2_8,[import_stamp])
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
               "WHERE ImportStamp=%s) ) * 100),2),'%') AS TotalRate "
               "FROM tb_template_imports "
               "WHERE ImportStamp=%s "
               "GROUP BY DataGrade) AS S; "
               )

    sql_all_2_9 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.GreadeCnt, 'key2', NULL, 'key3', TotalCnt, "
                   "'key4', '100%', 'key5', NULL, 'key6', NULL, "
                   "'key7', NULL, 'key8', NULL, 'key9', NULL, 'key_list', NULL)) AS data "
                   "FROM (SELECT COUNT(DISTINCT DataGrade) AS GreadeCnt, COUNT(*) AS TotalCnt "
                   "FROM tb_template_imports WHERE ImportStamp=%s) AS S;"
                   )

    try:
        str_2_9 = []
        str_all_2_9 = []
        cur.execute(sql_2_9,[import_stamp, import_stamp])
        res_2_9 = cur.fetchall()
        if res_2_9:
            str_2_9 = json.loads(res_2_9[0]['data'])
        cur.execute(sql_all_2_9,[import_stamp])
        res_all_2_9 = cur.fetchall()
        if res_all_2_9:
            str_all_2_9 = json.loads(res_all_2_9[0]['data'])
        str_2_9.append(str_all_2_9[0])
    except Exception as e:
        print(e)

    # 分类分级详情

    sql_3 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.DataTypeName, 'key2', S.TagCnt, 'key3', S.TypePath, "
             "'key4', S.DataGrade, 'key5', S.ControlRule, 'key6', S.GradeComment, "
             "'key7', NULL, 'key8', NULL, 'key9', TagCnt, 'key_list', JSON_ARRAY_APPEND(TagPath,'$',TagPathCnt))) AS data "
             "FROM(SELECT DataTypeName,COUNT(*) AS TagCnt, "
             "CONCAT('/', MAX(Class4)) AS TypePath, "
             "CASE MIN(IF(DataGrade NOT IN ('A','B','C','D'),'D',DataGrade)) "
             "WHEN 'A' THEN 'A级(第一级,敏感),' "
             "WHEN 'B' THEN 'B级(第二级,非敏感),' "
             "WHEN 'C' THEN 'C级(第三级,非敏感),' "
             "WHEN 'D' THEN 'D级(第四级,非敏感),' "
             "END AS DataGrade, "
             "CASE MIN(IF(DataGrade NOT IN ('A','B','C','D'),'D',DataGrade)) "
             "WHEN 'A' THEN '对【A级】数据应实施较严格的技术和管理措施，保护数据的机密性和完整性，确保数据访问控制安全，建立数据安全管理规范以及数据准实时监控机制。【A级】数据在满足相关条件的前提下，可以对外开放。'"
             "WHEN 'B' THEN '对【B级】数据应实施必要的技术和管理措施，确保数据生命周期安全，建立数据安全管理规范。【B级】数据在满足相关条件的前提2下，可以对外开放。' "
             "WHEN 'C' THEN '对【C级】数据应实施基本的技术和管理措施，确保数据生命周期安全。【C级】数据可以直接对外开放，但需要考虑对外开放的数据量及类别，避免由于类别较多或者数据量过大，导致能够用于关联分析。' "
             "WHEN 'D' THEN '对【D级】数据应实施基本的技术和管理措施，确保数据生命周期安全。【D级】数据可以直接对外开放，但需要考虑对外开放的数据量及类别，避免由于类别较多或者数据量过大，导致能够用于关联分析。' "
             "END AS ControlRule, "
             "CASE MIN(IF(DataGrade NOT IN ('A','B','C','D'),'D',DataGrade)) "
             "WHEN 'A' THEN '1、数据的安全属性（完整性、保密性、可用性）遭到破坏损失后，影响范围中等（一般局限在本机构），影响程度一般是“严重” 。\n2、一般特征：数据用于重要业务使用，一般针对特定人员公开，且仅为必须知悉的对象访问或使用。\n重要数据包括但不限于：\n车辆流量、物流等反映经济运行情况的数据；\n涉及个人信息主体超过10万人的个人信息；\n企业核心机密、战略规划、产品方案、经营管理机密信息；\n个人身份证明、个人联系信息、个人身份鉴权信息(传统身份鉴权、车联网身份鉴权);企业身份鉴权信息；' "
             "WHEN 'B' THEN '1、数据的安全属性（完整性、保密性、可用性）遭到破坏损失后，影响范围中等（一般局限在本机构），影响程度一般是“一般” 。\n2、一般特征：数据用于重要业务使用，一般针对特定人员公开，且仅为必须知悉的对象访问或使用。\n包括但不限于：个人基本信息、地理位置信息、车辆基本信息、个人信贷保险信息；企业核心机密、战略规划、产品方案信息、业务交易信息、生产研发信息、员工非公开信息、员工薪资信息、经营管理信息(财务收支、税务、资金往来等)；' "
             "WHEN 'C' THEN '1、数据的安全属性（完整性、保密性、可用性）遭到破坏损失后，影响范围较小（一般局限在本机构），影响程度一般是“轻微”或“无” 。\n2、一般特征：数据可被公开或可被公众获知、使用。\n包括但不限于：个人一般信息、个人行为信息；企业联系信息、财务信息、业务合约信息、企业一般交易信息、生产研发信息、经营管理一般信息；' "
             "WHEN 'D' THEN '1、数据的安全属性（完整性、保密性、可用性）遭到破坏后数据损失后，影响范围几乎可忽略不计（一般局限在本机构），影响程度基本为“无” 。\n2、一般特征：数据可被公开或可被公众获知、使用、共享。' "
             "END AS GradeComment, "
             "JSON_ARRAYAGG(CONCAT(AssetName,'-->',SUBSTRING_INDEX(DataColumns, '.', 1),'-->', "
             "SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 2), '.', -1),'-->', "
             "SUBSTRING_INDEX(SUBSTRING_INDEX(DataColumns, '.', 3), '.', -1))) AS TagPath, "
             "CONCAT('等 ',COUNT(*),' 个字段') AS TagPathCnt "
             "FROM tb_template_imports "
             "WHERE ImportStamp=%s "
             "GROUP BY DataTypeName) AS S;"
             )

    try:
        str_3 = []
        cur.execute(sql_3,[import_stamp])
        res_3 = cur.fetchall()
        if res_3:
            str_3 = json.loads(res_3[0]['data'])
    except Exception as e:
        print(e)

    # 4-1
    str_4_1 = rp_name

    # 4-2
    str_4_2 = rp_name + '分类分级'

    # 4-3
    sql_4_3 = ("SELECT JSON_ARRAYAGG(JSON_OBJECT('key1', S.ClassName, 'key2', S.DataTypeName, 'key3', S.DataGrade, "
               "'key4', S.IsSens, 'key5', '', 'key6', '', "
               "'key7', '', 'key8', '', 'key9', '', 'key_list', NULL)) AS data "
               "FROM (SELECT CONCAT(' / ', Class4) AS ClassName, DataTypeName,DataGrade, "
               "CASE IsSens WHEN '是' THEN '敏感' WHEN '否' THEN '非敏感' ELSE '非敏感' END AS IsSens "
               "FROM tb_template_imports WHERE ImportStamp=%s) AS S;"
               )
    try:
        str_4_3 = []
        cur.execute(sql_4_3,[import_stamp])
        res_4_3 = cur.fetchall()
        if res_4_3:
            str_4_3 = json.loads(res_4_3[0]['data'])
    except Exception as e:
        print(e)

    # 5-1
    str_5_1 = {
        "key1": "未发现应加密未加密数据，暂无数据安全风险！",
        "key2": "0",
        "key3": "",
        "key4": "",
        "key5": "",
        "key6": "",
        "key7": "",
        "key8": "",
        "key9": "",
        "key_list": [
            "无"
        ]
    }
    # 5-2
    str_5_2 = {
        "key1": "未发现应脱敏未脱敏数据，暂无数据安全风险！",
        "key2": "0",
        "key3": "",
        "key4": "",
        "key5": "",
        "key6": "",
        "key7": "",
        "key8": "",
        "key9": "",
        "key_list": [
            "无"
        ]
    }

    # 5-3
    str_5_3 = {
        "key1": "未发现应脱敏或加密数据，未进行脱敏或加密，暂无数据安全风险！",
        "key2": "0",
        "key3": "",
        "key4": "",
        "key5": "",
        "key6": "",
        "key7": "",
        "key8": "",
        "key9": "",
        "key_list": [
            "无"
        ]
    }

    # 更新报表字段
    sql_ins = ("INSERT INTO tb_report (task_id, task_name, template_id, template_name, status, data_json, "
              "task_create_id, task_create_name, create_time, update_time, report_number) VALUES (1, %s, "
              "1, %s, 1, %s, 1, %s, NOW(), NOW(), %s);"
              )

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
    rp_value["1_6"] = str_1_6.format(rp_tables, rp_table_sens)
    # 1_7 数据类型
    str_1_7 = ("数据类型：本次任务共涉及<span style='color: #0079fe'>{0}</span>个数据字段，"
               "有<span style='color: #0079fe'>{1}</span>个字段命中数据类型标签，"
               "其中敏感字段为<span style='color: #0079fe'>{2}</span>个")
    rp_value["1_7"] = str_1_7.format(rp_columns, rp_columns, rp_column_sens)
    # 1_8 分类分级标签
    str_1_8 = ("分类分级标签：本次任务共发现<span style='color: #0079fe'>{0}</span>个数据类型标签，"
               "涉及<span style='color: #0079fe'>{1}</span>个数据分类目录（最末级），"
               "其中最高分级为{2}级，在数据类型标签总数中占比{3}%")
    rp_value["1_8"] = str_1_8.format(rp_type, rp_tag, rp_class, rp_rate)
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
    rp_value["2_6"] = str_2_6
    # 2_7 数据类型标签统计
    rp_value["2_7"] = str_2_7
    # 2_8 分类分级标签统计
    rp_value["2_8"] = str_2_8
    # 2_9 分级标签统计
    rp_value["2_9"] = str_2_9
    # 分类分级详情
    rp_value["3"] = str_3
    # 分类分级任务名称
    rp_value["4_1"] = str_4_1
    # 应用分类分级模板
    rp_value["4_2"] = str_4_2
    # 分类分级模板详情
    rp_value["4_3"] = str_4_3
    # 应加密未加密
    rp_value["5_1"] = str_5_1
    # 应脱敏未脱敏
    rp_value["5_2"] = str_5_2
    # 应脱敏或加密，但未脱敏或未加密
    rp_value["5_3"] = str_5_3

    rp_json = json.dumps(rp_value, ensure_ascii=False)
    cur.execute(sql_ins, [rp_name, (rp_name + '分类分级'), rp_json, rp_author, rp_no])

    con.commit()
    cur.close()


get_rp_value('6ac3bb98-7d17-11ee-a46d-0c42a163ddf4')

con.close()
