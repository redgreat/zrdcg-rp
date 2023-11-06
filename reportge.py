#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw 
# @generate at 2023/10/30 09:57

from datetime import datetime, timedelta
from random import randint
import json

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
str_2_1 = {
    "key1": "消息中心",
    "key2": "rm-bp175bls4muud11z6.mysql.rds.aliyuncs.com:3306",
    "key3": "未分组",
    "key4": "未分组",
    "key5": "未分组",
    "key6": "",
    "key7": "",
    "key8": "",
    "key9": "",
    "key_list": None
}

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
str_2_6 = [
    {
        "key1": "消息中心",
        "key2": "MySQL",
        "key3": "1",
        "key4": "{0}",
        "key5": "{1}",
        "key6": "{2}",
        "key7": "{3}",
        "key8": "",
        "key9": "",
        "key_list": None
    },
    {
        "key1": "1",
        "key2": "",
        "key3": "1",
        "key4": "{0}",
        "key5": "{1}",
        "key6": "{2}",
        "key7": "{3}",
        "key8": "",
        "key9": "",
        "key_list": None
    }
]
str_2_6_table = 1
str_2_6_column = 1
str_2_6_class = 'D'

# 分级标签
str_2_9 = [
    {
      "key1": "A级",
      "key2": "敏感",
      "key3": "1",
      "key4": "0.53%",
      "key5": "",
      "key6": "",
      "key7": "",
      "key8": "",
      "key9": "",
      "key_list": None
    },
    {
      "key1": "B级",
      "key2": "非敏感",
      "key3": "1",
      "key4": "0.53%",
      "key5": "",
      "key6": "",
      "key7": "",
      "key8": "",
      "key9": "",
      "key_list": None
    },
    {
      "key1": "C级",
      "key2": "非敏感",
      "key3": "1",
      "key4": "0.53%",
      "key5": "",
      "key6": "",
      "key7": "",
      "key8": "",
      "key9": "",
      "key_list": None
    },
    {
      "key1": "D级",
      "key2": "非敏感",
      "key3": "185",
      "key4": "98.93%",
      "key5": "",
      "key6": "",
      "key7": "",
      "key8": "",
      "key9": "",
      "key_list": None
    },
    {
      "key1": "3",
      "key2": "",
      "key3": "187",
      "key4": "100%",
      "key5": "",
      "key6": "",
      "key7": "",
      "key8": "",
      "key9": "",
      "key_list": None
    }
  ]

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

    # 2_8 分类分级标签统计

    # 2_9 分级标签统计
    rp_value["str_2_9"] = str_2_9

    return json.dumps(rp_value, ensure_ascii=False)


rp_value = get_rp_value()
# 打印报告原始文本
print(rp_value)
