#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2024
# @generate at 2024/10/29 08:53
# comment: pdf导出

import mysql.connector
import configparser
import json
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib import colors
from reportlab.platypus import Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

# 数据库连接定义
config = configparser.ConfigParser()
config.read("../conf/db.cnf")

my_host = config.get("dcg_task_test", "host")
my_database = config.get("dcg_task_test", "database")
my_user = config.get("dcg_task_test", "user")
my_password = config.get("dcg_task_test", "password")

pdfmetrics.registerFont(TTFont('SimHei', '../priv/SimHei.ttf'))
pdfmetrics.registerFont(TTFont('SimSun', '../priv/SimSun.ttf'))
pdfmetrics.registerFont(TTFont('SimSun-Bold', '../priv/SimSun-Bold.ttf'))
con = mysql.connector.connect(
    host=my_host, user=my_user, password=my_password, database=my_database
)

try:
    # 数据读取
    sel_sql = "SELECT data_json FROM tb_report WHERE task_name='车务系统' ORDER BY create_time DESC LIMIT 1;"
    cur = con.cursor(dictionary=True)

    cur.execute(sel_sql)
    data_res = cur.fetchone()

    data_json = data_res['data_json']
    data_dict = json.loads(data_json)
    data_0_3 = data_dict.get("0_3", "未知系统")

    # 外部文件定义
    json_out_file = '../file/{0}分类分级报告_{1}.pdf'.format(data_0_3, datetime.now().year)
    logo_path = '../priv/avatar.png'

    # 创建文档
    c = canvas.Canvas(json_out_file, pagesize=letter)
    width, height = letter

    data_0_1 = data_dict.get("0_1", "未知编号")
    data_0_2 = data_dict.get("0_2", "未知日期")

    # 首页绘制
    # 插入 logo
    logo_width = 200
    c.drawImage(logo_path, (width - logo_width) / 2, height - 200, width=logo_width, height=130,
                preserveAspectRatio=True, mask='auto')

    # 标题
    c.setFont("SimSun-Bold", 30)
    c.drawCentredString(width / 2.0, height - 250, "分类分级任务报告")

    # 系统名称
    c.setFont("SimSun-Bold", 22)
    c.drawCentredString(width / 2.0, height - 310, data_0_3)

    # 报告详情
    c.setFont("SimSun-Bold", 12)

    c.drawCentredString(width / 2.0, height - 480, f"报告编号 {data_0_1}")
    c.drawCentredString(width / 2.0, height - 510, f"报告日期 {data_0_2}")

    # 分页
    c.showPage()

    # 第一章 分类分级任务统计报告概述
    # 标题
    c.setFont("SimHei", 18)
    c.drawString(50, height - 50, "一、分类分级任务统计报告概述")

    # 表格1
    styles = getSampleStyleSheet()
    my_style = ParagraphStyle(
        name='TableStyle',
        parent=styles['BodyText'],
        fontName='SimSun',
        fontSize=12
    )

    report_conclusion = "<br/>".join([
        data_dict.get("0_4", ""),
        data_dict.get("1_2", ""),
        data_dict.get("1_3", ""),
        data_dict.get("1_4", "").replace("<span style='color: #0079fe'>", "").replace("</span>", ""),
        data_dict.get("1_5", "").replace("<span style='color: #0079fe'>", "").replace("</span>", ""),
        data_dict.get("1_6", "").replace("<span style='color: #0079fe'>", "").replace("</span>", ""),
        data_dict.get("1_7", "").replace("<span style='color: #0079fe'>", "").replace("</span>", ""),
        data_dict.get("1_8", "").replace("<span style='color: #0079fe'>", "").replace("</span>", ""),
        data_dict.get("1_9", "").replace("<span style='color: #0079fe'>", "").replace("</span>", "")
    ]).replace("\n\n", "\n")

    data_table_1 = [["任务名称", data_dict.get("0_3", "")],
                    ["报告类型", "分类分级报告"],
                    ["报告内容",
                     Paragraph("依据国家相关法规及规范对数据安全治理的指导和要求，通过预先明确并定义发现模板的对象及策略，对资产中敏感数据进行全面检出分析，"
                               "汇总发现的敏感信息结果并生成整敏感数据发现报告", my_style)],
                    ["参考依据",
                     "《中华人民共和国网络安全法》\n《网络安全等级保护条例》\n《数据安全法》\n《信息安全技术 数据安全能力成熟度模型》\n《信息安全技术 个人信息安全规范》"
                     "\n《信息安全技术 大数据安全管理指南》\n《关键信息基础设施安全保护条例征求意见稿》等"],
                    ["报告结论", Paragraph(report_conclusion, my_style)],
                    ["开始时间", data_dict.get("1_10", "")],
                    ["结束时间", data_dict.get("1_11", "")],
                    ["总时长", data_dict.get("1_12", "")],
                    ["操作人员", data_dict.get("1_13", "")]]

    # 绘制表格
    table_1 = Table(data_table_1, colWidths=[80, 420])
    table_1.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, -1), 'SimSun'),
        ('FONTSIZE', (0, 0), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))

    # 插入到 PDF
    table_1.wrapOn(c, width, height)
    table_1.drawOn(c, 50, height - 600)

    # 分页
    c.showPage()

    # 第二章 分类分级统计信息
    # 标题
    c.setFont("SimHei", 18)
    c.drawString(50, height - 50, "二、分类分级统计信息")

    # 2.1
    c.setFont("SimHei", 16)
    c.drawString(50, height - 100, "2.1资产基本信息")
    data_table_2_1 = [["序号", "数据资产名称", "地址", "所属系统", "所属部门", "所属区域"],
                      ["合计", data_dict.get("2_1", "")[0].get("key1", ""),
                       data_dict.get("2_1", "")[0].get("key2", ""),
                       data_dict.get("2_1", "")[0].get("key3", ""),
                       data_dict.get("2_1", "")[0].get("key4", ""),
                       data_dict.get("2_1", "")[0].get("key5", ""),
                       ]]
    # 绘制表格
    table_2_1 = Table(data_table_2_1, colWidths=[40, 80, 152, 80, 80, 80])
    table_2_1.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, -1), 'SimSun'),
        ('FONTSIZE', (0, 0), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))

    # 插入到 PDF
    table_2_1.wrapOn(c, width, height)
    table_2_1.drawOn(c, 50, height - 180)

    # 2.2
    c.setFont("SimHei", 16)
    c.drawString(50, height - 230, "2.2系统分组统计")
    data_table_2_2 = [["序号", "部门名称", "资产数量", "占比情况"]]
    data_list_2_2 = data_dict.get("2_2", "")
    for index, data_2_2 in enumerate(data_list_2_2, start=1):
        data_2_2_s = [index, data_2_2.get("key1", ""), data_2_2.get("key2", ""), data_2_2.get("key3", "")]
        data_table_2_2.append(data_2_2_s)

    data_table_2_2[-1][0] = "合计"

    # 绘制表格
    table_2_2 = Table(data_table_2_2, colWidths=[40, 158, 157, 157])
    table_2_2.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, -1), 'SimSun'),
        ('FONTSIZE', (0, 0), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))

    # 插入到 PDF
    table_2_2.wrapOn(c, width, height)
    table_2_2.drawOn(c, 50, height - 320)

    # TODO 后面章节的PDF格式拼接

    # 保存 PDF
    c.save()

except Exception as e:
    print(e)
finally:
    if cur:
        cur.close()
    if con:
        con.close()
