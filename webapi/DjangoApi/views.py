# coding:utf-8
from django.shortcuts import render
from django.http import HttpResponse
import pymysql
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import pandas as pd

# Create your views here.

def index(request):
    return HttpResponse(u"Django Api Test !")

def sql(request):
    sql_text = request.GET.get("sql","")
    db = pymysql.connect(host="namenode", user="root", passwd="123456", db="test", charset='utf8')

    cur = db.cursor()

    sql = f"{sql_text}"

    cur.execute(sql)

    text = cur.fetchall()

    print(text)

    cur.close()
    return HttpResponse(text)

def listToStrHead(li):
    text = ""
    for i in li:
        text += "<th>" + i + "</th>"
    return text

def listToStrBody(li):
    data_len = len(li[0])
    text = ""
    for i in li:
        for j in range(data_len):
            text += "<td>" + str(i[j]) + "</td>"
        text = "<tr>" + text + "</tr>"
    return text



def presto(request):
    sql_text = request.GET.get("sql", "")
    engine = create_engine('presto://bdmaster1:9999/hive/default')  # host是服务器ip，port是端口，hive指的是Presto的catalog，my_schema是hive的schema。
    df = pd.read_sql(f"{sql_text}", engine)  # 和一般pandas从数据库中读取数据无任何区别，分析师们应该非常熟悉了。
    cols = listToStrHead(df.columns)
    data = listToStrBody(df.values)
    text = """<html><body><table border="1">""" + cols + data + "</table></body></html>"
    return HttpResponse(text)