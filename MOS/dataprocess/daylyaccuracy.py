#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/17
description:计算mos统计气温均值，最大值，最小值的准确率
"""
import os,MySQLdb
if __name__ == "__main__":
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
    cursor=db.cursor()
    sql='select a.avg_preds,a.max_preds,min_preds,a.avg_origin,a.max_origin,a.min_origin,b.max_temp,b.min_temp,b.avg_temp from mosdayly a,mos_livemaxmindayly b where a.stationid=b.stationid and a.origintime =b.livetime and a.foretimes="6"'
    cursor.execute(sql)
    rows=cursor.fetchall()
    avg_predslist=[]
    max_predslist=[]
    min_predslist=[]
    avg_originlist=[]
    max_originlist=[]
    min_originlist=[]
    avg_livelist=[]
    max_livelist=[]
    min_livelist=[]
    a=0
    print len(rows)
    for row in rows:
        a=a+1
        avg_predslist.append(row[0])
        max_predslist.append(row[1])
        min_predslist.append(row[2])
        avg_originlist.append(row[3])
        max_originlist.append(row[4])
        min_originlist.append(row[5])
        avg_livelist.append(row[6])
        max_livelist.append(row[7])
        min_livelist.append(row[8])
n=0
for x,y in zip(avg_livelist,avg_originlist):
    if abs(x-y)<3:
        n=n+1
accuracy1=float(n)/float(len(avg_originlist))
n=0
for x,y in zip(max_originlist,max_livelist):
    if abs(x-y)<3:
        n=n+1
accuracy2=float(n)/float(len(max_originlist))
n=0
for x,y in zip(min_originlist,min_livelist):
    if abs(x-y)<3:
        n=n+1
accuracy3=float(n)/float(len(min_originlist))
n=0
for x,y in zip(avg_predslist,avg_livelist):
    if abs(x-y)<3:
        n=n+1
accuracy4=float(n)/float(len(avg_predslist))
n=0
for x,y in zip(max_predslist,max_livelist):
    if abs(x-y)<3:
        n=n+1
accuracy5=float(n)/float(len(max_predslist))
n=0
for x,y in zip(min_predslist,min_livelist):
    if abs(x-y)<3:
        n=n+1
accuracy6=float(n)/float(len(min_predslist))
print accuracy1,accuracy2,accuracy3,accuracy4,accuracy5,accuracy6,a