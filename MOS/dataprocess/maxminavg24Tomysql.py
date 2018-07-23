#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/16
description:将mos统计依赖的实况数据按照每天气温均值、最高气温、最低气温录入的数据库
"""
import os,MySQLdb
if __name__ == "__main__":
    csvpath='/Users/yetao.lu/Desktop/mos/maxmin'
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
    for rootpath,dirs,files in os.walk(csvpath):
        for file in files:
            filedatalist = []

            cursor = db.cursor()
            timestring=file[6:14]+'12'
            print timestring
            filepath=os.path.join(rootpath,file)
            if filepath[-3:]=='csv':
                fileRead=open(filepath,'r')
                while True:
                    line=fileRead.readline()
                    linelist=line.split(',')
                    perlist = []
                    if len(linelist)>3:
                        perlist.append(timestring)
                        for i in range(len(linelist)):
                            if i==0:
                                perlist.append(linelist[i])
                            else:
                                perlist.append(float(linelist[i]))
                        #print perlist
                        filedatalist.append(perlist)
                    if not line:
                        break
            print len(filedatalist)
            #print filedatalist
            print perlist
            sql = 'INSERT INTO mos_livemaxmindayly(livetime,stationid,max_temp,min_temp,avg_temp) values(%s,%s,%s,%s,%s)'
            cursor.executemany(sql,filedatalist)
            print filepath
            db.commit()
    db.close()
