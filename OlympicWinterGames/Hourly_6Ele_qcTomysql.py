#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/22
description:
"""
import os,MySQLdb
db=MySQLdb.connect('192.168.1.84','admin','moji_China_123','olympic')
cursor=db.cursor()
path='/Users/yetao.lu/Desktop/冬奥会/data2/Hourly_6Ele_qc'
for rootpath,dirs,files in os.walk(path):
    for file in files:
        if file[-3:]=='txt':
            filepath=os.path.join(rootpath,file)
            print filepath
            fileread=open(filepath,'r')
            while True:
                perline=fileread.readline()
                perArray=perline.split()
                if (len(perArray))>3:
                    stationid=perArray[1]
                    observationtime=perArray[0]
                    if perArray[2]=='///':
                        wd_2m=999999
                    else:
                        wd_2m = int(perArray[2])
                    if perArray[3]=='///':
                        ws_2m = 999999
                    else:
                        ws_2m=float(perArray[3]) * 0.1
                    if perArray[4]=='////':
                        pre_1h=999999
                    else:
                        pre_1h = float(perArray[4]) * 0.1
                    if perArray[5]=='////':
                        temp = 999999
                    else:
                        temp = float(perArray[5]) * 0.1
                    if perArray[6]=='///':
                        rh = 999999
                    else:
                        rh = int(perArray[6])
                    if perArray[7]=='/////':
                        prs=999999
                    else:
                        prs = float(perArray[7]) * 0.1
                    qccode=int(perArray[8])
                    sql='insert into Hourly_6Ele_qc (stationID,observatedate,wd_2m,ws_2m,pre_1h,temp,rh,prs,qccode)VALUES ("'+stationid+'","'+observationtime+'","'+str(wd_2m)+'","'+str(ws_2m)+'","'+str(pre_1h)+'","'+str(temp)+'","'+str(rh)+'","'+str(prs)+'","'+str(qccode)+'")'
                    print sql
                    cursor.execute(sql)
                    db.commit()
                if not perline:
                    break
db.close()
            