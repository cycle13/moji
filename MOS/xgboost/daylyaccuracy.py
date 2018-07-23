#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/14
description:本部分为将MOS统计的结果，四个矩阵防入数据库中其中包括ID矩阵，EC原始数据矩阵，实况检验样本矩阵，MOS之后预报样本矩阵
"""
import os,MySQLdb
def ReadCSV(i,csvpath):
    kidcsv00=os.path.join(csvpath,'kid'+str(i)+'.csv')
    kidfile=open(kidcsv00,'r')
    fileread=kidfile.read()
    kidarray=fileread.split(',')
    print len(kidarray)
    origincsv=os.path.join(csvpath,'origin'+str(i)+'.csv')
    originfile=open(origincsv,'r')
    originRead=originfile.read()
    originarray=originRead.split(',')
    print len(originarray)
    predscsv=os.path.join(csvpath,'preds'+str(i)+'.csv')
    predsfile=open(predscsv,'r')
    predsread=predsfile.read()
    predsarray=predsread.split(',')
    print len(predsarray)
    testcsv=os.path.join(csvpath,'test'+str(i)+'.csv')
    testfile=open(testcsv,'r')
    testread=testfile.read()
    testarray=testread.split(',')
    print len(testarray)
    zlist=[]
    for i in range(len(kidarray)):
        print kidarray[i],originarray[i],testarray[i],predsarray[i]
        perlist=[]
        kidlist=kidarray[i].split('_')
        if len(kidlist)==3:
            perlist.append(kidlist[0])
            perlist.append(kidlist[1])
            perlist.append(kidlist[2])
            perlist.append(float(originarray[i]))
            perlist.append(float(testarray[i]))
            perlist.append(float(predsarray[i]))
            zlist.append(perlist)
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
    db.ping(True)
    cursor = db.cursor()
    list_len=len(zlist)
    insert_len = 5000  # 自定义每次插入的记录量
    insert_times = list_len / insert_len + 1
    for i in xrange(0, insert_times):
        start_index = i * insert_times
        end_index = start_index + insert_len
        if start_index >= list_len: break
        if end_index >= list_len: end_index = list_len
    
        tmp_list = zlist[start_index:end_index]
        print (tmp_list[0][3]),(tmp_list[0][4]),(tmp_list[0][5])
        sql='INSERT INTO mosresult(stationid,origintime,foretimes,origin,test,preds) VALUES(%s,%s,%s,%s,%s,%s)'
        cursor.executemany(sql,tmp_list)
        db.commit()
    db.close()
if __name__ == "__main__":
    csvpath='/Users/yetao.lu/Desktop/mos/result_temp'
    for i in range(65):
        ReadCSV(i, csvpath)

    

