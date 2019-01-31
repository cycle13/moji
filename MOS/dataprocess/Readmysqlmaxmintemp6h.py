#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/29
description:
"""
import MySQLdb,datetime
if __name__ == "__main__":
    firstdate = '20160601 00:00:00'
    pdatetime = datetime.datetime.strptime(firstdate, '%Y%m%d %H:%M:%S')
    db = MySQLdb.connect('192.168.10.84', 'admin', 'moji_China_123', 'moge')
    cursor = db.cursor()
    filepath='/Users/yetao.lu/Desktop/mos/data/temmaxmin6h/maxmin2016.csv'
    filewrite=open(filepath,'w')
    for i in range(92):
        tmpdate=pdatetime+datetime.timedelta(days=i)
        for j in range(4):
            ppdate=tmpdate+datetime.timedelta(hours=6*j)
            odatetime=tmpdate+datetime.timedelta(hours=6*(j+1))
            pdatestring=datetime.datetime.strftime(ppdate,'%Y-%m-%d %H:%M:%S')
            odatestring=datetime.datetime.strftime(odatetime,'%Y-%m-%d %H:%M:%S')
            #2015
            sql = 'SELECT Station_Id_d , ANY_VALUE(Lat) , ANY_VALUE(Lon) , ANY_VALUE(Alti) , MAX(TEM),MIN(TEM) FROM cimiss_mos WHERE vdate > "' + pdatestring + '" AND vdate <= "' + odatestring + '" GROUP BY Station_Id_d'
            #2016\2017
            #sql='SELECT Station_Id_d , ANY_VALUE(Lat) , ANY_VALUE(Lon) , ANY_VALUE(Alti) , MAX(TEM_Max),MIN(TEM_Min) FROM cimiss_mos WHERE vdate >= "'+pdatestring+'" AND vdate < "'+odatestring+'" GROUP BY Station_Id_d'
            cursor.execute(sql)
            rows=cursor.fetchall()
            for row in rows:
                print str(row[0]) + ','+pdatestring+',' + str(row[1]) + ',' + str(row[2]) + ',' + str(row[3]) + ',' + str(row[4]) + ',' + str(row[5])
                filewrite.write(str(row[0]) + ','+pdatestring+',' + str(row[1]) + ',' + str(row[2]) + ',' + str(row[3]) + ',' + str(row[4]) + ',' + str(row[5]))
                filewrite.write('\n')
    filewrite.close()
    db.close()
