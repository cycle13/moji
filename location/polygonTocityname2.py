#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/30
description:
"""
import psycopg2
if __name__ == "__main__":
    csvfile='/Users/yetao.lu/Downloads/sunec/world_admin_1.csv'
    csvwr=open(csvfile,'w')
    conn = psycopg2.connect(database='adc2018', user='postgres', password='mojichina',
                            host='192.168.1.21', port='5432')
    cur = conn.cursor()
    sql001='SELECT * FROM world_admin_1'
    cur.execute(sql001)
    rows = cur.fetchall()
    for row in rows:
        if row[2]<>None:
            #判断eng_name是否在城市表中
            sql002="select * from world_cities where eng_name= '"+row[2].replace("'","\''")+"'"
            cur.execute(sql002)
            rows002=cur.fetchall()
            if len(rows002)==0:
                print row[0],sql002
                csvwr.write(str(row[0]))
                csvwr.write('\n')
    csvwr.close()
    conn.commit()
    cur.close()
    conn.close()
        

