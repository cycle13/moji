#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/30
description:
"""
import psycopg2
if __name__ == "__main__":
    csvfile='/Users/yetao.lu/Downloads/sunec/1.csv'
    csvwr=open(csvfile,'w')
    conn = psycopg2.connect(database='adc2018', user='postgres', password='mojichina',
                            host='192.168.1.21', port='5432')
    cur = conn.cursor()
    sql001='SELECT * FROM world_admin_1'
    cur.execute(sql001)
    rows = cur.fetchall()
    # print(rows)
    for row in rows:
        #判断eng_name是否在城市表中,eng_name是索引为2的字段，geom是索引10的字段
        sql002="select gid,longitude,latitude,ST_AsText(geom) from world_cities where eng_name= '"+row[2].replace("'","\''")+"'"
        cur.execute(sql002)
        rows002=cur.fetchall()
        #检索polygon表中的名称是否在city表中，如果在的话，再判断经纬度是否落在区域内，来排除重名的情况。
        boolflag = False
        if len(rows002)>0:
            #检索的城市点不一定是一个，因此需要遍历
            for perrow in rows002:
                sql="SELECT ST_Contains((SELECT geom FROM world_admin_1 WHERE gid = "+str(row[0])+"),st_geometryfromtext('"+perrow[3]+"',4326))"
                print sql
                # #判断点是否在面内
                cur.execute(sql)
                rerows=cur.fetchall()
                for rerow in rerows:
                    print rerow[0]
                    boolflag=boolflag or rerow[0]
        if boolflag=='False':
            print row[0]
            csvwr.write(str(row[0]))
            csvwr.write('\n')
    csvwr.close()
    conn.commit()
    cur.close()
    conn.close()
        

