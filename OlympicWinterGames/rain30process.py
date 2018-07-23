#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/4/24
description:
"""
import MySQLdb
def rainprocess():
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor=db.cursor()
    startyear=1980
    for i in range(37):
        targetyear=startyear+i
        sql='SELECT SUM(rain) from yanqing_rain30 where `year`="'+targetyear+'"'
        
if __name__ == "__main__":
    print ''
