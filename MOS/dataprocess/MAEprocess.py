#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/12
description:
"""
import MySQLdb
db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
cursor = db.cursor()
sql=''
cursor.execute(sql)
rows=cursor.fetchall()
for row in rows:
    print row
    



