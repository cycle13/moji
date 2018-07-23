#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/2
description:湿球温度的计算方法
"""
import os,MySQLdb
def calculate():
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor = db.cursor()
    sql=''
if __name__ == "__main__":
    print ''
