#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/17
description:
"""
import MySQLdb
def maxwsperyear():
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor = db.cursor()
if __name__ == "__main__":
    print ''
