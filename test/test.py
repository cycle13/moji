#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb

"""
author:     yetao.lu
date:       2017/12/29
description:
"""
import Nio
inputfile2='/Users/yetao.lu/Documents/mosdata/pl_20150601_12.grib'
readfile=Nio.open_file(inputfile2,'r')
print readfile
