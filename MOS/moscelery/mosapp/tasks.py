#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/13
description:
"""
#from __future__ import absolute_import
from mosapp import app
def add004(a):
    return a+1
@app.task
def add003(x,y):
    x=add004(x)
    return x+y

