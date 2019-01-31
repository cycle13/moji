#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/13
description:
"""
#from __future__ import absolute_import
from roottask import app
print id(app)
@app.task
def add003(x,y):
    return x+y

