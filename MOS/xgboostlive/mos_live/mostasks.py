#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/6/4
description:
"""
from __future__ import absolute_import
from moscelery import app

@app.task
def add(x,y):
    return x+y

