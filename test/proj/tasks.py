#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/25
description:
"""

from __future__ import absolute_import

from celery import app


@app.task()
def add(x, y):
    return x + y


@app.task()
def mul(x, y):
    return x * y


@app.task
def xsum(numbers):
    return sum(numbers)
