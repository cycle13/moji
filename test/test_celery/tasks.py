#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/11
description:
"""
from celery import Celery

app = Celery('tasks', backend='redis://localhost:6379/0',
             broker='redis://localhost:6379/0')  # 配置好celery的backend和broker


@app.task  # 普通函数装饰为 celery task


def add(x, y):
    return x + y