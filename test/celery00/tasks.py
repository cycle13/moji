#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/11
description:
"""
import time
from celery import Celery, platforms

platforms.C_FORCE_ROOT = True
celery = Celery('tasks', broker='redis://localhost:6379/0',backend='redis://localhost:6379/0')
@celery.task
def abc(x,y):
    return x*2+y
@celery.task
def add(x, y):
    return x + y
@celery.task
def multi(x,y):
    return x*y
@celery.task
def hello(a):
    b='hello'
    c=a+b
    return c

