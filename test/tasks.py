#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/24
description:
"""
from celery import Celery
from time import sleep
#NAME必须和文件名相同，刚学做这个规定
app = Celery('tasks')
app.config_from_object('celeryconfig')

@app.task
def add(x, y):
    csv='/Users/'
    return x + y

@app.task(ignore_result=True)    #这个hello函数不需要返回有用信息，设置ignore_rsult可以忽略任务结果
def hello():
    print 'Hello,Celery!'

@app.task
def add001(x,y):
    sleep(5)    #模拟耗时操作
    return x+y
if __name__=='__main__':
    app.start()
