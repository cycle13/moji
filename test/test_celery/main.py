#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/25
description:
"""
from celery import Celery
if __name__ == "__main__":
    app=Celery(name='task',broker='amqp://')
@app.task
def sayhi():
    return 'hi'
