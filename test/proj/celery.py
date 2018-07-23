#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/25
description:
"""

from __future__ import absolute_import
from celery import Celery
app=Celery('proj',broker='amqp://',backend='amqp://',include=['proj.tasks'])
app.conf.update(CELERY_TASK_RESULT_EXPIRES=3600,)
if __name__=='__main__':
    app.start()