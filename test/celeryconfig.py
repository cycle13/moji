#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/25
description:
"""
from datetime import timedelta
broker_url = 'pyamqp://'
result_backend = 'rpc://'
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Asia/Shanghai'
enable_utc = True
beat_schedule={
    'add-every-30-seconds':{
        'task':'tasks.add',
        'schedule':timedelta(seconds=30),
        'args':(16,16)
    },
}

