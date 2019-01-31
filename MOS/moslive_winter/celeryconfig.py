#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/10
description:
"""
from datetime import timedelta
from celery.schedules import crontab
broker_url = "redis://localhost:6379/0"
result_backend = "redis://localhost:6379/0"
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Asia/Shanghai'
enable_utc = True
beat_schedule = {
    # 'add-every-30-seconds':{
    #     'task':'tasks.add003',
    #     'schedule':timedelta(seconds=30),
    #     'args':(16,16)
    # },
    # Executes every Monday morning at 5 A.M and 17 P.M
    'add-every-day': {
        'task': 'run.mosrun',
        'schedule': crontab(minute=53,hour='5,17'),
        'args': (),
    }
}

