#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/10
description:
"""
from celery.schedules import crontab
import logging.config
broker_url = "redis://localhost:6379/0"
result_backend = "redis://localhost:6379/0"
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Asia/Shanghai'
enable_utc = True

oldfilepath = '/opt/meteo/cluster/data/ecmwf/orig'
dirpath = '/moji/ecdata'

beat_schedule = {
    # 'add-every-30-seconds':{
    #     'task':'tasks.add003',
    #     'schedule':timedelta(seconds=30),
    #     'args':(16,16)
    # },
    # Executes every Monday morning at 5 A.M and 17 P.M
    'add-every-day': {
        'task': 'taskrun.runmodel',
        #'schedule': crontab(minute=7,hour='5,18'),
        'schedule': crontab(minute='*/5'),
        'args': (oldfilepath,dirpath),
    }
}

worker_hijack_root_logger = False
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        "verbose": {
            'format': '%(asctime)s %(levelname)s [Line: %(lineno)s] -- %(message)s',
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    'handlers': {
        'taskrun': {
            'level': 'INFO',
            'filters': None,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'taskrun.log',
            'formatter': 'verbose'
        },
        'ecxgboostlive_dem_temp': {
            'level': 'INFO',
            'filters': None,
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'dem.log',
            'formatter': 'verbose'
        },
    }, 'loggers': {
        'taskrun': {
            'handlers': ['taskrun'],
            'level': 'INFO',
            'propagate': True,
        },
        'ecxgboostlive_dem_temp': {
            'handlers': ['ecxgboostlive_dem_temp'],
            'level': 'INFO',
            'propagate': True,
        },
    }
}
logging.config.dictConfig(LOG_CONFIG)


