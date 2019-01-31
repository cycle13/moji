#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/9/21
description:
"""
from celery import Celery
from celery.utils.log import get_task_logger
logger=get_task_logger(__name__)
app=Celery('powerapp',include=['powerweatherfeature'])
app.config_from_object("celeryconfig")
if __name__ == "__main__":
    app.start()
