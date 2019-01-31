#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/13
description:
"""
#下面的意思是局部的包不能覆盖全局的包
#from __future__ import absolute_import
from celery import Celery
from celery.utils.log import get_task_logger

logger=get_task_logger(__name__)
#
app = Celery('mosapp',include=['tasks','bigdataDownloadEC','bz2fileTogrib','ecxgboostlive_dem_temp','temp_accuracy_dem','taskrun','ecxgboostlive_temp','temp_accuracy','deleteECdata','temp_accuracy_dem2'])
app.config_from_object("celeryconfig")

if __name__ == "__main__":
    app.start()