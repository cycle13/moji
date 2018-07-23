#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/6/4
description:
"""
from celery import Celery
from time import sleep

app = Celery('mos_live')
app.config_from_object('celeryconfig')

if __name__=='__main__':
    app.start()
