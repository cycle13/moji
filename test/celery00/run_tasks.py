#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/11
description:
"""
from celery import Celery
from celery import chain
from tasks import *
add.delay(3,2)
multi.delay(2,6)
hello.delay('hello')
abc.delay(2,2)
task=chain(add.si(3,2)|multi.si(2,2)|abc.si(2,2)|hello.si('abcc'))
result=task().get()