#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/25
description:
"""
from tasks import add
result=add.delay(4,4)
print result

