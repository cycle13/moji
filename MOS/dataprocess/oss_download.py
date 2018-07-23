#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/16
description:python多进程下载数据并处理
"""
import multiprocesstest
import time
import oss2

auth=oss2.Auth('LTAIipB2qbeW3V99','Dtyx9XKvFdQvmniSlns18j7lXpmA03')
bucket=oss2.Bucket(auth,'http://oss-cn-beijing.aliyuncs.com','mojimeteo')
remote_stream=bucket.get_object('/moge/data/ecmwf/20180116/D1D01160000011603001.bz2')
print remote_stream.read()
print '123'


