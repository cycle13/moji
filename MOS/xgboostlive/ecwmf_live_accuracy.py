#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/21
description:获取线上EC数据逐小时，逐3小时和逐6小时的准确率
"""
import datetime
class ec_accuracy_live():
    def __init__(self,logger):
        self.logger=logger
    def ecmwf_get_temperature_value(self,path):
        starttime=datetime.datetime.now()
        yearint=starttime.year
        
if __name__ == "__main__":
    print ''
