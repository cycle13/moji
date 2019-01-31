#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/10/8
description:
"""
import time
from apscheduler.schedulers.background import BackgroundScheduler
def abc1(a,b):
    c=a+b
    print c
if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    scheduler.add_job(abc1,'cron',minute='*/1',args=(1,2))
    scheduler.add_job(abc1,'cron',minute='*/1',args=(2,2))
    scheduler.add_job(abc1,'cron',minute='*/1',args=(3,2))
    #scheduler.add_job(weatherfeatureFromEC,'cron',minute='*/2',args=(114.0352, 40.4638, 'TZHCS', txtpath,starttime))
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except Exception as e:
        print e.message
