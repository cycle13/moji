#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/19
description: 墨盘上的旧的气温准确率计算。
"""
import MySQLdb,datetime,logging.handlers,sys,time
from apscheduler.schedulers.background import BackgroundScheduler


class accuracy_old_procedure():
    def __init__(self,logger):
        self.logger=logger
    def run_procedure_mos_accuracy(self):
        starttime=datetime.datetime.now()
        yearint=starttime.year
        monthint=starttime.month
        dayint=starttime.day
        initialtime=datetime.datetime(yearint,monthint,dayint,12,0,0)
        db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
        cursor = db.cursor()
        cursor.callproc('proc_make_ec_forecast_accuracy_mos_dem',(initialtime,1,240,30,3))
        cursor.execute(
            'select @_proc_make_ec_forecast_accuracy_mos_dem_1,@_proc_make_ec_forecast_accuracy_mos_dem_2')
        data=cursor.fetchall()
        cursor.commit()
        db.close()
if __name__ == "__main__":
    #加日志
    logger = logging.getLogger('apscheduler.executors.default')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    file_handler = logging.FileHandler("/home/wlan_dev/log/learning.log")
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值

    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    #构建定时任务；
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    accuoldprocess=accuracy_old_procedure(logger)
    scheduler.add_job(accuoldprocess.run_procedure_mos_accuracy, 'cron', hour='17')
    try:
        scheduler.start()
        while True:
            time.sleep(2)
    except Exception as e:
        logger.info(e.message)