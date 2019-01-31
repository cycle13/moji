#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/26
description:python3.6
"""
import pygrib,os,logging,sys,pymysql
class ecdataprocessforrainsnow(object):
    def __init__(self,logger,db):
        self.logger=logger
        self.db=db
    def featurefromEC(self,ecfilepath):
        for root, dirs, files in os.walk(ecfilepath):
            for file in files:
                filefullname=os.path.join(root,file)
                
            
if __name__ == "__main__":
    logpath='/home/wlan_dev/result'
    # 日志模块
    logging.basicConfig()
    logger = logging.getLogger("apscheduler.executors.default")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile = os.path.join(logpath,'mos_temp.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    ecfilepath='/home/wlan_dev/mosdata'
    db=pymysql.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    
    
    
    
