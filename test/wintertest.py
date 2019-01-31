#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/10/23
description:
"""
import Nio,os,logging,sys

if __name__ == "__main__":
    # 加日志
    logfile = '/home/wlan_dev/log/learntest.log'
    logger = logging.getLogger(logfile)
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值
    
    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    path='/mnt/data/MOS'
    for root,dirs,files in os.walk(path):
        for file in files:
            if file[:3] == 'sfc' and file[-5:] == '.grib' and (file[8:10]<>'06' and file[8:10]<>'07' and file[8:10]<>'08'):
                filepath=os.path.join(root,file)
                sfc_file=Nio.open_file(filepath,'r')
                logger.info(sfc_file)
                pl=filepath.replace('sfc','pl')
                pl_file=Nio.open_file(pl,'r')
                logger.info(pl_file)
            
