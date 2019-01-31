#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/28
description:
"""
import logging,sys
class Logger:
    def __init__(self,name):
        # 加日志
        self.logger = logging.getLogger('learing.logger')
        # 指定logger输出格式
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
        # 文件日志learning
        file_handler = logging.FileHandler(name)
        file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter  # 也可以直接给formatter赋值
        
        # 为logger添加的日志处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        # 指定日志的最低输出级别，默认为WARN级别
        self.logger.setLevel(logging.INFO)
    def info(self,message):
        self.logger.info(message)