#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/21
description:
"""
import subprocess,os
from roottask import app
from celery.utils.log import get_task_logger
logger=get_task_logger(__name__)
@app.task
def deleteECMWF(ecpath):
    filefullname=''
    for root,dirs,files in os.walk(ecpath):
        for file in files:
            filefullname=os.path.join(root,file)
            print filefullname
            subprocess.call('sudo rm -rf '+filefullname,shell=True)
        for dir in dirs:
            dirpath=os.path.join(root,dir)
            print dirpath
            subprocess.call('sudo rm -rf ' + dirpath)
    if not os.listdir(ecpath):
        subprocess.call('sudo rm -rf ' + ecpath,shell=True)
if __name__ == "__main__":
    print ''
