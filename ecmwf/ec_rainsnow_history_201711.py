#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/27
description: python3.6
"""
import os,datetime,subprocess
class ecrainsnow(object):
    def __init__(self):
        print('')
    def main(self,ecfile):
        for root,dirs,files in os.walk(ecfile):
            for file in files:
                if len(file)<1 and file[-4:]=='grib':
                    filename=os.path.join(root,file)
                    print(filename)
                    subprocess.call(['./ec_snow',filename],cwd='/opt/meteo/cluster/program/moge/ec_rainsnow')
                    #os.system('/opt/meteo/cluster/program/moge/ec_rainsnow/ec_snow ' +filename)
if __name__ == "__main__":
    #starttime=datetime.datetime.strptime('2018-12-26 12:00:00','%Y-%m-%d %H:%M:%S')
    ecrootpath='/home/cluser/mosdata/2018/2018-12-06'
    ecrainsnow=ecrainsnow()
    ecrainsnow.main(ecrootpath)