#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/17
description:
"""
import urllib,datetime,json
pdatestring='2018-01-16 01:00:00'
pdatetime=datetime.datetime.strptime(pdatestring,'%Y-%m-%d %H:%M:%S')
for i in range(17):
    odatetime=pdatetime+datetime.timedelta(hours=i)
    datestr=datetime.datetime.strftime(odatetime,'%Y%m%d%H%M%S')
    url='http://api.data.cma.cn:8090/api?userId= mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_HRCLDAS_RT_NC&time='+datestr+'&elements=Bul_Center,FILE_NAME,Data_Area,Datetime,File_URL&dataFormat=json'
    content=urllib.urlopen(url).read()
    #print content
    jfile=json.loads(content)
    datset=jfile['DS']
    print type(datset)
    data=datset[1]
    print type(data)
    name=data['FILE_NAME']
    fileurl=data['FILE_URL']
    print fileurl
    file=urllib.urlopen(fileurl)
    filewrite=open('/Users/yetao.lu/Downloads/shared/rain/'+name,'w')
    filewrite.write(file.read())
    filewrite.flush()
    
    
    
    
    