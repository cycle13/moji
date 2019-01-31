#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/30
description: 根据地址获取经纬度信息，用的高德接口
"""
import json,requests
def getlatlonfromaddress(file,csvfile):
    f=open(file,'r')
    content=f.read().decode('gbk')
    con=content.replace('\\\"','\"')
    #print con
    newdict=json.loads(con)
    #print newdict
    dict={}
    #print newdict['a']
    filec = open(csvfile, 'w')
    print len(newdict['a'])
    for i in range(len(newdict['a'])):
        add=newdict['a'][i]
        address= newdict['a'][i]['WarehouseAddress']
        name= newdict['a'][i]['WarehouseName']
        xy=geocodeG(filec,address,name)
        #print type(xy)



def geocodeG(filec,address,name):
    par = {'address': address, 'key': 'f38183211d9c375916c00e846621fa92'}
    base = 'http://restapi.amap.com/v3/geocode/geo'
    response = requests.get(base, par)
    answer = response.json()
    geocode=answer['geocodes']
    if geocode<>[]:
        GPS=answer['geocodes'][0]['location'].split(",")
        #print GPS[0],GPS[1]
        #print address
        filec.write(name.encode('utf-8')+','+str(GPS[0])+','+str(GPS[1])+','+address.encode('utf-8'))
        filec.write('\n')
    else:
        print name,address
    filec.close
    
def geocodeQQ(address):
    url='https://apis.map.qq.com/ws/geocoder/v1/?address='+address+'&key=IRGBZ-CQV6X-5QB4V-7PCKP-RZIIS-L3BTY'
    response=requests.get(url)
    answer=response.json()
    gps=answer['message']

    print gps.encode('utf-8')
    
if __name__ == "__main__":
    print ''
    file='/Users/yetao.lu/Documents/a.txt'
    csvfile='/Users/yetao.lu/Documents/b.csv'
    getlatlonfromaddress(file,csvfile)