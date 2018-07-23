#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/4
description:最高气温、最低气温、气温预测
"""
import datetime, os, xgboost, numpy, bz2, \
    multiprocessing, sys, MySQLdb, pygrib,shutil
from sklearn.externals import joblib
def perstationvalue(vstring,latlonArray,indexlat,indexlon):
    vstring.append(latlonArray[indexlat][indexlon])
    vstring.append(latlonArray[indexlat][indexlon + 1])
    vstring.append(latlonArray[indexlat + 1][indexlon + 1])
    vstring.append(latlonArray[indexlat + 1][indexlon])
    vstring.append(latlonArray[indexlat - 1][indexlon - 1])
    vstring.append(latlonArray[indexlat - 1][indexlon])
    vstring.append(latlonArray[indexlat - 1][indexlon + 1])
    vstring.append(latlonArray[indexlat - 1][indexlon + 2])
    vstring.append(latlonArray[indexlat][indexlon + 2])
    vstring.append(latlonArray[indexlat + 1][indexlon + 2])
    vstring.append(latlonArray[indexlat + 2][indexlon + 2])
    vstring.append(latlonArray[indexlat + 2][indexlon + 1])
    vstring.append(latlonArray[indexlat + 2][indexlon])
    vstring.append(latlonArray[indexlat + 2][indexlon - 1])
    vstring.append(latlonArray[indexlat + 1][indexlon - 1])
    vstring.append(latlonArray[indexlat][indexlon - 1])
    return vstring
def calculateStationVariable(tempvariablelist,maxtempvariablelist,mintempvariablelist,inputfile,stationlist):
    if inputfile[-4:]=='grib':
        grbs=pygrib.open(inputfile)
        #把数据矩阵都拿出来
        grb=grbs.select(name='2 metre temperature')
        tempArray=grb[0].values
        grb=grbs.select(name='2 metre dewpoint temperature')
        dewpointArray=grb[0].values
        grb=grbs.select(name='10 metre U wind component')
        u10Array=grb[0].values
        grb=grbs.select(name='10 metre V wind component')
        v10Array=grb[0].values
        grb=grbs.select(name='Total cloud cover')
        tccArray=grb[0].values
        grb=grbs.select(name='Low cloud cover')
        lccArray=grb[0].values
        grb=grbs.select(name='Relative humidity', level=500)
        rh500Array=grb[0].values
        grb=grbs.select(name='Relative humidity', level=850)
        rh850Array=grb[0].values
        #遍历2868个站点
        #csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
        csvfile = '/home/wlan_dev/stations.csv'
        idlist=[]
        fileread = open(csvfile, 'r')
        #fileread.readline()
        iii = 0
        while True:
            iii = iii + 1
            line = fileread.readline()
            perlist = line.split(',')
            if len(perlist) >= 4:
                stationlist.append(perlist)
                idlist.append(perlist[0])
            if not line:
                break
        for p in range(len(stationlist)):
            perlist=stationlist[p]
            latitude=float(perlist[1])
            longitude=float(perlist[2])
            # 经纬度索引
            indexlat = int((90 - latitude) / 0.1)
            indexlon = int((longitude + 180) / 0.1)
            templist=[]
            vstring=[]
            #气温
            perstationvalue(vstring,tempArray,indexlat,indexlon)
            for i in range(len(vstring)):
                templist.append(vstring[i])
            vstring=[]
            perstationvalue(vstring,dewpointArray,indexlat,indexlon)
            for i in range(len(vstring)):
                templist.append(vstring[i])
            vstring=[]
            perstationvalue(vstring,u10Array,indexlat,indexlon)
            for i in range(len(vstring)):
                templist.append(vstring[i])
            vstring=[]
            perstationvalue(vstring,v10Array,indexlat,indexlon)
            for i in range(len(vstring)):
                templist.append(vstring[i])
            vstring=[]
            perstationvalue(vstring,tccArray,indexlat,indexlon)
            for i in range(len(vstring)):
                templist.append(vstring[i])
            vstring=[]
            perstationvalue(vstring,lccArray,indexlat,indexlon)
            for i in range(len(vstring)):
                templist.append(vstring[i])
            vstring=[]
            perstationvalue(vstring, rh500Array, indexlat, indexlon)
            for i in range(len(vstring)):
                templist.append(vstring[i])
            vstring=[]
            perstationvalue(vstring, rh850Array, indexlat, indexlon)
            for i in range(len(vstring)):
                templist.append(vstring[i])
            tempvariablelist.append(templist)
        print len(tempvariablelist),len(stationlist)
def Predict(outfilename,modelname,tempscaler, origintime, n, outpath):
    # 取气温、最高气温、最低气温的训练训练矩阵。
    tempvariablelist = []
    maxtempvariablelist = []
    mintempvariablelist = []
    stationlist=[]
    calculateStationVariable(tempvariablelist,maxtempvariablelist,mintempvariablelist,outfilename,stationlist)
    # 加载训练模型
    params = {
        'booster': 'gbtree',
        'objective': 'reg:linear',  # 线性回归
        'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
        'max_depth': 12,  # 构建树的深度，越大越容易过拟合
        'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
        'subsample': 0.7,  # 随机采样训练样本
        'colsample_bytree': 0.7,  # 生成树时进行的列采样
        'min_child_weight': 3,
        # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
        # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
        # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
        'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
        'eta': 0.02,  # 如同学习率
        'seed': 1000,
        # 'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    bst = xgboost.Booster(params)
    bst.load_model(modelname)
    # 模型预测
    ecvaluelist = numpy.array(tempvariablelist)
    ecvaluelistbak=numpy.array(tempvariablelist)
    #ecvaluelist=ecvaluelist.astype('float64')
    print ecvaluelist
    # savecsv='/Users/yetao.lu/Desktop/mos/anonymous/aaa.txt'
    # filew=open(savecsv,'w')
    # for iii in range(len(ecvaluelist)):
    #     for jjj in range(len(ecvaluelist[iii])):
    #         filew.write(str(ecvaluelist[iii][jjj])+',')
    #     filew.write('\n')
    # filew.close()
    #加载标准化预处理文件，对数据进行与模型一致的标准化
    scaler=joblib.load(tempscaler)
    ecvaluelist=scaler.transform(ecvaluelist)
    print ecvaluelist
    # savecsv='/Users/yetao.lu/Desktop/mos/anonymous/aa.txt'
    # filew=open(savecsv,'w')
    # for iii in range(len(ecvaluelist)):
    #     for jjj in range(len(ecvaluelist[iii])):
    #         filew.write(str(ecvaluelist[iii][jjj])+',')
    #     filew.write('\n')
    # filew.close()
    xgbtrain = xgboost.DMatrix(ecvaluelist)
    result = bst.predict(xgbtrain)
    
    #原始数据气温值 ecvaluelist经过标准化，注意用先前的
    originarray=ecvaluelistbak[:,0]-273.15
    #print result,originarray,len(result),len(originarray),len(stationlist)
    foretime = origintime + datetime.timedelta(hours=n * 3)
    db = MySQLdb.connect('bj28', 'admin', 'moji_China_123', 'moge',3307)
    #db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
    cursor = db.cursor()
    origin = datetime.datetime.strftime(origintime, '%Y-%m-%d %H:%M:%S')
    forecast = datetime.datetime.strftime(foretime, '%Y-%m-%d %H:%M:%S')
    forecast_year = foretime.year
    forecast_month = foretime.month
    forecast_day = foretime.day
    forecast_hour = foretime.hour
    forecast_minute = foretime.minute
    timestr = datetime.datetime.strftime(origintime, '%Y%m%d%H%M%S')
    csv = os.path.join(outpath, timestr + '_' + str(n) + '.csv')
    csvfile = open(csv, 'w')
    for j in range(len(stationlist)):
        stationid = stationlist[j][0]
        temp = result[j]
        origintemp=originarray[j]
        #print temp,originarray,origintemp
        # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
        sql = 'insert into t_r_ec_mos_city_forecast_ele (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ("' + stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(temp) + '") ON DUPLICATE KEY UPDATE temperature="'+str(temp)+'"'
        #print sql
        csvfile.write(stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            temp)+'","'+str(origintemp))
        csvfile.write('\n')
        # print sql
        cursor.execute(sql)
    db.commit()
    db.close()
    csvfile.close()
if __name__ == "__main__":
    starttime=datetime.datetime.now()
    # 遍历所有文件，预测历史数据
    path='/mnt/data/grib'
    outpath='/home/wlan_dev/result'
    #path = '/Users/yetao.lu/Desktop/mos/mosdata/d'
    #outpath = '/Users/yetao.lu/Desktop/mos/temp'
    pool = multiprocessing.Pool(10)
    for root, dirs, files in os.walk(path):
        for file in files:
            if file[-4:] == 'grib' and file[7:9]=='12':
                filename = os.path.join(root, file)
                start = file[3:9]
                end = file[11:17]
                #print start, end
                starttime = datetime.datetime.strptime(start, '%m%d%H')
                origintime = datetime.datetime.strptime('2018' + start,
                                                        '%Y%m%d%H')
                endtime = datetime.datetime.strptime(end, '%m%d%H')
                d = (endtime - starttime).days
                f = (endtime - starttime).seconds / 3600
                allhours=d * 24 + (endtime - starttime).seconds / 3600
                if allhours>144:
                    i=48+(allhours-144)/6
                else:
                    i = allhours / 3
                if i<>0 :
                    modelname='/mnt/data/tempscaler/ectt'+str(i)+'.model'
                    #modelname = '/Users/yetao.lu/Desktop/mos/model/temp/ectt' + str(i) + '.model'
                    tempscalerfile='/mnt/data/tempscaler/scale' + str(i) + '.save'
                    #tempscalerfile='/Users/yetao.lu/Desktop/mos/model/temp/scale'+str(i)+'.save'
                    print modelname,tempscalerfile,filename
                    #Predict(filename,modelname,tempscalerfile,origintime,i,outpath)
                    pool.apply_async(Predict,(filename,modelname,tempscalerfile,origintime,i,outpath,))
        pool.close()
        pool.join()
    endtime = datetime.datetime.now()
    print(endtime - starttime).seconds
