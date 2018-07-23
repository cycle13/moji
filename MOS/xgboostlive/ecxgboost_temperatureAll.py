#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/4
description:最高气温、最低气温、气温预测
"""
import Nio, datetime, os, xgboost, numpy, bz2, \
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
        grb=grbs.select(name='Maximum temperature at 2 metres in the last 6 hours')
        maxtempArray=grb[0].values
        grb=grbs.select(name='Minimum temperature at 2 metres in the last 6 hours')
        mintempArray=grb[0].values
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
        #遍历2867个站点
        csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
        #csvfile = '/home/wlan_dev/stations.csv'
        idlist=[]
        fileread = open(csvfile, 'r')
        fileread.readline()
        iii = 0
        while True:
            iii = iii + 1
            line = fileread.readline()
            perlist = line.split(',')
            if len(perlist) >= 4:
                stationlist.append(perlist)
                latitude=float(perlist[1])
                longitude=float(perlist[2])
                idlist.append(perlist[0])
                # 经纬度索引
                indexlat = int((90 - latitude) / 0.1)
                indexlon = int((longitude + 180) / 0.1)
                maxlist=[]
                minlist=[]
                templist=[]
                vstring=[]
                #气温
                perstationvalue(vstring, maxtempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring, mintempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,tempArray,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,dewpointArray,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,u10Array,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,v10Array,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,tccArray,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,lccArray,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring, rh500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring, rh850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                #把站点经度纬度和高度加上
                maxlist.append(perlist[1])
                minlist.append(perlist[1])
                maxlist.append(perlist[2])
                minlist.append(perlist[2])
                maxlist.append(perlist[3])
                minlist.append(perlist[3])
                #print perlist[1],perlist[2],perlist[3]
            #添加到总的矩阵中
            tempvariablelist.append(templist)
            maxtempvariablelist.append(maxlist)
            mintempvariablelist.append(minlist)
            if not line:
                break
def Predict(outfilename,modelname,maxmodel,minmodel,tempscaler,maxtempscaler,mintempscaler, origintime, n, outpath):
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
    #ecvaluelist=ecvaluelist.astype('float64')
    print ecvaluelist
    #加载标准化预处理文件，对数据进行与模型一致的标准化
    scaler=joblib.load(tempscalerfile)
    scaler.transform(ecvaluelist)
    print ecvaluelist
    xgbtrain = xgboost.DMatrix(ecvaluelist)
    result = bst.predict(xgbtrain)
    print result
    #最高气温
    # 加载训练模型
    params01 = {
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
        'eta': 0.1,  # 如同学习率
        'seed': 1000,
        # 'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    #最低气温
    params02 = {
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
        'eta': 0.1,  # 如同学习率
        'seed': 1000,
        # 'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    bst01 = xgboost.Booster(params01)
    bst01.load_model(maxmodel)
    bst02=xgboost.Booster(params02)
    bst02.load_model(minmodel)#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/4
description:最高气温、最低气温、气温预测
"""
import Nio, datetime, os, xgboost, numpy, bz2, \
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
        grb=grbs.select(name='Maximum temperature at 2 metres in the last 6 hours')
        maxtempArray=grb[0].values
        grb=grbs.select(name='Minimum temperature at 2 metres in the last 6 hours')
        mintempArray=grb[0].values
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
        #遍历2867个站点
        csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
        #csvfile = '/home/wlan_dev/stations.csv'
        idlist=[]
        fileread = open(csvfile, 'r')
        fileread.readline()
        iii = 0
        while True:
            iii = iii + 1
            line = fileread.readline()
            perlist = line.split(',')
            if len(perlist) >= 4:
                stationlist.append(perlist)
                latitude=float(perlist[1])
                longitude=float(perlist[2])
                idlist.append(perlist[0])
                # 经纬度索引
                indexlat = int((90 - latitude) / 0.1)
                indexlon = int((longitude + 180) / 0.1)
                maxlist=[]
                minlist=[]
                templist=[]
                vstring=[]
                #气温
                perstationvalue(vstring, maxtempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring, mintempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,tempArray,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,dewpointArray,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,u10Array,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,v10Array,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,tccArray,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring,lccArray,indexlat,indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring, rh500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                perstationvalue(vstring, rh850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring=[]
                #把站点经度纬度和高度加上
                maxlist.append(perlist[1])
                minlist.append(perlist[1])
                maxlist.append(perlist[2])
                minlist.append(perlist[2])
                maxlist.append(perlist[3])
                minlist.append(perlist[3])
                #print perlist[1],perlist[2],perlist[3]
            #添加到总的矩阵中
            tempvariablelist.append(templist)
            maxtempvariablelist.append(maxlist)
            mintempvariablelist.append(minlist)
            if not line:
                break
def Predict(outfilename,modelname,maxmodel,minmodel,tempscaler,maxtempscaler,mintempscaler, origintime, n, outpath):
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
    #ecvaluelist=ecvaluelist.astype('float64')
    print ecvaluelist
    #加载标准化预处理文件，对数据进行与模型一致的标准化
    scaler=joblib.load(tempscalerfile)
    scaler.transform(ecvaluelist)
    print ecvaluelist
    xgbtrain = xgboost.DMatrix(ecvaluelist)
    result = bst.predict(xgbtrain)
    print result
    #最高气温
    # 加载训练模型
    params01 = {
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
        'eta': 0.1,  # 如同学习率
        'seed': 1000,
        # 'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    #最低气温
    params02 = {
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
        'eta': 0.1,  # 如同学习率
        'seed': 1000,
        # 'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    bst01 = xgboost.Booster(params01)
    bst01.load_model(maxmodel)
    bst02=xgboost.Booster(params02)
    bst02.load_model(minmodel)
    # 模型预测
    maxtempvariablelist = numpy.array(maxtempvariablelist)
    maxscaler=joblib.load(maxscalerfile)
    maxscaler.transform(maxtempvariablelist)
    xgbtrain01 = xgboost.DMatrix(maxtempvariablelist)
    result01 = bst01.predict(xgbtrain01)
    mintempvariablelist= numpy.array(mintempvariablelist)
    minscaler=joblib.load(minscalerfile)
    minscaler.transform(mintempvariablelist)
    xgbtrain02 = xgboost.DMatrix(mintempvariablelist)
    result02=bst02.predict(xgbtrain02)
    print result01,result02
    foretime = origintime + datetime.timedelta(hours=n * 3)
    #db = MySQLdb.connect('bj28', 'admin', 'moji_China_123', 'moge')
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
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
        maxtemp=result01[j]
        mintemp=result02[j]
        # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
        sql = 'insert into t_r_ec_mos_city_forecast_ele (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,temp_max_6h,temp_min_6h)VALUES ("' + stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(temp) + '","' + str(maxtemp)+ '","' + str(mintemp)+ '")'
        #print sql
        csvfile.write(stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            temp)+ '","' + str(maxtemp)+ '","' + str(mintemp))
        csvfile.write('\n')
        # print sql
        cursor.execute(sql)
    db.commit()
    db.close()
    csvfile.close()
if __name__ == "__main__":
    starttime=datetime.datetime.now()
    # 遍历所有文件，预测历史数据
    # path='/mnt/data/test'
    # outpath='home/wlan_dev/result'
    path = '/Users/yetao.lu/Desktop/mos/mosdata/d'
    outpath = '/Users/yetao.lu/Desktop/mos/temp'
    # pool = multiprocessing.Pool(20)
    for root, dirs, files in os.walk(path):
        for file in files:
            if file[-4:] == 'grib':
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
                i = (d * 24 + (endtime - starttime).seconds / 3600) / 3
                # modelname='/mnt/data/tempmodel/ectemp'+str(i)+'.model'
                # maxmodel='/mnt/data/maxtemp/max_temp' + str(i) + '.model'
                # minmodel='/mnt/data/mintemp/min_temp' + str(i) + '.model'
                modelname = '/Users/yetao.lu/Desktop/mos/model/temp/ectt' + str(i) + '.model'
                maxmodel='/Users/yetao.lu/Desktop/mos/anonymous/max_temp1.model'
                minmodel='/Users/yetao.lu/Desktop/mos/anonymous/min_temp1.model'
                # tempscalerfile='/mnt/data/tempscaler/scale' + str(i) + '.save'
                # maxscalerfile='/mnt/data/maxtempscaler/max_scaler1.save'
                # minscalerfile='/mnt/data/mintempscaler/max_scaler1.save'
                tempscalerfile='/Users/yetao.lu/Desktop/mos/model/temp/scale'+str(i)+'.save'
                maxscalerfile='/Users/yetao.lu/Desktop/mos/anonymous/max_scaler1.save'
                minscalerfile='/Users/yetao.lu/Desktop/mos/anonymous/max_scaler1.save'
                print tempscalerfile,modelname
                Predict(filename,modelname,maxmodel,minmodel,tempscalerfile,maxscalerfile,minscalerfile,origintime,i,outpath)
        #       pool.apply_async(Predict,(filename,modelname,origintime,i,outpath,))
        # pool.close()
        # pool.join()
    endtime = datetime.datetime.now()
    print(endtime - starttime).seconds

    # 模型预测
    maxtempvariablelist = numpy.array(maxtempvariablelist)
    maxscaler=joblib.load(maxscalerfile)
    maxscaler.transform(maxtempvariablelist)
    xgbtrain01 = xgboost.DMatrix(maxtempvariablelist)
    result01 = bst01.predict(xgbtrain01)
    mintempvariablelist= numpy.array(mintempvariablelist)
    minscaler=joblib.load(minscalerfile)
    minscaler.transform(mintempvariablelist)
    xgbtrain02 = xgboost.DMatrix(mintempvariablelist)
    result02=bst02.predict(xgbtrain02)
    print result01,result02
    foretime = origintime + datetime.timedelta(hours=n * 3)
    #db = MySQLdb.connect('bj28', 'admin', 'moji_China_123', 'moge')
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
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
        maxtemp=result01[j]
        mintemp=result02[j]
        # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
        sql = 'insert into t_r_ec_mos_city_forecast_ele (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,temp_max_6h,temp_min_6h)VALUES ("' + stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(temp) + '","' + str(maxtemp)+ '","' + str(mintemp)+ '")'
        #print sql
        csvfile.write(stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            temp)+ '","' + str(maxtemp)+ '","' + str(mintemp))
        csvfile.write('\n')
        # print sql
        cursor.execute(sql)
    db.commit()
    db.close()
    csvfile.close()
if __name__ == "__main__":
    starttime=datetime.datetime.now()
    # 遍历所有文件，预测历史数据
    # path='/mnt/data/test'
    # outpath='home/wlan_dev/result'
    path = '/Users/yetao.lu/Desktop/mos/mosdata/d'
    outpath = '/Users/yetao.lu/Desktop/mos/temp'
    # pool = multiprocessing.Pool(20)
    for root, dirs, files in os.walk(path):
        for file in files:
            if file[-4:] == 'grib':
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
                i = (d * 24 + (endtime - starttime).seconds / 3600) / 3
                # modelname='/mnt/data/tempmodel/ectemp'+str(i)+'.model'
                # maxmodel='/mnt/data/maxtemp/max_temp' + str(i) + '.model'
                # minmodel='/mnt/data/mintemp/min_temp' + str(i) + '.model'
                modelname = '/Users/yetao.lu/Desktop/mos/model/temp/ectt' + str(i) + '.model'
                maxmodel='/Users/yetao.lu/Desktop/mos/anonymous/max_temp1.model'
                minmodel='/Users/yetao.lu/Desktop/mos/anonymous/min_temp1.model'
                # tempscalerfile='/mnt/data/tempscaler/scale' + str(i) + '.save'
                # maxscalerfile='/mnt/data/maxtempscaler/max_scaler1.save'
                # minscalerfile='/mnt/data/mintempscaler/max_scaler1.save'
                tempscalerfile='/Users/yetao.lu/Desktop/mos/model/temp/scale'+str(i)+'.save'
                maxscalerfile='/Users/yetao.lu/Desktop/mos/anonymous/max_scaler1.save'
                minscalerfile='/Users/yetao.lu/Desktop/mos/anonymous/max_scaler1.save'
                print tempscalerfile,modelname
                Predict(filename,modelname,maxmodel,minmodel,tempscalerfile,maxscalerfile,minscalerfile,origintime,i,outpath)
        #       pool.apply_async(Predict,(filename,modelname,origintime,i,outpath,))
        # pool.close()
        # pool.join()
    endtime = datetime.datetime.now()
    print(endtime - starttime).seconds
