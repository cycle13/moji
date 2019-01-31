#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/9
description:
"""
import xgboost,Nio,datetime,logging,sys,os,multiprocessing,numpy,math,time,MySQLdb
import logging.handlers
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn import preprocessing
from sklearn.externals import joblib
def getStationList(csvfile):
    stationlist=[]
    fileread=open(csvfile,'r')
    firstline=fileread.readline()
    while True:
        line=fileread.readline()
        perlist=line.split(',')
        if len(perlist)>=4:
            stationlist.append(perlist)
        if not line or line=='':
            break
    return stationlist
def predictmodel(stationlist,demdict,ecfile_sfc,ecfile_pl,sfc_varinames,pl_varinames,ff,trainlist,origintime,foretime):
    sfc_file=Nio.open_file(ecfile_sfc,'r')
    pl_file=Nio.open_file(ecfile_pl,'r')
    #遍历站点列表：
    for i in range(len(stationlist)):
        #根据EC文件名建立索引来获取实况数据气温的值，文件名中日期为起报时间+预报时效
        ecnameArray=ecfile_sfc.split('_')
        #起报时间
        origindatetime=datetime.datetime.strptime((ecnameArray[1]+ecnameArray[2][:2]),'%Y%m%d%H')
        #EC的预报时间,同样是对应的实况数据的时间
        fdatetime=origindatetime+datetime.timedelta(hours=ff)
        fdatestring=datetime.datetime.strftime(fdatetime,'%Y%m%d%H%M%S')
        #根据站号+时间的索引来获取预报对应的实况数据
        kid=stationlist[i][0]+'_'+fdatestring
        vstring=[]
        # 根据预报时效来计算数组的索引
        if ff <= 120:
            j = ff / 3
        else:
            j = (ff - 120) / 6 + 40
        levelArray=pl_file.variables['lv_ISBL1']
        latitude=float(stationlist[i][1])
        longitude=float(stationlist[i][2])
        alti=float(stationlist[i][3])
        #取左上角点的索引，而不是最邻近点
        indexlat=int((60-latitude)/0.1)
        indexlon=int((longitude-60)/0.1)
        for m in range(len(sfc_varinames)):
            variArray=sfc_file.variables[sfc_varinames[m]]
            latlonArray=variArray[j]
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
        for n in range(len(pl_varinames)):
            pl_variArray=pl_file.variables[pl_varinames[n]]
            phaArray=pl_variArray[j]
            for k in range(len(phaArray)):
                llArray=phaArray[k]
                pha=levelArray[k]
                if pha==500 or pha==850:
                    if llArray[indexlat][indexlon]=='NaN' or llArray[indexlat][indexlon]==None:
                        logger.info(pl_file)
                    vstring.append(llArray[indexlat][indexlon])
                    vstring.append(llArray[indexlat][indexlon + 1])
                    vstring.append(llArray[indexlat + 1][indexlon + 1])
                    vstring.append(llArray[indexlat + 1][indexlon])
                    vstring.append(llArray[indexlat - 1][indexlon - 1])
                    vstring.append(llArray[indexlat - 1][indexlon])
                    vstring.append(llArray[indexlat - 1][indexlon + 1])
                    vstring.append(llArray[indexlat - 1][indexlon + 2])
                    vstring.append(llArray[indexlat][indexlon + 2])
                    vstring.append(llArray[indexlat + 1][indexlon + 2])
                    vstring.append(llArray[indexlat + 2][indexlon + 2])
                    vstring.append(llArray[indexlat + 2][indexlon + 1])
                    vstring.append(llArray[indexlat + 2][indexlon])
                    vstring.append(llArray[indexlat + 2][indexlon - 1])
                    vstring.append(llArray[indexlat + 1][indexlon - 1])
                    vstring.append(llArray[indexlat][indexlon - 1])
        #站点经纬度
        vstring.append(latitude)
        vstring.append(longitude)
        vstring.append(alti)
        #站点高程：取计算好的站点周边16个点的高程值
        demlist=demdict[stationlist[i][0]]
        for u in range(1,len(demlist),1):
            vstring.append(float(demlist[u]))
        #vstring为一个站点的因子列表，添加到训练样本中
        if vstring<>[]:
            trainlist.append(vstring)
    sfc_file.close()
    pl_file.close()
    #模型训练
    trainarray=numpy.array(trainlist)
    print trainarray.shape
    print trainarray
    params001 = {
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
        # 'nthread':3,# cpu 线程数,不设置取最大值
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    xgbst=xgboost.Booster(params001)
    xgbst.load_model(modelfile)
    scaler=joblib.load(scalefile)
    trainarray_t=scaler.transform(trainarray)
    print trainarray_t
    xgbtrain=xgboost.DMatrix(trainarray_t)
    result=xgbst.predict(xgbtrain)
    print result
    #结果入库
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
    #db = MySQLdb.connect('192.168.10.84', 'admin', 'moji_China_123','moge')
    cursor = db.cursor()
    origin = datetime.datetime.strftime(origintime, '%Y-%m-%d %H:%M:%S')
    forecast = datetime.datetime.strftime(foretime, '%Y-%m-%d %H:%M:%S')
    forecast_year = foretime.year
    forecast_month = foretime.month
    forecast_day = foretime.day
    forecast_hour = foretime.hour
    forecast_minute = foretime.minute
    timestr = datetime.datetime.strftime(origintime, '%Y%m%d%H%M%S')
    # csv = os.path.join(outpath, origin+'_'+forecast + '.csv')
    # csvfile = open(csv, 'w')
    sql = 'replace into t_r_ec_city_forecast_ele_mos_dem_winter (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
    L = []
    for j in range(len(stationlist)):
        perstationlist = []
        stationid = stationlist[j][0]
        temp = result[j]
        # 每个站点存储
        perstationlist.append(stationid)
        perstationlist.append(origin)
        perstationlist.append(forecast)
        perstationlist.append(forecast_year)
        perstationlist.append(forecast_month)
        perstationlist.append(forecast_day)
        perstationlist.append(forecast_hour)
        perstationlist.append(temp)
        L.append(perstationlist)
        #logger.info(perstationlist)
        #             # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
        #             # sql = 'insert into t_r_ec_city_forecast_ele_mos (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,temp_max_6h,temp_min_6h,rainstate,precipitation)VALUES ("' + stationid + '","' + origin + '","' + str(
        #             #     forecast) + '","' + str(forecast_year) + '","' + str(
        #             #     forecast_month) + '","' + str(forecast_day) + '","' + str(
        #             #     forecast_hour) + '","' + str(temp) + '","' + str(maxtemp)+ '","' + str(mintemp)+'","' + str(rainstate)+'","' + str(prevalue)+ '")
        #             # csvfile.write(stationid + '","' + origin + '","' + str(
        #             #     forecast) + '","' + str(forecast_year) + '","' + str(
        #             #     forecast_month) + '","' + str(forecast_day) + '","' + str(
        #             #     forecast_hour) + '","' + str(forecast_minute) + '","' + str(
        #             #     temp)+ '","' + str(maxtemp)+ '","' + str(mintemp)+'","' + str(rainstate)+'","' + str(prevalue))
        #             # csvfile.write('\n')
        #             # print sql
        #             # cursor.execute(sql)
    cursor.executemany(sql, L)
    db.commit()
    db.close()

if __name__ == "__main__":
    #加日志
    logfile='/home/wlan_dev/log/learning006.log'
    logger = logging.getLogger(logfile)
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    #file_handler=logging.handlers.RotatingFileHandler(logfile, maxBytes=1024 * 1024,backupCount=5)
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值
    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.DEBUG)
    demdict={}
    demcsv='/mnt/data/mosfile/dem0.1.csv'
    csvread=open(demcsv,'r')
    while True:
        line=csvread.readline()
        linearray=line.split(',')
        if len(linearray)>2:
            demdict[linearray[0]]=linearray
        if not line:
            break
    csvfile='/mnt/data/mosfile/stations.csv'
    stationlist=getStationList(csvfile)
    ecpath='/mnt/data/MOS/2018/10'
    trainlist=[]
    sfc_varinames = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC','10V_GDS0_SFC','TCC_GDS0_SFC', 'LCC_GDS0_SFC','Z_GDS0_SFC']
    pl_varinames = ['R_GDS0_ISBL']
    for ecroot,dirs,files in os.walk(ecpath):
        for file in files:
            if file[:3] == 'sfc' and file[-5:] == '.grib':
                ecfile_sfc=os.path.join(ecroot,file)
                ecfile_pl=ecfile_sfc.replace('sfc','pl')
                #这里需要根据文件来确定模型名称和标准化名称，因为是测试暂时略掉
                modelfile = '/mnt/data/wintermodel/temp_model006.model'
                scalefile = '/mnt/data/wintermodel/temp_scale006.save'
                origintime=datetime.datetime.strptime('2018-10-01 12:00:00','%Y-%m-%d %H:%M:%S')
                foretime=origintime+datetime.timedelta(hours=6)
                if not os.path.exists(ecfile_pl):
                    continue
                else:
                    predictmodel(stationlist,demdict,ecfile_sfc,ecfile_pl,sfc_varinames,pl_varinames,6,trainlist,origintime,foretime)
