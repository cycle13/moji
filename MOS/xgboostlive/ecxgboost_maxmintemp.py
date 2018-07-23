#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/3
description: 最高气温线上数据预测
"""
import Nio, datetime, os, xgboost, numpy, bz2, \
    multiprocessing, sys, MySQLdb, pygrib, time, shutil


def getvaluebyindex(vstring, latlonArray, indexlat, indexlon):
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


# 因为EC线上的数据和历史数据格式不一致导致我代码的重写，好恶心呀
def calculatePerStationVariableForMaxtemp(inputfile, longitude, latitude):
    if inputfile[-4:] == 'grib':
        grbs = pygrib.open(inputfile)
        # 经纬度索引
        indexlat = int((90 - latitude) / 0.1)
        indexlon = int((longitude + 180) / 0.1)
        vstring = []
        names = ['Maximum temperature at 2m in the last 6 hours','Minimum temperature at 2m in the last 6 hours','2 metre temperature', '2 metre dewpoint temperature',
            '10 metre U wind component', '10 metre V wind component',
            'Total cloud cover', 'Low cloud cover', 'Relative humidity',]
        # 各要素---气温
        for name in names:
            if name == 'Relative humidity':
                for i in range(2):
                    if i == 0:
                        grb = grbs.select(name='Relative humidity', level=500)
                    elif i == 1:
                        grb = grbs.select(name='Relative humidity', level=850)
            else:
                grb = grbs.select(name=name)
            latlonArray = grb[0].values
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
# 取单点的预测数据集，然后在循环取全部站点的
def calculateRaster(inputfile, longitude, latitude):
    file = Nio.open_file(inputfile, 'r')
    names = file.variables.keys()
    sfc_varinames = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC',
        '10V_GDS0_SFC', 'TCC_GDS0_SFC', 'LCC_GDS0_SFC']
    pl_varinames = ['R_GDS0_ISBL']
    # print names
    # 则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
    latArray = file.variables['g0_lat_0']
    lonArray = file.variables['g0_lon_1']
    # print latArray,lonArray
    indexlat = int((90 - latitude) / 0.1)
    indexlon = int((longitude - 0) / 0.1)
    vstring = []
    for i in range(len(sfc_varinames)):
        basearray = file.variables[sfc_varinames[i]]
        # print len(basearray),len(basearray[0])
        latlonArray = basearray
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
    for j in range(len(pl_varinames)):
        alllarray = file.variables[pl_varinames[j]]
        levelarray = file.variables['lv_ISBL2']
        for jj in range(len(levelarray)):
            # print levelarray[jj]
            if levelarray[jj] == 500 or levelarray[jj] == 850:
                basearray = alllarray[jj]
                # print 'basearray',len(basearray)
                llArray = basearray
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
    return vstring
def Predict(outfilename, maxmodel,minmodel, origintime, n, outpath):
    bz2filepath = os.path.split(outfilename)
    timestr = datetime.datetime.strftime(origintime, '%Y%m%d%H%M%S')
    logfile = os.path.join(bz2filepath[0], timestr + '_' + str(n) + ".out")
    # sys.stdout = open(logfile, "w")
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
        'eta': 0.1,  # 如同学习率
        'seed': 1000,
        # 'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
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
    bst = xgboost.Booster(params)
    bst.load_model(maxmodel)
    bst01=xgboost.Booster(params01)
    bst01.load_model(minmodel)
    # 站点列表数据
    stationlist = []
    # 所有站点的EC数据
    ecvaluelist = []
    # idlist存储站号
    idlist = []
    # csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
    csvfile = '/home/wlan_dev/stations.csv'
    fileread = open(csvfile, 'r')
    fileread.readline()
    iii = 0
    while True:
        iii = iii + 1
        line = fileread.readline()
        perlist = line.split(',')
        if len(perlist) >= 4:
            stationlist.append(perlist)
            # perstationlist = calculateRaster(newfile, float(perlist[2]),float(perlist[1]))
            perstationlist = calculatePerStationVariableForMaxtemp(outfilename,
                                                         float(perlist[2]),
                                                         float(perlist[1]))
        if not line:
            break
        # idlist和ecvaluelist是一一对应的，list是有顺序的
        idlist.append(perlist[0])
        ecvaluelist.append(perstationlist)
        # print iii
    # 模型预测
    ecvaluelist = numpy.array(ecvaluelist)
    print ecvaluelist
    xgbtrain = xgboost.DMatrix(ecvaluelist)
    result = bst.predict(xgbtrain)
    result01=bst01.predict(xgbtrain)
    print result,result01
    foretime = origintime + datetime.timedelta(hours=n * 3)
    db = MySQLdb.connect('bj28', 'admin', 'moji_China_123', 'moge')
    # db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
    cursor = db.cursor()
    origin = datetime.datetime.strftime(origintime, '%Y-%m-%d %H:%M:%S')
    forecast = datetime.datetime.strftime(foretime, '%Y-%m-%d %H:%M:%S')
    forecast_year = foretime.year
    forecast_month = foretime.month
    forecast_day = foretime.day
    forecast_hour = foretime.hour
    forecast_minute = foretime.minute
    csv = os.path.join(outpath, timestr + '_' + str(n) + '.csv')
    csvfile = open(csv, 'w')
    for j in range(len(stationlist)):
        stationid = stationlist[j][0]
        maxtemp = result[j]
        mintemp=result01[j]
        # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
        # sql = 'insert into t_r_ec_mos_city_forecast_ele (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ("' + stationid + '","' + origin + '","' + str(
        #     forecast) + '","' + str(forecast_year) + '","' + str(
        #     forecast_month) + '","' + str(forecast_day) + '","' + str(
        #     forecast_hour) + '","' + str(temp) + '")'
        sql='update t_r_ec_mos_city_forecast_ele set temp_max_6h=maxtemp,temp_min_6h=mintemp where city_id='+stationid+' and initial_time='+origin+'and forecast_time='+forecast
        csvfile.write(stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            maxtemp)+'","'+str(mintemp))
        csvfile.write('\n')
        # print sql
        cursor.execute(sql)
    db.commit()
    db.close()
    csvfile.close()
    newfilename = os.path.join(outpath + bz2filepath[1])
    shutil.move(outfilename, newfilename)


if '__name__==__main__':
    start1 = time.time()
    # 遍历所有文件，预测历史数据
    #path = '/mnt/data/ecdata'
    #outpath = '/mnt/data/grib'
    path = '/Users/yetao.lu/Desktop/mos/mosdata/d'
    outpath = '/Users/yetao.lu/Desktop/mos/temp'
    pool = multiprocessing.Pool(20)
    for root, dirs, files in os.walk(path):
        for file in files:
            if file[-4:] == 'grib':
                filename = os.path.join(root, file)
                start = file[3:9]
                end = file[11:17]
                print start, end
                starttime = datetime.datetime.strptime(start, '%m%d%H')
                origintime = datetime.datetime.strptime('2018' + start,
                                                        '%Y%m%d%H')
                endtime = datetime.datetime.strptime(end, '%m%d%H')
                d = (endtime - starttime).days
                f = (endtime - starttime).seconds / 3600
                i = (d * 24 + (endtime - starttime).seconds / 3600) / 3
                maxmodel = '/mnt/data/tempmax/max_temp' + str(i) + '.model'
                minmodel='/mnt/data/tempmin/min_temp'+ str(i) + '.model'
                # Predict(filename, modelname, origintime, i, outpath)
                pool.apply_async(Predict, (filename, maxmodel,minmodel, origintime, i, outpath,))
        pool.close()
        pool.join()
    end1 = time.time()
    print end1 - start1