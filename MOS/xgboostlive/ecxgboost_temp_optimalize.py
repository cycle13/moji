#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/4/25
description: 需要用pygrib库来解析了，EC数据更新导致pyNio的包无法正常读取文件e
首先下载数据，从OSS下载，解压，然后解析，再预测结果，预测结果入库
#效率太低了，优化一下，先把所有的矩阵读出来
"""
import Nio, datetime, os, xgboost, numpy, bz2, \
    multiprocessing, sys, MySQLdb, pygrib,threading
class MyThread(threading.Thread):

    def __init__(self,func,args=()):
        super(MyThread,self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None
def ReadMatrixFromGrib(newfile):
    matrixlist=[]
    if newfile[-4:] == 'grib':
        grbs = pygrib.open(newfile)
        #预测数据的列是不能打乱的，因此还是按照训练时的顺序获取数据
        grb=grbs.select(name='2 metre temperature')
        latlonArray=grb[0].values
        matrixlist.append(latlonArray)
        grb=grbs.select(name='2 metre dewpoint temperature')
        latlonArray=grb[0].values
        matrixlist.append(latlonArray)
        grb=grbs.select(name='10 metre U wind component')
        latlonArray=grb[0].values
        matrixlist.append(latlonArray)
        grb=grbs.select(name='10 metre V wind component')
        latlonArray=grb[0].values
        matrixlist.append(latlonArray)
        grb=grbs.select(name='Total cloud cover')
        latlonArray=grb[0].values
        matrixlist.append(latlonArray)
        grb=grbs.select(name='Low cloud cover')
        latlonArray=grb[0].values
        matrixlist.append(latlonArray)
        grb=grbs.select(name='Relative humidity',level=500)
        latlonArray=grb[0].values
        matrixlist.append(latlonArray)
        grb=grbs.select(name='Relative humidity',level=850)
        latlonArray=grb[0].values
        matrixlist.append(latlonArray)
    return matrixlist
def calculateValuesfrommatrixlist(matrixlist,indexlat,indexlon,idstr):
    dict={}
    vstring=[]
    for i in range(len(matrixlist)):
        latlonArray=matrixlist[i]
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
    dict[idstr]=vstring
    return dict
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
def calculatePerStationVariable(inputfile, longitude, latitude):
    if inputfile[-4:] == 'grib':
        grbs = pygrib.open(inputfile)
        # 经纬度索引
        indexlat = int((90 - latitude) / 0.1)
        indexlon = int((longitude + 180) / 0.1)
        vstring = []
        names = ['2 metre temperature', '2 metre dewpoint temperature',
            '10 metre U wind component', '10 metre V wind component',
            'Total cloud cover', 'Low cloud cover', 'Relative humidity']
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


def downloadfileandPredict(filefullname, outfilename, modelname):
    # 首先下载文件
    oss = OssUtils(False)
    oss.get_objs(filefullname, outfilename)
    # 先解压
    # bz2filepath='/Users/yetao.lu/Documents/mosdata/D1D01020000010203001.bz2'
    bz2filepath = outfilename
    # modelfile='/Users/yetao.lu/Desktop/mos/model/temp/ectemp5.model'
    modelfile = modelname
    newfile = bz2filepath[:-4] + '.grib'
    if not os.path.exists(newfile):
        a = bz2.BZ2File(bz2filepath, 'rb')
        b = open(newfile, 'wb')
        b.write(a.read())
        a.close()
        b.close()
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
        'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    
    bst = xgboost.Booster(params)
    bst.load_model(modelfile)
    # 站点列表数据
    stationlist = []
    # 所有站点的EC数据
    ecvaluelist = []
    # idlist存储站号
    idlist = []
    # csvfile='/Users/yetao.lu/Desktop/mos/stations.csv'
    csvfile = '/home/wlan_dev/stations.csv'
    fileread = open(csvfile, 'r')
    firstline = fileread.readline()
    iii = 0
    while True:
        iii = iii + 1
        line = fileread.readline()
        perlist = line.split(',')
        if len(perlist) >= 4:
            stationlist.append(perlist)
            perstationlist = calculateRaster(newfile, float(perlist[2]),
                                             float(perlist[1]))
        if not line:
            break
        # idlist和ecvaluelist是一一对应的，list是有顺序的
        idlist.append(perlist[0])
        ecvaluelist.append(perstationlist)
        # print iii
    # 模型预测
    ecvaluelist = numpy.array(ecvaluelist)
    xgbtrain = xgboost.DMatrix(ecvaluelist)
    result = bst.predict(xgbtrain)
    print result


def Predict(outfilename, modelname, origintime, n, outpath):
    bz2filepath = os.path.split(outfilename)
    timestr = datetime.datetime.strftime(origintime, '%Y%m%d%H%M%S')
    logfile = os.path.join(bz2filepath[0], timestr + '_' + str(n) + ".out")
    sys.stdout = open(logfile, "w")
    # 先解压
    modelfile = modelname
    newfile = os.path.join(outpath, bz2filepath[1][:-4] + '.grib')
    if not os.path.exists(newfile):
        a = bz2.BZ2File(outfilename, 'rb')
        b = open(newfile, 'wb')
        b.write(a.read())
        a.close()
        b.close()
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
    bst.load_model(modelfile)
    # 站点列表数据
    stationlist = []
    # 所有站点的EC数据
    ecvaluelist = []
    # idlist存储站号
    idlist = []
    csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
    # csvfile = '/home/wlan_dev/stations.csv'
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
            # perstationlist = calculatePerStationVariable(newfile,
            #                                              float(perlist[2]),
            #                                              float(perlist[1]))
            
        if not line:
            break
        # idlist和ecvaluelist是一一对应的，list是有顺序的
        idlist.append(perlist[0])
        ecvaluelist.append(perstationlist)
        # print iii
    # 模型预测
    ecvaluelist = numpy.array(ecvaluelist)
    xgbtrain = xgboost.DMatrix(ecvaluelist)
    result = bst.predict(xgbtrain)
    print result
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
        temp = result[j]
        # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
        sql = 'insert into t_r_ec_mos_city_forecast_ele (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ("' + stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(temp) + '")'
        print sql
        csvfile.write(stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            temp))
        csvfile.write('\n')
        # print sql
        cursor.execute(sql)
    db.commit()
    db.close()
    csvfile.close()


def Predict2(outfilename, modelname, origintime, n, outpath):
    bz2filepath = os.path.split(outfilename)
    timestr = datetime.datetime.strftime(origintime, '%Y%m%d%H%M%S')
    logfile = os.path.join(bz2filepath[0], timestr + '_' + str(n) + ".out")
    #sys.stdout = open(logfile, "w")
    # 先解压
    modelfile = modelname
    newfile = os.path.join(outpath, bz2filepath[1][:-4] + '.grib')
    if not os.path.exists(newfile):
        a = bz2.BZ2File(outfilename, 'rb')
        b = open(newfile, 'wb')
        b.write(a.read())
        a.close()
        b.close()
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
    bst.load_model(modelfile)
    # 站点列表数据
    stationlist = []
    # 所有站点的EC数据
    ecvaluelist = []
    # idlist存储站号
    idlist = []
    csvfile = '/Users/yetao.lu/Desktop/mos/stations1.csv'
    # csvfile = '/home/wlan_dev/stations.csv'
    fileread = open(csvfile, 'r')
    fileread.readline()
    iii = 0
    threadlist=[]
    while True:
        iii = iii + 1
        line = fileread.readline()
        perlist = line.split(',')
        if len(perlist) >= 4:
            stationlist.append(perlist)
            # 经纬度索引
            latitude=float(perlist[1])
            longitude=float(perlist[2])
            indexlat = int((90 - latitude) / 0.1)
            indexlon = int((longitude + 180) / 0.1)
            vmatrix=ReadMatrixFromGrib(newfile)
            #perstationlist=calculateValuesfrommatrixlist(vmatrix,indexlat,indexlon)
            t = MyThread(calculateValuesfrommatrixlist, args=(vmatrix,indexlat,indexlon,perlist[0]))
            threadlist.append(t)
            t.start()
        if not line:
            break
        # idlist和ecvaluelist是一一对应的，list是有顺序的
        # idlist.append(perlist[0])
        # ecvaluelist.append(perstationlist)
        # print iii
    dictalllist=[]
    for l in threadlist:
        l.join()
        dictalllist.append(l.get_result())
    print len(dictalllist)
    for g in range(len(dictalllist)):
        for key in dictalllist[g]:
            idlist.append(key)
            ecvaluelist.append(dictalllist[g][key])
    # 模型预测
    ecvaluelist = numpy.array(ecvaluelist)
    print ecvaluelist
    xgbtrain = xgboost.DMatrix(ecvaluelist)
    result = bst.predict(xgbtrain)
    print result
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
    csv = os.path.join(outpath, timestr + '_' + str(n) + '.csv')
    csvfile = open(csv, 'w')
    for j in range(len(stationlist)):
        stationid = stationlist[j][0]
        temp = result[j]
        # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
        sql = 'insert into t_r_ec_mos_city_forecast_ele (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ("' + stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(temp) + '")'
        print sql
        csvfile.write(stationid + '","' + origin + '","' + str(
            forecast) + '","' + str(forecast_year) + '","' + str(
            forecast_month) + '","' + str(forecast_day) + '","' + str(
            forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            temp))
        csvfile.write('\n')
        # print sql
        cursor.execute(sql)
    db.commit()
    db.close()
    csvfile.close()
if '__name__==__main__':
    starttime=datetime.datetime.now()
    # 遍历所有文件，预测历史数据
    # path='/mnt/data/test'
    # outpath='home/wlan_dev/result'
    path = '/Users/yetao.lu/Desktop/mos/mosdata/d'
    outpath = '/Users/yetao.lu/Desktop/mos/temp'
    # pool = multiprocessing.Pool(20)
    for root, dirs, files in os.walk(path):
        for file in files:
            if file[-3:] == 'bz2':
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
                modelname = '/Users/yetao.lu/Desktop/mos/model/temp/ectemp' + str(
                    i) + '.model'
                Predict2(filename, modelname, origintime, i, outpath)
        #       pool.apply_async(Predict,(filename,modelname,origintime,i,outpath,))
        # pool.close()
        # pool.join()
    endtime = datetime.datetime.now()
    print(endtime - starttime).seconds