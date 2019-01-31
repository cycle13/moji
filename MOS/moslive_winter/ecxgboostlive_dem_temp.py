#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/4
description:线上的最新最高气温、最低气温、气温预测,晴雨预测
该是根据DEM模型来进行预测的
20180709修改：把DEM数据独立出来，
总降水也要减去1.87进行预测
"""
import datetime, os, xgboost, numpy, bz2, \
    multiprocessing, sys, MySQLdb, pygrib, logging,time
from osgeo import gdal
from decimal import Decimal
from sklearn.externals import joblib
from apscheduler.schedulers.background import BackgroundScheduler
from roottask import app
from celery.utils.log import get_task_logger
logger=get_task_logger(__name__)
@app.task
def perstationvalue(vstring, latlonArray, indexlat, indexlon):
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
@app.task
def demdatefromcsvTodict(demcsv):
    dictdem = {}
    fileread = open(demcsv, 'r')
    while True:
        line = fileread.readline()
        linearray = line.split(',')
        if linearray > 10:
            stationid001 = linearray[0]
            vstring = []
            for i in range(1, len(linearray)):
                vstring.append(linearray[i])
            dictdem[stationid001] = vstring
        if not line:
            break
    return dictdem
@app.task
def calculateStationVariable(tempvariablelist, inputfile,stationlist, csvfile, demcsv):
    if inputfile[-3:] == '001' :
        grbs = pygrib.open(inputfile)
        # grbs.seek(0)
        # for grb in grbs:
        #     print grb
        # 把数据矩阵都拿出来
        grb = grbs.select(name='2 metre temperature')
        tempArray = grb[0].values
        grb = grbs.select(name='2 metre dewpoint temperature')
        dewpointArray = grb[0].values
        grb = grbs.select(name='10 metre U wind component')
        u10Array = grb[0].values
        grb = grbs.select(name='10 metre V wind component')
        v10Array = grb[0].values
        grb = grbs.select(name='Total cloud cover')
        tccArray = grb[0].values
        grb = grbs.select(name='Low cloud cover')
        lccArray = grb[0].values
        grb = grbs.select(name='Relative humidity', level=500)
        rh500Array = grb[0].values
        grb = grbs.select(name='Relative humidity', level=850)
        rh850Array = grb[0].values
        grb = grbs.select(name='Geopotential')
        geoArray = grb[0].values
        idlist = []
        fileread = open(csvfile, 'r')
        fileread.readline()
        iii = 0
        valuecsv=open('/home/wlan_dev/value.csv','w')
        while True:
            iii = iii + 1
            logger.info(iii)
            line = fileread.readline()
            perlist = line.split(',')
            if len(perlist) >= 4:
                stationlist.append(perlist)
                station_id = perlist[0]
                latitude = float(perlist[1])
                longitude = float(perlist[2])
                alti = float(perlist[3])
                idlist.append(perlist[0])
                logger.info(station_id)
                # 经纬度索引
                indexlat = int((90 - latitude) / 0.1)
                indexlon = int((longitude + 180) / 0.1)
                # 定义气温要素训练的一个站点的一条记录为templist，存多个训练因子
                templist = []
                vstring = []
                # 各类要素按照要素训练的顺序保持一致:根据索引取值
                # 气温数据的获取从这里开始：顺序是：
                # 2T,2D,10u,10v,tcc,lcc,z,500Rh,850rh,lat,lon,alti,dem16个点。
                # 气温
                perstationvalue(vstring, tempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 露点温度
                perstationvalue(vstring, dewpointArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 10
                perstationvalue(vstring, u10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 10V
                perstationvalue(vstring, v10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 总云量
                perstationvalue(vstring, tccArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 低云量
                perstationvalue(vstring, lccArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 位势高度
                perstationvalue(vstring, geoArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 500hpaRH
                perstationvalue(vstring, rh500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 850hPaRH
                perstationvalue(vstring, rh850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 站点的纬度经度高度加到矩阵中
                templist.append(latitude)
                templist.append(longitude)
                templist.append(alti)
                # dem16个点加载到矩阵中
                demdict = demdatefromcsvTodict(demcsv)
                vstring = demdict[station_id]
                for i in range(len(vstring)):
                    templist.append(vstring[i])
            # 添加到总的矩阵中
            tempvariablelist.append(templist)
            for pp in range(len(templist)):
                valuecsv.write(str(templist[pp])+',')
            valuecsv.write('\n')
            if not line:
                break
        # 关闭grib文件
        valuecsv.close()
        grbs.close()
@app.task
def Predict(outfilename, modelname, tempscalerfile, origintime, foretime,csvfile, demcsv):
    tempvariablelist = []
    stationlist = []
    calculateStationVariable(tempvariablelist,outfilename,stationlist, csvfile, demcsv)
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
        # 'nthread':3,# cpu 线程数,不设置取最大值
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    bst = xgboost.Booster(params)
    bst.load_model(modelname)
    # 气温模型预测
    ecvaluelist = numpy.array(tempvariablelist)
    # ecvaluelist=ecvaluelist.astype('float64')
    # logger.info('ecvaluelist')
    logger.info(ecvaluelist.shape)
    # print ecvaluelist
    # logger.info('\n')
    # 加载标准化预处理文件，对数据进行与模型一致的标准化
    scaler = joblib.load(tempscalerfile)
    # transform后必须重新赋值给新的变量，原来矩阵是不变的
    ecvaluelist_t = scaler.transform(ecvaluelist)
    ###哎哎哎
    xgbtrain = xgboost.DMatrix(ecvaluelist)
    result = bst.predict(xgbtrain)
    # logger.info('result')
    logger.info(result.shape)
    # logger.info('\n')
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
    # csvfile.close()
    #os.remove(outfilename)
    logger.info(outfilename)
    logger.removeHandler(file_handler)
'''
path:ec数据的存储路径
outpath:结果输出路径
csvfile:站点类别的文件
demcsv:高程数据的文件
starttime:起报时间
'''
@app.task
def modelProdict(path,csvfile,demcsv,starttime):
    bz2list=[]
    for root, dirs, files in os.walk(path):
        logger.info('模型预测的文件：'+path)
        rootpath = root
        for file in files:
            if file[-3:] == '001' and file[:3] == 'D1D':
                bz2list.append(file)
    bz2list001 = sorted(bz2list)
    pool=multiprocessing.Pool(processes=15)
    for i in range(len(bz2list001)):
        file = bz2list[i]
        # 降水需要取前一个时次的文件
        start = file[3:9]
        end = file[11:17]
        start001 = str(starttime.year) + start
        end001 = str(starttime.year) + end
        if start001 > end001:
            end001 = str(starttime.year + 1) + end
        starttime = datetime.datetime.strptime(start001, '%Y%m%d%H')
        origintime = datetime.datetime.strptime(str(starttime.year) + start,
                                                '%Y%m%d%H')
        endtime = datetime.datetime.strptime(end001, '%Y%m%d%H')
        d = (endtime - starttime).days
        f = (endtime - starttime).seconds / 3600
        hours = (d * 24 + (endtime - starttime).seconds / 3600)
        # 气温和降水的ID获取,把0排除掉了,如果hours<=3则降水不用减前一个时次的
        #历史数据是144个小时是逐3小时预报，线上数据只有120小时是逐3小时，但是不影响取模型数据
        if hours<10 and hours > 0:
            id = '00'+str(hours)
        elif hours>=10 and hours<100:
            id = '0'+str(hours)
        elif hours>=100:
            id=str(hours)
        elif hours == 0:
            continue
        # 当前文件
        newfile = file
        newfilepath = os.path.join(rootpath, newfile)
        #filename的路径，这里是grib数据
        filename = newfilepath
        modelname = '/home/wlan_dev/wintermodel/temp_model' + id+ '.model'
        tempscalerfile = '/home/wlan_dev/wintermodel/temp_scale' + id + '.save'
        # args= 不能省略
        if os.path.exists(modelname) and os.path.exists(tempscalerfile):
            logger.info(filename)
            pool.apply_async(Predict, args=(filename, modelname, tempscalerfile, origintime, endtime,csvfile, demcsv))
    pool.close()
    pool.join()
if __name__ == "__main__":
    # 加日志
    logger = logging.getLogger('learing.logger')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    logfile = '/home/wlan_dev/lea.log'
    #logfile='/Users/yetao.lu/Desktop/mos/temp/loging.log'
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值
    
    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    #starttime = datetime.datetime.now()
    starttime=datetime.datetime.strptime('2018-10-01 00:00:00','%Y-%m-%d %H:%M:%S')
    # 遍历所有文件，预测历史数据
    # path = '/Users/yetao.lu/Desktop/mos/new'
    # outpath = '/Users/yetao.lu/Desktop/mos/temp'
    # csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
    # demcsv = '/Users/yetao.lu/Desktop/mos/dem.csv'
    # 遍历2867个站点
    path = '/moji/ecdata'
    outpath = '/home/wlan_dev/result'
    csvfile = '/mnt/data/mosfile/stations.csv'
    #修改dem数据由0.125分辨率改为0.1
    demcsv = '/mnt/data/mosfile/dem0.1.csv'
    yearint=starttime.year
    hours=starttime.hour
    midpath = path + '/' + str(yearint)
    if hours>12:
        datestr=datetime.datetime.strftime(starttime,'%Y-%m-%d')
    else:
        nowdate=starttime+datetime.timedelta(days=-1)
        datestr=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
    ecpath=midpath+'/'+datestr
    if not os.path.exists(ecpath):
        logger.info(ecpath+'不存在')
    #ecpath='/Users/yetao.lu/Desktop/mos/new'ex
    if not os.path.exists(outpath):
        os.makedirs(outpath)
    #print '/moji/ecdata/2018/2018-10-01',csvfile,demcsv,starttime
    modelProdict('/moji/ecdata/2018/2018-10-01',csvfile,demcsv,starttime)
    # 创建后台执行的schedulers
    # scheduler = BackgroundScheduler()
    # # 添加调度任务
    # # 调度方法为timeTask,触发器选择定时，
    # scheduler.add_job(modelProdict, 'cron', hour='6,18', max_instances=1,args=(ecpath,outpath,csvfile,demcsv,starttime))
    # try:
    #     scheduler.start()
    #     while True:
    #         time.sleep(2)
    # except Exception as e:
    #     print e.message
