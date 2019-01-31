#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/4
description:线上的最新最高气温、最低气温、气温预测
"""
import Nio, datetime, os, xgboost, numpy, bz2, \
    multiprocessing, sys, MySQLdb, pygrib,shutil,logging,time
from sklearn.externals import joblib
from apscheduler.schedulers.background import BackgroundScheduler
from mosapp import app
from celery.utils.log import get_task_logger
logger=get_task_logger(__name__)
@app.task()
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
@app.task()
def calculateStationVariable(tempvariablelist,inputfile,stationlist,csvfile):
    if inputfile[-3:]=='001':
        grbs=pygrib.open(inputfile)
        # grbs.seek(0)
        # print grbs.seek(0)
        # for grb in grbs:
        #     print grb
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
                rainlist=[]
                vstring=[]
                #各类要素按照要素训练的顺序保持一致:根据索引取值
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
                vstring=[]
            #添加到总的矩阵中
            tempvariablelist.append(templist)
            if not line:
                break
@app.task
def Predict(hours,outfilename,modelname,tempscalerfile, origintime, foretime, outpath,csvfile):
    try:
        #气温
        print 'aaa'
        tempvariablelist = []
        rainvariablelist=[]
        stationlist=[]
        calculateStationVariable(tempvariablelist,outfilename,stationlist,csvfile)
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
        # 气温模型预测
        ecvaluelist = numpy.array(tempvariablelist)
        #ecvaluelist=ecvaluelist.astype('float64')
        #print ecvaluelist
        #加载标准化预处理文件，对数据进行与模型一致的标准化
        scaler=joblib.load(tempscalerfile)
        #transform后必须重新复制，原来矩阵是不变的
        ecvaluelist_t=scaler.transform(ecvaluelist)
        #print ecvaluelist,ecvaluelist_t
        xgbtrain = xgboost.DMatrix(ecvaluelist_t)
        result = bst.predict(xgbtrain)
        #logger.info(result)
 
        #预测结果入库
        db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
        #db = MySQLdb.connect('192.168.10.84', 'admin', 'moji_China_123', 'moge')
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
        sql = 'replace into t_r_ec_city_forecast_ele_mos (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
        L=[]
        for j in range(len(stationlist)):
            perstationlist = []
            stationid = stationlist[j][0]
            #遍历降水预测中是否有该站点
            prevalue=0
            temp = result[j]
            #每个站点存储
            perstationlist.append(stationid)
            perstationlist.append(origin)
            perstationlist.append(forecast)
            perstationlist.append(forecast_year)
            perstationlist.append(forecast_month)
            perstationlist.append(forecast_day)
            perstationlist.append(forecast_hour)
            perstationlist.append(temp)
            #logger.info(perstationlist)
            L.append(perstationlist)
            # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
            # sql = 'insert into t_r_ec_city_forecast_ele_mos (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,temp_max_6h,temp_min_6h,rainstate,precipitation)VALUES ("' + stationid + '","' + origin + '","' + str(
            #     forecast) + '","' + str(forecast_year) + '","' + str(
            #     forecast_month) + '","' + str(forecast_day) + '","' + str(
            #     forecast_hour) + '","' + str(temp) + '","' + str(maxtemp)+ '","' + str(mintemp)+'","' + str(rainstate)+'","' + str(prevalue)+ '")'
            # csvfile.write(stationid + '","' + origin + '","' + str(
            #     forecast) + '","' + str(forecast_year) + '","' + str(
            #     forecast_month) + '","' + str(forecast_day) + '","' + str(
            #     forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            #     temp)+ '","' + str(maxtemp)+ '","' + str(mintemp)+'","' + str(rainstate)+'","' + str(prevalue))
            # csvfile.write('\n')
            #cursor.execute(sql)
        cursor.executemany(sql,L)
        db.commit()
        db.close()
        # csvfile.close()
        logger.info(outfilename)
    except Exception as e:
        logger.info(e.message)
@app.task
def modelpredict001(path,outpath,csvfile,starttime):
    fromtime=datetime.datetime.now()
    pool = multiprocessing.Pool(16)
    #打开文件读取变量的时间比较长，因此想把所有要素的预测都做了,
    bz2list=[]
    rootpath=''
    #遍历所有的文件存入列表
    for root, dirs, files in os.walk(path):
        rootpath=root
        for file in files:
            if file[-3:] == '001' and file[:3]=='D1D':
                filename=os.path.join(root,file)
                bz2list.append(file)
    #把列表排序
    bz2list001=sorted(bz2list)
    logger.info(bz2list001)
    for i in range(len(bz2list001)):
        file=bz2list[i]
        bz2file = os.path.join(root, file)
        start = file[3:9]
        end = file[11:17]
        print start, end
        start001=str(starttime.year)+start
        end001=str(starttime.year)+end
        if start001>end001:
            end001=str(starttime.year+1)+end
        starttime = datetime.datetime.strptime(start001, '%Y%m%d%H')
        origintime = datetime.datetime.strptime(str(starttime.year) + start,
                                                '%Y%m%d%H')
        endtime = datetime.datetime.strptime(end001, '%Y%m%d%H')
        d = (endtime - starttime).days
        f = (endtime - starttime).seconds / 3600
        hours = (d * 24 + (endtime - starttime).seconds / 3600)
        logger.info('hours='+str(hours))
        #气温和降水的ID获取,把0排除掉了,如果hours<=3则降水不用减前一个时次的
        if hours<=144 and hours>0:
            id=hours/3
        elif hours>144:
            id=48+(hours-144)/6
        elif hours==0:
            continue
        #上一个文件的名字和路径
        #加载模型文件
        modelname='/mnt/data/tempscaler/ectt'+str(id)+'.model'
        tempscalerfile='/mnt/data/tempscaler/scale' + str(id) + '.save'
        # modelname = '/Users/yetao.lu/Desktop/mos/model/temp/ectt' + str(
        #     id) + '.model'
        # tempscalerfile = '/Users/yetao.lu/Desktop/mos/model/temp/scale' + str(
        #     id) + '.save'
        print tempscalerfile
        #Predict(hours,filename ,modelname, tempscalerfile, origintime, endtime, outpath,csvfile)
        pool.apply_async(Predict,args=(hours,filename ,modelname, tempscalerfile, origintime, endtime, outpath,csvfile))
    pool.close()
    pool.join()
    totime=datetime.datetime.now()
    alltime=totime-fromtime
    logger.info(alltime.seconds)
# if __name__ == "__main__":
#     #加日志
#     logger = logging.getLogger('learing.logger')
#     # 指定logger输出格式
#     formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
#     # 文件日志learning
#     logfile='/home/wlan_dev/learning.log'
#     #logfile='/Users/yetao.lu/Desktop/mos/temp/loging.log'
#     file_handler = logging.FileHandler(logfile)
#     file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
#     # 控制台日志
#     console_handler = logging.StreamHandler(sys.stdout)
#     console_handler.formatter = formatter  # 也可以直接给formatter赋值
#
#     # 为logger添加的日志处理器
#     logger.addHandler(file_handler)
#     logger.addHandler(console_handler)
#
#     # 指定日志的最低输出级别，默认为WARN级别
#     logger.setLevel(logging.INFO)
#     starttime=datetime.datetime.now()
#     # 遍历所有文件，预测历史数据
#     yearint=datetime.datetime.now().year
#     path='/moji/ecdata'
#     hours=datetime.datetime.now().hour
#     midpath = path + '/' + str(yearint)
#     if hours>12:
#         datestr=datetime.datetime.strftime(starttime,'%Y-%m-%d')
#     else:
#         nowdate=starttime+datetime.timedelta(days=-1)
#         datestr=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
#     ecpath=midpath+'/'+datestr
#     if not os.path.exists(ecpath):
#         logger.info(ecpath+'不存在')
#     outpath='/home/wlan_dev/result'
#     if not os.path.exists(outpath):
#         os.makedirs(outpath)
#     # path = '/Users/yetao.lu/Desktop/mos/new'
#     # outpath = '/Users/yetao.lu/Desktop/mos/temp'
#     # 遍历2867个站点
#     # csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
#     csvfile = '/home/wlan_dev/stations.csv'
#     if not os.path.exists(csvfile):
#         logger.info(csvfile+'--csv文件不存在')
#     modelpredict001('/home/wlan_dev/tmp/2018-07-10',outpath,csvfile,starttime)
#     # 创建后台执行的schedulers
#     scheduler = BackgroundScheduler()
#     # 添加调度任务
#     # 调度方法为timeTask,触发器选择定时，
#     scheduler.add_job(modelpredict001, 'cron', hour='6,18', max_instances=1,args=(ecpath,outpath,csvfile,starttime))
#     try:
#         scheduler.start()
#         while True:
#             time.sleep(2)
#     except Exception as e:
#         print e.message
