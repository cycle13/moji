#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/8
description:根据所有模型用一个卡串行，修改为一个模型用一个看，服务器有四个看，至少可以4个并行
"""
import logging,sys,datetime,os,pygrib,numpy,MySQLdb,time,math
import xgboost,multiprocessing
from sklearn.externals import joblib
from apscheduler.schedulers.background import BackgroundScheduler
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
def calculate16gribvalue(latlonArray,indexlat,indexlon,vstring):
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
def calculatedemvalue(demcsv):
    demdict={}
    csvread=open(demcsv,'r')
    while True:
        line=csvread.readline()
        linearray=line.split(',')
        if len(linearray)>2:
            demdict[linearray[0]]=linearray
        if not line:
            break
    return demdict
'''
遍历文件要放在外层，这里是单个文件进行预测
'''
def predictmodel(file,filefullpath,modelfile,scalefile,stationlist,demdict,origintime,foretime,gpu):
    os.environ["CUDA_VISIBLE_DEVICES"] = gpu
    allvaluelist=[]
    if file[-3:]=='001' and (file[:3]=='D2S' or file[:3]=='D2D'):
        grbs=pygrib.open(filefullpath)
        grb_2t = grbs.select(name='2 metre temperature')
        tempArray = grb_2t[0].values
        grb_2d = grbs.select(name='2 metre dewpoint temperature')
        dewpointArray = grb_2d[0].values
        grb_10u = grbs.select(name='10 metre U wind component')
        u10Array = grb_10u[0].values
        grb_10v = grbs.select(name='10 metre V wind component')
        v10Array = grb_10v[0].values
        grb_tcc = grbs.select(name='Total cloud cover')
        tccArray = grb_tcc[0].values
        grb_lcc = grbs.select(name='Low cloud cover')
        lccArray = grb_lcc[0].values
        grb_z = grbs.select(name='Geopotential')
        geoArray=grb_z[0].values
        grb_500rh = grbs.select(name='Relative humidity', level=500)
        rh500Array = grb_500rh[0].values
        grb_850rh = grbs.select(name='Relative humidity', level=850)
        rh850Array = grb_850rh[0].values
        #遍历站点->要素遍历、
        for i in range(len(stationlist)):
            #print len(stationlist)
            perlist=stationlist[i]
            stationid=perlist[0]
            latitude=float(perlist[1])
            longitude=float(perlist[2])
            alti=float(perlist[3])
            #站点左上角点的索引
            indexlat = int((60 - latitude) / 0.1)
            indexlon = int((longitude -60) / 0.1)
            per_station_value_list=[]
            calculate16gribvalue(tempArray,indexlat,indexlon,per_station_value_list)
            calculate16gribvalue(dewpointArray,indexlat,indexlon,per_station_value_list)
            calculate16gribvalue(u10Array,indexlat,indexlon,per_station_value_list)
            calculate16gribvalue(v10Array,indexlat,indexlon,per_station_value_list)
            calculate16gribvalue(tccArray,indexlat,indexlon,per_station_value_list)
            calculate16gribvalue(lccArray,indexlat,indexlon,per_station_value_list)
            calculate16gribvalue(geoArray,indexlat,indexlon,per_station_value_list)
            calculate16gribvalue(rh500Array,indexlat,indexlon,per_station_value_list)
            calculate16gribvalue(rh850Array,indexlat,indexlon,per_station_value_list)
            per_station_value_list.append(latitude)
            per_station_value_list.append(longitude)
            per_station_value_list.append(alti)
            # 站点高程：取计算好的站点周边16个点的高程值
            demlist = demdict[stationlist[i][0]]
            for u in range(1, len(demlist), 1):
                per_station_value_list.append(float(demlist[u]))
            allvaluelist.append(per_station_value_list)
            #print(per_station_value_list)
    trainarray=numpy.array(allvaluelist)
    params001 = {
        'tree_method': 'gpu_hist',
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
        'eta': 0.01,  # 如同学习率
        'seed': 1000,
        # 'nthread':3,# cpu 线程数,不设置取最大值
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1,
        'n_gpus': 1
    }
    xgbst=xgboost.Booster(params001)
    xgbst.load_model(modelfile)
    scaler=joblib.load(scalefile)
    #print(modelfile,scalefile)
    trainarray_t=scaler.transform(trainarray)
    #标准化后的矩阵坑我2次了:看好是标准化后的还是标准化前的
    xgbtrain=xgboost.DMatrix(trainarray_t)
    result=xgbst.predict(xgbtrain)
    #print(result)
    logger.info(result)
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
    sql = 'replace into t_r_ec_city_forecast_ele_mos_dem (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
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
        # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
        #         # sql = 'insert into t_r_ec_city_forecast_ele_mos (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,temp_max_6h,temp_min_6h,rainstate,precipitation)VALUES ("' + stationid + '","' + origin + '","' + str(
        #         #     forecast) + '","' + str(forecast_year) + '","' + str(
        #         #     forecast_month) + '","' + str(forecast_day) + '","' + str(
        #         #     forecast_hour) + '","' + str(temp) + '","' + str(maxtemp)+ '","' + str(mintemp)+'","' + str(rainstate)+'","' + str(prevalue)+ '")
        #         # csvfile.write(stationid + '","' + origin + '","' + str(
        #         #     forecast) + '","' + str(forecast_year) + '","' + str(
        #         #     forecast_month) + '","' + str(forecast_day) + '","' + str(
        #         #     forecast_hour) + '","' + str(forecast_minute) + '","' + str(
        #         #     temp)+ '","' + str(maxtemp)+ '","' + str(mintemp)+'","' + str(rainstate)+'","' + str(prevalue))
        #         # csvfile.write('\n')
        #         # print sql
        #         # cursor.execute(sql)
    cursor.executemany(sql, L)
    db.commit()
    db.close()
def calculate_avg_temp_24h_from3h_accurity(db, initial, pdate, odate, ii):
    initialtime = datetime.datetime.strptime(initial, '%Y-%m-%d %H:%M:%S')
    year002 = initialtime.year
    month002 = initialtime.month
    day002 = initialtime.day
    hour002 = initialtime.hour
    cursor001 = db.cursor()
    #选择MOS统计之后的24小时气温、24小时气温最大值，24小时气温最小值
    sql = 'select city_id,initial_time,avg(temperature),max(temperature),min(temperature) from t_r_ec_city_forecast_ele_mos_dem where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '" group by city_id'
    print sql
    cursor001.execute(sql)
    rows = cursor001.fetchall()
    if rows==():
        #logger.info('该时间段没有数据' +sql)
        print '该时间段没有数据'
        return
    #实况数据
    sql_live = 'select v01000,AVG(TEM),MAX(TEM_Max),MIN(TEM_Min) from t_r_surf_hour_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and TEM<90 and TEM_Max<90 and TEM_Min<90 group by v01000;'
    print sql_live
    #logger.info(sql_live)
    cursor = db.cursor()
    cursor.execute(sql_live)
    rows_live = cursor.fetchall()
    #print len(rows_live)
    rowdict = {}
    nn = 0
    # 3度的平均气温准确率
    na_avg = 0
    # 3度的气温最大值准确率:用逐小时的气温最大值和预报最高气温计算
    na_max = 0
    # 3度的气温最小值准确率：用逐小时的气温最小值和预报最低气温计算
    na_min = 0
    #实况数据存入字典
    for row_live in rows_live:
        rowdict[row_live[0]] = row_live
    #预报站点在实况字典里，计算accu
    for row in rows:
        if row[0] in rowdict.keys():
            nn = nn + 1
            #print rowdict[row[0]]
            avg_temp_live = float(rowdict[row[0]][1])
            avg_temp = float(row[2])
            if abs(avg_temp_live - avg_temp) < 3:
                na_avg = na_avg + 1
            avg_mmax_live = float(rowdict[row[0]][2])
            max_temp = float(row[3])
            if abs(avg_mmax_live - max_temp) < 3:
                na_max = na_max + 1
            avg_mmin_live = float(rowdict[row[0]][3])
            min_temp = float(row[4])
            if abs(avg_mmin_live - min_temp) < 3:
                na_min = na_min + 1
    # 把一条SQL的值加载list中，然后把这个list合并到总的list中
    #print na_avg, na_max, na_min, na_mmax, na_mmin
    cursor = db.cursor()
    sql='replace into t_r_avg_temp_est_daily_mos_dem(initial_time,forecast_days,initial_year,initial_month,initial_day,initial_hour,NN,na_avg,na_max,na_min)VALUES ("'+initial+'","'+str(ii+1)+'","'+str(year002)+'","'+str(month002)+'","'+str(day002)+'","'+str(hour002)+'","'+str(nn)+'","'+str(na_avg)+'","'+str(na_max)+'","'+str(na_min)+'")'
    #sql = 'insert into t_r_avg_temp_est_daily_mos(initial_time,forecast_days,initial_year,initial_month,initial_day,initial_hour,NN,na_avg,na_max,na_min,na_mmax,na_mmin)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    #print sql
    cursor.execute(sql)
    db.commit()
def calculate_temp3h_accurity(db,initial,pdate,odate,ii):
    #72小时气温的准确率
    cursor001 = db.cursor()
    sql = 'select city_id,initial_time,temperature from t_r_ec_city_forecast_ele_mos_dem where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '"'
    #print sql
    cursor001.execute(sql)
    rows_f=cursor001.fetchall()
    #print rows_f,type(rows_f)
    if rows_f==():
        #logger.info('该时间段没有数据' +sql)
        print '该时间段没有数据'
        return
    #取逐小时的共3h的均值
    sql_live = 'select v01000,AVG(TEM) from t_r_surf_hour_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and TEM <80 group by v01000;'
    #print sql_live
    cursor=db.cursor()
    cursor.execute(sql_live)
    rows_l=cursor.fetchall()
    dict_live={}
    nn=0
    na=0
    sum_mae=0
    sum_rmse=0
    #3h的气温存入字典
    for row in rows_l:
        dict_live[row[0]]=float(row[1])
    #判断预报站号在实况字典中，计算准确率
    for row_f in rows_f:
        if row_f[0] in dict_live.keys():
            nn=nn+1
            livetemp=float(dict_live[row_f[0]])
            foretemp=float(row_f[2])
            if abs(livetemp-foretemp)<3:
                na=na+1
            sum_mae=sum_mae+abs(livetemp-foretemp)
            sum_rmse=sum_rmse+pow(livetemp-foretemp,2)
    if nn<>0:
        mae=sum_mae/nn
        rmse=math.sqrt(sum_rmse/nn)
        accu=na*100/nn
        sql_into='replace into t_r_3h_temp_accu_mos_dem(initial_time,forecast_hours,temp_accu,temp_mae,temp_rmse)VALUES ("'+initial+'","'+str(ii)+'","'+str(accu)+'","'+str(mae)+'","'+str(rmse)+'")'
        #print sql_into
        #logger.info(sql_into)
        cursor=db.cursor()
        cursor.execute(sql_into)
        db.commit()
def readymodel(starttime001,path):
    outpath='/home/wlan_dev/mos/mosresult'
    csvfile='/home/wlan_dev/mos/mosbase/stations.csv'
    demcsv='/home/wlan_dev/mos/mosbase/dem0.1.csv'
    #给定的时间是实时时间凌晨3点，计算的是前一天的12时，相差15个小时
    starttime=starttime001
    yearint=starttime.year
    hours=starttime.hour
    midpath = path + '/' + str(yearint)
    #给定时间大于12时，计算今天的数据，如果小于12计算昨天的数据。
    # if hours>=12:
    #     #     #     datestr=datetime.datetime.strftime(starttime,'%Y-%m-%d')
    #     #     # else:
    #     #     #     nowdate=starttime+datetime.timedelta(days=-1)
    #     #     #     datestr=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
    datestr=datetime.datetime.strftime(starttime,'%Y-%m-%d')
    ecpath=midpath+'/'+datestr
    stationlist=getStationList(csvfile)
    demdict=calculatedemvalue(demcsv)
    #根据文件名称中的时间差来计算要用哪个模型文件和标准化文件
    #遍历文件夹放在list列表中
    ecfilelist=[]
    for root,dirs,files in os.walk(ecpath):
        for file in files:
            print file
            rootpath=root
            if file[-3:] == '001' and (file[:3] == 'D2S' or file[:3]=='D2D') and file[7:9]=='12':
                ecfilelist.append(file)
    #对ec列表进行排序:排序的目的是什么？忘记了
    sortecfilelist=sorted(ecfilelist)
    #开2个进程池，分别各占一张卡，每次只训练2个模型，显卡的显存限制2个模型是最好的，也就是进程池里面有一个进程
    pool001 = multiprocessing.Pool(processes=2)
    pool002 = multiprocessing.Pool(processes=2)
    for i in range(len(sortecfilelist)):
        file=sortecfilelist[i]
        # 取文件中时间字段
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
        #计算两个时间差几个小时
        d = (endtime - starttime).days
        f = (endtime - starttime).seconds / 3600
        hours = (d * 24 + (endtime - starttime).seconds / 3600)
        if hours<10 and hours > 0:
            id = '00'+str(hours)
        elif hours>=10 and hours<100:
            id = '0'+str(hours)
        elif hours>=100:
            id=str(hours)
        elif hours == 0:
            continue
        #用小时数来定义预报时次，从而确定模型文件和标准化文件
        filefullpath=os.path.join(rootpath,file)
        datamonth=starttime.month
        if datamonth>=5 and datamonth<=9:
            modelfile = '/home/wlan_dev/mos/demtemp/temp_model' + id + '.model'
            scalefile = '/home/wlan_dev/mos/demtemp/temp_scale' + id + '.save'
        else:
            modelfile = '/home/wlan_dev/mos/wintermodel_gpu/temp_model'+id+'.model'
            scalefile = '/home/wlan_dev/mos/wintermodel_gpu/temp_scale'+id+'.save'
        logger.info(id)
        logger.info(file)
        if os.path.exists(modelfile) and os.path.exists(scalefile):
            #阻塞的进程池
            if int(id)<=90:
                #predictmodel(file,filefullpath,modelfile,scalefile,stationlist,demdict,origintime,endtime)
                pool001.apply_async(predictmodel,args=(file,filefullpath,modelfile,scalefile,stationlist,demdict,origintime,endtime,'0'))
            elif int(id)>90:
                pool002.apply_async(predictmodel,args=(file,filefullpath,modelfile,scalefile,stationlist,demdict,origintime,endtime,'1'))
    pool001.close()
    pool002.close()
    pool001.join()
    pool002.join()
    #增加计算准确率函数,计算10天前的准确率
    initialtime001=starttime
    initial = datetime.datetime.strftime(initialtime001, '%Y-%m-%d %H:%M:%S')
    # logger.info(initial)
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    # db = MySQLdb.connect('192.168.10.84', 'admin', 'moji_China_123', 'moge')
    for n in range(10):
        print n
        pdatetime = initialtime001 + datetime.timedelta(days=n)
        pdate = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
        odatetime = initialtime001 + datetime.timedelta(days=n + 1)
        odate = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
        calculate_avg_temp_24h_from3h_accurity(db, initial, pdate, odate, n)
    for u in range(64):
        if u < 48:
            pdatetime = initialtime001 + datetime.timedelta(hours=3 * u)
            pdate = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
            odatetime = initialtime001 + datetime.timedelta(hours=3 * (u + 1))
            odate = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
            print initial, pdate, odate, (u + 1) * 3
            calculate_temp3h_accurity(db, initial, pdate, odate, (u + 1) * 3)
        else:
            pdatetime = initialtime001 + datetime.timedelta(
                hours=6 * (u - 48) + 144)
            pdate = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
            odatetime = initialtime001 + datetime.timedelta(
                hours=6 * (u + 1 - 48) + 144)
            odate = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
            print initial, pdate, odate, 6 * (u + 1 - 48) + 144
            calculate_temp3h_accurity(db, initial, pdate, odate,
                                      6 * (u + 1 - 48) + 144)
    db.close()

if __name__ == "__main__":
    # 加日志
    logger = logging.getLogger('learing.logger')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    logfile = '/home/wlan_dev/log/gpu_winter.log'
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
    '''
    # 设定定时任务，EC数据每天4次，分布为00、06、12、18时
    # 计划设定在15点、21点、第二天的3点、9点执行，
    暂时线上只算12时的，其中5-9月采用夏季模型，10月到4月采用冬季模型，每天的凌晨3点计算前一天的12时数据。
    定时任务用aps
    '''
    start='2019-01-03 12:00:00'
    starttime = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    logger.info(starttime)
    #path='/home/wlan_dev/mosdata'
    path='/home/wlan_dev/mosdata/'
    for i in range(20):
        starttime001=starttime+datetime.timedelta(days=i)
        readymodel(starttime001,path)


