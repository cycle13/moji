#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/30
description:  气温训练模型增加DEM数据和位势高度因子来增强模型训练
"""
import xgboost,Nio,datetime,logging,sys,os,multiprocessing,numpy,math,time
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
def getLiveTempdict(stationfile,stationdict):
        fileR=open(stationfile,'r')
        while True:
            line=fileR.readline()
            linearray=line.split(',')
            if len(linearray)>4:
                sdictId=linearray[0]+linearray[1]
                pdatetime=datetime.datetime.strptime(linearray[1],'%Y-%m-%d %H:%M:%S')
                timestring=datetime.datetime.strftime(pdatetime,'%Y%m%d%H%M%S')
                sdictId = linearray[0] + '_'+timestring
                #气温的值不可能超过999（开尔文温度，或者热力学温度）
                if float(linearray[5])<999 and float(linearray[5])<>None:
                    stationdict[sdictId]=float(linearray[5])
            if not line:
                break
'''
ecfile_sfc：EC地面数据
ecfile_pl：EC高空数据
'''
def getTrainfeatureFromECfile(ecfile_sfc,ecfile_pl,sfc_varinames,pl_varinames,ff,trainlist):
    logger.info('=====')
    sfc_file=Nio.open_file(ecfile_sfc,'r')
    pl_file=Nio.open_file(ecfile_pl,'r')
    #遍历站点列表：
    # vvfile=os.path.join(outpath,'vv.csv')
    # vvcsv=open(vvfile,'w')
    print 'stationlist长度',len(stationlist)
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
        # #从实况字典中获取该站点，该时间的实况气温
        trainlabel=stationdict.get(kid)
        print origindatetime,ff,kid,trainlabel
        vstring=[]
        # 根据预报时效来计算数组的索引
        if ff <= 144:
            j = ff / 3
        else:
            j = (ff - 144) / 6 + 48
        print ff,j
        levelArray=pl_file.variables['lv_ISBL1']
        # 判断该实况数据是否是有效值（不为99999或者None）,若有效再计算16个格点值，将其一起添加到训练样本
        latitude=float(stationlist[i][1])
        longitude=float(stationlist[i][2])
        alti=float(stationlist[i][3])
        #取左上角点的索引，而不是最邻近点
        indexlat=int((60-latitude)/0.1)
        indexlon=int((longitude-60)/0.1)
        for m in range(len(sfc_varinames)):
            variArray=sfc_file.variables[sfc_varinames[m]]
            print len(variArray)
            latlonArray=variArray[j]
            print 'sfc',len(latlonArray)
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
            print 'pl',len(pl_variArray)
            phaArray=pl_variArray[j]
            print 'pha',len(phaArray)
            for k in range(len(phaArray)):
                llArray=phaArray[k]
                pha=levelArray[k]
                print '=====',k,len(llArray),pha
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
        # #最后一列加载气温值,这里应该是trainlabel,用0替代
        vstring.append(trainlabel)
        print 'vstring的长度',len(vstring)
        #vstring为一个站点的因子列表，添加到训练样本中
        # for a1 in range(len(vstring)):
        #     vvcsv.write(str(vstring[a1])+',')
        # vvcsv.write('\n')
        if vstring<>[]:
            trainlist.append(vstring)
    print len(trainlist)
    # vvcsv.close()
    sfc_file.close()
    pl_file.close()
def logprint(ecfile_sfc,ecfile_pl):
    logger.info(ecfile_sfc+'&&&&&&&&&'+ecfile_pl)
if __name__=='__main__':
    time001=time.time()
    ff=150
    fstr=str(ff)
    if 3-len(fstr)==2:
        fstr='00'+fstr
    elif 3-len(fstr)==1:
        fstr='0'+fstr
    #加日志
    logfile='/Users/yetao.lu/Desktop/mos/learning'+fstr+'.log'
    #logfile='/home/wlan_dev/log/learning_test'+fstr+'.log'
    logger = logging.getLogger(logfile)
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    file_handler=logging.handlers.RotatingFileHandler(logfile, maxBytes=1024 * 1024,backupCount=5)
    #file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值
    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.DEBUG)
    
    starttime=datetime.datetime.now()
    logger.info(starttime)
    #outpath='/home/wlan_dev/'
    outpath='/Users/yetao.lu/tmp'
    #EC数据周边16个点在DEM数据中的高程
    demdict={}
    #demcsv='/mnt/data/mosfile/dem0.1.csv'
    demcsv='/Users/yetao.lu/Desktop/mos/dem0.1.csv'
    csvread=open(demcsv,'r')
    while True:
        line=csvread.readline()
        linearray=line.split(',')
        if len(linearray)>2:
            demdict[linearray[0]]=linearray
        if not line:
            break
    #站点列表
    #csvfile='/mnt/data/mosfile/stations.csv'
    csvfile='/Users/yetao.lu/Desktop/mos/stations.csv'
    stationlist=getStationList(csvfile)
    #先把实况数据写进字典里,改了多进程
    #tempcsv='/mnt/data/tempcsv_winter'
    tempcsv='/Users/yetao.lu/Desktop/mos/data/tempcsv_winter'
    manager=multiprocessing.Manager()
    #多进程共享字典存储实况数据
    stationdict=manager.dict()
    pool=multiprocessing.Pool(processes=28)
    for srootpath, sdirs, stationfiles in os.walk(tempcsv):
        for stationfilename in stationfiles:
            stationfile=os.path.join(srootpath,stationfilename)
            pool.apply_async(getLiveTempdict,args=(stationfile,stationdict))
    pool.close()
    pool.join()
    logger.info(len(stationdict))
    #读EC数据，遍历EC数据把需要的要素都遍历出来，然后把气温添加到最后，因为多进程会打乱数据源，所以不能存到2个矩阵里面了。
    #多进程共享列表，提升效率
    #ecpath='/home/wlan_dev/2015/10'
    ecpath='/Users/yetao.lu/Desktop/mos/temp'
    sfc_varinames = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC','10V_GDS0_SFC','TCC_GDS0_SFC', 'LCC_GDS0_SFC','Z_GDS0_SFC']
    pl_varinames = ['R_GDS0_ISBL']
    trainlist=manager.list()
    ecpool=multiprocessing.Pool(processes=28)
    for ecroot,dirs,files in os.walk(ecpath):
        for file in files:
            logger.info(file+'---------------')
            if file[:3] == 'sfc' and file[-5:] == '.grib' and file[8:10]<>'06' and file[8:10]<>'07' and file[8:10]<>'08':
                ecfile_sfc=os.path.join(ecroot,file)
                ecfile_pl=ecfile_sfc.replace('sfc','pl')
                if not os.path.exists(ecfile_pl):
                    continue
                else:
                    ecpool.apply_async(getTrainfeatureFromECfile,args=(ecfile_sfc,ecfile_pl,sfc_varinames,pl_varinames,ff,trainlist))
    ecpool.close()
    ecpool.join()
    time002=time.time()
    logger.info(len(trainlist))
    trainlistarray=numpy.array(trainlist)
    for ii in range(len(trainlist)):
        logger.info(trainlist[ii])
    #分割出最后一列气温
    trainarray=trainlistarray[:,:-1]
    print trainarray
    testarray=trainlistarray[:,-1]
    logger.info(trainarray.shape)
    logger.info(testarray.shape)
    #logger.info(numpy.isnan(trainarray).any())
    logger.info("开始训练了。。。。。。。。")
    #对样本因子集进行分割
    a_train,a_test=train_test_split(trainarray,test_size=0.33,random_state=7)
    #模型训练前数据标准化
    scaler=preprocessing.StandardScaler().fit(trainarray)
    #保存标准化参数
    x_scaled=scaler.transform(trainarray)
    scaler_file=os.path.join(outpath,'temp_scale'+fstr+'.save')
    joblib.dump(scaler,scaler_file)
    #xgboost模型训练
    x_train,x_test,y_train,y_test=train_test_split(x_scaled,testarray,test_size=0.33,random_state=7)
    xgbtrain=xgboost.DMatrix(x_train,label=y_train)
    xgbtest=xgboost.DMatrix(x_test,label=y_test)
    #保存训练数据集
    train_file=os.path.join(outpath,'temp_train'+fstr+'.buffer')
    xgbtrain.save_binary(train_file)
    #训练和验证的错误率
    watchlist=[(xgbtrain,'xgbtrain'),(xgbtest,'xgbeval')]
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
    plist=list(params.items())
    num_rounds=50000
    # early_stopping_rounds当设置的迭代次数较大时,early_stopping_rounds 可在一定的迭代次数内准确率没有提升就停止训练
    model=xgboost.train(plist,xgbtrain,num_rounds,watchlist,early_stopping_rounds=800)
    #模型预测
    preds=model.predict(xgbtest,ntree_limit=model.best_iteration)
    y_test=y_test.astype('float32')
    mse = mean_squared_error(y_test, preds)
    rmse = math.sqrt(mse)
    mae = mean_absolute_error(y_test, preds, multioutput='uniform_average')
    print("训练后MSE: %.4f" % mse)
    print("训练后RMSE: %.4f" % rmse)
    print("训练后MAE: %.4f" % mae)
    #保存模型
    outmodel = os.path.join(outpath, 'temp_model' + fstr + '.model')
    model.save_model(outmodel)
    time003=time.time()
    # 气温差2度的准确率
    n = 0
    for x, y in zip(y_test, preds):
        if abs(x - y) < 2:
            n = n + 1
    accuracy2_after = float(n) / float(len(y_test))
    print ("训练后2度的accuracy: %.4f" % accuracy2_after)
    n = 0
    for x, y in zip(y_test, preds):
        if abs(x - y) < 3:
            n = n + 1
    accuracy3_after = float(n) / float(len(y_test))
    print ("训练后3度的accuracy: %.4f" % accuracy3_after)
    # 和EC中原始数据对比获取均方误差
    y_origin = a_test[:, 0]
    # print y_origin
    y_origin = y_origin - 273.15
    # print y_origin
    mse0 = mean_squared_error(y_test, y_origin)
    rmse0 = math.sqrt(mse0)
    mae0 = mean_absolute_error(y_test, y_origin, multioutput='uniform_average')
    print("训练前MSE: %.4f" % mse0)
    print("训练前RMSE: %.4f" % rmse0)
    print("训练前MAE: %.4f" % mae0)
    n = 0
    for x, y in zip(y_test, y_origin):
        if abs(x - y) < 2:
            n = n + 1
    accuracy2_before = float(n) / float(len(y_test))
    print ("训练前2度的accuracy: %.4f" % accuracy2_before)
    n = 0
    for x, y in zip(y_test, y_origin):
        if abs(x - y) < 3:
            n = n + 1
    accuracy3_before = float(n) / float(len(y_test))
    print ("训练前3度的accuracy: %.4f" % accuracy3_before)
    print str(rmse0) + ',' + str(mae0) + ',' + str(
        accuracy2_before) + ',' + str(accuracy3_before) + ',' + str(
        rmse) + ',' + str(mae) + ',' + str(accuracy2_after) + ',' + str(
        accuracy3_after) + ',' + str(len(a_train)) + ',' + str(len(a_test))
    #数据集保存
    testfile=os.path.join(outpath,'temp_test'+fstr+'.csv')
    predsfile=os.path.join(outpath,'temp_preds'+fstr+'.csv')
    originfile=os.path.join(outpath,'temp_origin'+fstr+'.csv')
    testfw=open(testfile,'w')
    predsfw=open(predsfile,'w')
    originfw=open(originfile,'w')
    for u in y_test:
        testfw.write(str(u)+',')
    testfw.close()
    for o in preds:
        predsfw.write(str(o)+',')
    predsfw.close()
    for q in y_origin:
        originfw.write(str(q)+',')
    originfw.close()
    time004=time.time()
    logger.info('数据采集时间(分钟):'+str((time002-time001)/60)+'模型训练(分钟)：'+str((time003-time002)/60)+'准确率计算数据保存(分钟)：'+str((time004-time003)/60))