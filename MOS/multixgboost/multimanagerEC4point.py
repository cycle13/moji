#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/8
description:
"""
import Nio, datetime, os,string,logging,xgboost,numpy,math,multiprocessing,sys
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn import preprocessing


def GetOnetimeFromEC(n, sfc_varinames, sfc_file, indexlat, indexlon,
                     pl_varinames, pl_file):
    vstring = []
    levelArray = pl_file.variables['lv_ISBL1']
    for i in range(len(sfc_varinames)):
        variArray = sfc_file.variables[sfc_varinames[i]]
        latlonArray = variArray[n]
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
    for p in range(len(pl_varinames)):
        pl_variArray = pl_file.variables[pl_varinames[p]]
        phaArray = pl_variArray[n]
        for k in range(len(phaArray)):
            llArray = phaArray[k]
            pha = levelArray[k]
            # print pha
            if pha == 500 or pha == 850:
                # vstring.append(str(pha)+'hpa')
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
def GetStationsAndOnetimesFromEC(i, sfc_varinames, sfc_file, pl_varinames,
                                 pl_file, inputfile,stationsVlist,trainlebellist,stationdict,stationlist,dict01,dictlist):
    print 'stationlist:',len(stationlist)
    for j in range(len(stationlist)):
        # 根据文件名来建立索引获取实况数据气温的值
        strarray = inputfile.split('_')
        odatetime = datetime.datetime.strptime(
            (strarray[1] + strarray[2][:2]),
            '%Y%m%d%H')
        if i<=48:
            fdatetime = odatetime + datetime.timedelta(hours=i*3)
        else:
            fdatetime=odatetime+datetime.timedelta(hours=48*3+(i-48)*6)
        fdateStr = datetime.datetime.strftime(fdatetime, '%Y%m%d%H%M%S')
        # 这里只能是起报时间加预报时效，不能用预报时间。因预报时间有重复
        dictid = stationlist[j][0] + '_' + strarray[1] + strarray[2][
        :2] + '_' + str(i)
        # 根据站号+实况数据的索引来获取实况数据
        kid = stationlist[j][0] + '_' + fdateStr
        trainlebel = stationdict.get(kid)
        # 判断该实况数据是否是有效值（不为99999或者None）,若有效再计算16个格点值，将其一起添加到训练样本
        if trainlebel <> None and trainlebel <> 999999:
            latitude = float(stationlist[j][4])
            longitude = float(stationlist[j][5])
            # #    #首先计算经纬度对应格点的索引，
            indexlat = int((60+0.125/2 - latitude) / 0.125)
            indexlon = int((longitude+0.125/2 - 60) / 0.125)
            # print latitude,longitude,indexlat,indexlon
            # 则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
            perstationlist = GetOnetimeFromEC(i, sfc_varinames, sfc_file,
                                              indexlat, indexlon,
                                              pl_varinames, pl_file)
            #print j,dictid,kid,trainlebel
            dict01[dictid] = perstationlist
            dictlist.append(dictid)
            stationsVlist.append(perstationlist)
            trainlebellist.append(trainlebel)
    # print 'stationsVlist',len(stationsVlist),'trainlebellist',len(trainlebellist),'stationdict',len(stationdict)
#EC格点数据的获取
def modelprocess(stationdict,stationlist,ll):
    sys.stdout=open(os.path.join(outpath,'t_t'+str(ll)+'.out'),'w')
    allpath = '/moji/meteo/cluster/data/MOS/'
    sfc_varinames = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC','10V_GDS0_SFC','TCC_GDS0_SFC', 'LCC_GDS0_SFC']
    pl_varinames = ['R_GDS0_ISBL']
    # 遍历文件
    dict01={}
    stationsVlist = []
    trainlebellist=[]
    dictlist=[]
    #遍历所有的文件，
    for rootpath, dirs, files in os.walk(allpath):
        print allpath
        for file in files:
            if file[:3] == 'sfc' and file[-5:] == '.grib' and (string.find(file,'2014')==-1):
                inputfile = os.path.join(rootpath, file)
                inputfile2=inputfile.replace('sfc','pl')
                sfcfile=Nio.open_file(inputfile,'r')
                plfile=Nio.open_file(inputfile2,'r')
                #参数0是指第0个时次的预报,这里只是一个文件的2000个站的列表。
                #print sfcfile,plfile
                GetStationsAndOnetimesFromEC(ll,sfc_varinames,sfcfile,pl_varinames,plfile,inputfile,stationsVlist,trainlebellist,stationdict,stationlist,dict01,dictlist)
    stationArray=numpy.array(stationsVlist)
    trainlebelArray=numpy.array(trainlebellist)
    dictArray=numpy.array(dictlist)
    a_train,a_test=train_test_split(stationArray,test_size=0.33,random_state=7)
    #print len(a_train),len(a_test),len(a_train)+len(a_test)
    #数据训练前进行标准化
    x_scaled=preprocessing.scale(stationArray)
    stationArray=x_scaled
    #xgboost
    x_train,x_test,y_train,y_test,z_train,z_test=train_test_split(stationArray,trainlebelArray,dictArray,test_size=0.33,random_state=7)
    xgbtrain=xgboost.DMatrix(x_train,label=y_train)
    xgbtest=xgboost.DMatrix(x_test,label=y_test)
    #xgbtrain.save_binary('train.buffer')
    #print len(x_train),len(x_test),len(y_train),len(y_test)
    #print xgbtest
    #训练和验证的错误率
    watchlist=[(xgbtrain,'xgbtrain'),(xgbtest,'xgbeval')]
    params={
    'booster':'gbtree',
    'objective': 'reg:linear', #线性回归
    'gamma':0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
    'max_depth':12, # 构建树的深度，越大越容易过拟合
    'lambda':2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
    'subsample':0.7, # 随机采样训练样本
    'colsample_bytree':0.7, # 生成树时进行的列采样
    'min_child_weight':3,
    # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
    #，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
    #这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
    'silent':0 ,#设置成1则没有运行信息输出，最好是设置为0.
    'eta': 0.01, # 如同学习率
    'seed':1000,
    'nthread':3,# cpu 线程数
    #'eval_metric': 'auc'
    'scale_pos_weight':1
    }
    plst=list(params.items())
    num_rounds=50000
    #early_stopping_rounds当设置的迭代次数较大时,early_stopping_rounds 可在一定的迭代次数内准确率没有提升就停止训练
    model=xgboost.train(plst,xgbtrain,num_rounds,watchlist,early_stopping_rounds=800)
    #print model,watchlist
    preds=model.predict(xgbtest,ntree_limit=model.best_iteration)
    # 将预测结果写入文件，方式有很多，自己顺手能实现即可
    # numpy.savetxt('submission_xgb_MultiSoftmax.csv',numpy.c_[range(1,len(test)+1),preds],
    #                 delimiter=',',header='ImageId,Label',comments='',fmt='%d')
    #print preds
    #print y_test.dtype,preds.dtype
    y_test=y_test.astype('float32')
    mse=mean_squared_error(y_test,preds)
    rmse=math.sqrt(mse)
    mae=mean_absolute_error(y_test,preds,multioutput='uniform_average')
    print("训练后MSE: %.4f" % mse)
    print("训练后RMSE: %.4f" % rmse)
    print("训练后MAE: %.4f" % mae)
    #气温差2度的准确率
    n=0
    for x,y in zip(y_test,preds):
        if abs(x-y)<2:
            n=n+1
    accuracy2_after=float(n)/float(len(y_test))
    print ("训练后2度的accuracy: %.4f" % accuracy2_after)
    n=0
    for x,y in zip(y_test,preds):
        if abs(x-y)<3:
            n=n+1
    accuracy3_after=float(n)/float(len(y_test))
    print ("训练后3度的accuracy: %.4f" % accuracy3_after)
    #和EC中原始数据对比获取均方误差
    y_origin=a_test[:,0]
    #print y_origin
    y_origin=y_origin-273.15
    #print y_origin
    mse0=mean_squared_error(y_test,y_origin)
    rmse0=math.sqrt(mse0)
    mae0=mean_absolute_error(y_test,y_origin,multioutput='uniform_average')
    print("训练前MSE: %.4f" % mse0)
    print("训练前RMSE: %.4f" % rmse0)
    print("训练前MAE: %.4f" % mae0)
    n=0
    for x,y in zip(y_test,y_origin):
        if abs(x-y)<2:
            n=n+1
    accuracy2_before=float(n)/float(len(y_test))
    print ("训练前2度的accuracy: %.4f" % accuracy2_before)
    n=0
    for x,y in zip(y_test,y_origin):
        if abs(x-y)<3:
            n=n+1
    accuracy3_before=float(n)/float(len(y_test))
    print ("训练前3度的accuracy: %.4f" % accuracy3_before)
    print str(rmse0)+','+str(mae0)+','+str(accuracy2_before)+','+str(accuracy3_before)+','+str(rmse)+','+str(mae)+','+str(accuracy2_after)+','+str(accuracy3_after)+','+str(len(a_train))+','+str(len(a_test))
    model.save_model(os.path.join(outpath,'temperature'+str(ll)+'.model'))
    predsfile=os.path.join(outpath,'t4_preds'+str(ll)+'.csv')
    predsfw=open(predsfile,'w')
    for jj in range(len(y_test)):
        predsfw.write(str(z_test[jj])+','+str(y_origin[jj])+','+str(y_test[jj])+','+str(preds[jj]))
        predsfw.write('/n')
    predsfw.close()
    del stationArray
    del trainlebelArray
    del a_test
    del a_train
    del x_test
    del x_train
    del y_origin
    del y_test
    del y_train
    del xgbtest
    del xgbtrain
    del x_scaled
    del plfile
    del sfcfile

if __name__=='__main__':
    starttime=datetime.datetime.now()
    #站点气温实况数据
    tempcsv='/home/wlan_dev/tempCSV'
    outpath='/home/wlan_dev/model'
    manager=multiprocessing.Manager()
    stationdict=manager.dict()
    for srootpath,sdirs,stationfile in os.walk(tempcsv):
        for i in range(len(stationfile)):
            if stationfile[i][-4:]=='.csv':
                stationfilepath=os.path.join(srootpath,stationfile[i])
                fileR=open(stationfilepath,'r')
                while True:
                    line=fileR.readline()
                    linearray=line.split(',')
                    if len(linearray)>4:
                        sdictId=linearray[0]+linearray[1]
                        pdatetime=datetime.datetime.strptime(linearray[1],'%Y-%m-%d %H:%M:%S')
                        timestring=datetime.datetime.strftime(pdatetime,'%Y%m%d%H%M%S')
                        sdictId = linearray[0] + '_'+timestring
                        #气温的值不可能超过999（开尔文温度，或者热力学温度）
                        if float(linearray[5])<999 or float(linearray[5])<>None:
                            stationdict[sdictId]=float(linearray[5])
                    if not line:
                        break
    #print 'dict',len(stationdict)
    # 站点列表经纬度数据
    stationlist = manager.list()
    csvfile = '/home/wlan_dev/t_p_station_cod.csv'
    fileread = open(csvfile, 'r')
    firstline = fileread.readline()
    while True:
        line = fileread.readline()
        perlist = line.split(',')
        if len(perlist) > 4:
            stationlist.append(perlist)
        if not line or line == '':
            break
    #print 'list',len(stationlist)
    #传个CSV文件进去，大家同时写，涉及到多进程写同一个文件
    pool=multiprocessing.Pool()
    results=[]
    for ii in range(0,65):
        result=pool.apply_async(modelprocess,args=(stationdict,stationlist,ii))
        results.append(result)
    pool.close()
    pool.join()
    endtime=datetime.datetime.now()
    print(endtime-starttime).seconds