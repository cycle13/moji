#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/30
description:  对最高气温、最低气温进行训练，该程序对降水和其他多个因子进行训练

"""
import Nio, datetime, os, xgboost, numpy, math
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import accuracy_score
from sklearn import preprocessing
from sklearn.feature_selection import f_regression, mutual_info_regression
from sklearn.externals import joblib

def pre3hTodict(tempcsv):
    stationdict = {}
    for srootpath, sdirs, stationfile in os.walk(tempcsv):
        for i in range(len(stationfile)):
            if stationfile[i][-4:] == '.csv':
                stationfilepath = os.path.join(srootpath, stationfile[i])
                fileR = open(stationfilepath, 'r')
                while True:
                    line = fileR.readline()
                    linearray = line.split(',')
                    if len(linearray) > 4:
                        sdictId = linearray[0] + linearray[1]
                        pdatetime = datetime.datetime.strptime(linearray[1],
                                                               '%Y-%m-%d %H:%M:%S')
                        timestring = datetime.datetime.strftime(pdatetime,
                                                                '%Y%m%d%H%M%S')
                        sdictId = linearray[0] + '_' + timestring
                        if float(linearray[5]) <> 999999 or float(
                                linearray[5]) <> None or float(
                                linearray[5]) <> 999998:
                            stationdict[sdictId] = float(linearray[5])
                    if not line:
                        break
    return stationdict
def pre6hTodict(precsv):
    station6hdict = {}
    for prootpath, pdirs, pstationfile in os.walk(precsv):
        for ii in range(len(pstationfile)):
            if pstationfile[ii][-4:] == '.csv':
                pstationfilepath = os.path.join(prootpath, pstationfile[ii])
                pfileread = open(pstationfilepath, 'r')
                while True:
                    pline = pfileread.readline()
                    plinearray = pline.split(',')
                if len(plinearray) > 4:
                    pdictid = plinearray[0] + plinearray[1]
                    ppdatetime = datetime.datetime.strptime(plinearray[1],
                                                            '%Y-%m-%d %H:%M:%S')
                    ptimestring = datetime.datetime.strftime(ppdatetime,
                                                             '%Y%m%d%H%M%S')
                    pdictid = plinearray[0] + '_' + ptimestring
                    if float(plinearray[5]) <> 999999 or float(
                            plinearray[5]) <> None or float(
                            plinearray[5]) <> 999998:
                        station6hdict[pdictid] = float(plinearray[5])
                if not pline:
                    break
    return station6hdict
def temp3hmaxminTodict(tempcsv):
    stationdict = {}
    for srootpath, sdirs, stationfile in os.walk(tempcsv):
        for i in range(len(stationfile)):
            if stationfile[i][-4:] == '.csv':
                stationfilepath = os.path.join(srootpath, stationfile[i])
                fileR = open(stationfilepath, 'r')
                while True:
                    line = fileR.readline()
                    linearray = line.split(',')
                    if len(linearray) > 4:
                        sdictId = linearray[0] + linearray[1]
                        pdatetime = datetime.datetime.strptime(linearray[1],
                                                               '%Y-%m-%d %H:%M:%S')
                        timestring = datetime.datetime.strftime(pdatetime,
                                                                '%Y%m%d%H%M%S')
                        sdictId = linearray[0] + '_' + timestring
                        stationdict[sdictId] = linearray
                        #print sdictId,linearray
                    if not line:
                        break
    #print stationdict
    return stationdict
def temp6hmaxminTodict(tempcsv):
    stationdict = {}
    for srootpath, sdirs, stationfile in os.walk(tempcsv):
        for i in range(len(stationfile)):
            if stationfile[i][-4:] == '.csv':
                stationfilepath = os.path.join(srootpath, stationfile[i])
                fileR = open(stationfilepath, 'r')
                while True:
                    line = fileR.readline()
                    linearray = line.split(',')
                    if len(linearray) > 4:
                        sdictId = linearray[0] + linearray[1]
                        pdatetime = datetime.datetime.strptime(linearray[1],
                                                               '%Y-%m-%d %H:%M:%S')
                        timestring = datetime.datetime.strftime(pdatetime,
                                                                '%Y%m%d%H%M%S')
                        sdictId = linearray[0] + '_' + timestring
                        stationdict[sdictId] = linearray
                    if not line:
                        break
    #print stationdict
    return stationdict
def GetOnetimeFromEC(n,max_varinames,h3file,sfc_varinames1, sfc_file,pl_varinames,pl_file, indexlat, indexlon,trainlabellist):
    vstring = []
    levelArray = pl_file.variables['lv_ISBL1']
    #cp,tp两个变量比较特殊，是累计，需要减去前一个时次的
    #print indexlat,indexlon
    for i in range(len(max_varinames)):
        variArray = h3file.variables[max_varinames[i]]
        latlonArray = variArray[n]
        vstring.append((latlonArray[indexlat][indexlon]))
        vstring.append((latlonArray[indexlat][indexlon + 1]))
        vstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        vstring.append((latlonArray[indexlat + 1][indexlon]))
        vstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        vstring.append((latlonArray[indexlat - 1][indexlon]))
        vstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        vstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        vstring.append((latlonArray[indexlat][indexlon + 2]))
        vstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        vstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        vstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        vstring.append((latlonArray[indexlat + 2][indexlon]))
        vstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        vstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        vstring.append((latlonArray[indexlat][indexlon - 1]))
    #把经度纬度和
    vstring.append(trainlabellist[2])
    vstring.append(trainlabellist[3])
    vstring.append(trainlabellist[4])
    #影响因子
    for ii in range(len(sfc_varinames1)):
        variArray = sfc_file.variables[sfc_varinames1[ii]]
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
    for j in range(len(pl_varinames)):
        pl_variArray=pl_file.variables[pl_varinames[j]]
        phaArray = pl_variArray[n]
        for k in range(len(phaArray)):
            llArray = phaArray[k]
            pha = levelArray[k]
            # print pha
            if pha == 500 or pha == 850:
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
def GetStationsAndOnetimesFromEC(i,max_varinames,h3file,sfc_varinames1, sfc_file,pl_varinames,pl_file,inputfile,
                                ecvaluelist, stationdict,
                                 stationlist, dict01, maxtemplist,
                                 mintemplist):
    print 'stationlist:',len(stationlist)
    for j in range(len(stationlist)):
        # 根据文件名来建立索引获取实况数据气温的值
        strarray = inputfile.split('_')
        odatetime = datetime.datetime.strptime((strarray[1] + strarray[2][:2]),
                                               '%Y%m%d%H')
        if i <= 48:
            fdatetime = odatetime + datetime.timedelta(hours=i * 3)
        else:
            fdatetime = odatetime + datetime.timedelta(
                hours=48 * 3 + (i - 48) * 6)
        fdateStr = datetime.datetime.strftime(fdatetime, '%Y%m%d%H%M%S')
        # 这里只能是起报时间加预报时效，不能用预报时间。因预报时间有重复
        dictid = stationlist[j][0] + '_' + strarray[1] + strarray[2][
        :2] + '_' + str(i)
        #print dictid
        # 根据站号+实况数据的索引来获取实况数据
        kid = stationlist[j][0] + '_' + fdateStr
        #print 'kid',kid
        trainlebellist = stationdict.get(kid)
        #print trainlebellist
        if trainlebellist<>None:
            maxvalue=float(trainlebellist[5])
            minvalue=float(trainlebellist[6])
            # 判断该实况数据是否是有效值（不为99999或者None）,若有效再计算16个格点值，将其一起添加到训练样本
            if maxvalue < 9999 and minvalue<9999:
                latitude = float(stationlist[j][1])
                longitude = float(stationlist[j][2])
                #print maxvalue,minvalue,latitude,longitude
                # #首先计算经纬度对应格点的索引，
                indexlat = int((60 - latitude) / 0.125)
                indexlon = int((longitude - 60) / 0.125)
                #print latitude,longitude,indexlat,indexlon
                # 则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
                perstationlist = GetOnetimeFromEC(i,max_varinames,h3file,sfc_varinames1, sfc_file,pl_varinames,pl_file,
                                                  indexlat, indexlon,trainlebellist)
                #print dictid,perstationlist,kid
                dict01[dictid] = perstationlist
                ecvaluelist.append(perstationlist)
                maxtemplist.append(maxvalue)
                mintemplist.append(minvalue)
    print 'ecvaluelist',len(ecvaluelist),'maxtemplist',len(maxtemplist),'mintemplist',len(mintemplist)
# EC格点数据的获取
def modelprocess(stationdict, stationlist, ll,allpath):
    sfc_varinames1 = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC','10V_GDS0_SFC', 'TCC_GDS0_SFC', 'LCC_GDS0_SFC']
    pl_varinames = ['R_GDS0_ISBL']
    max_varinames=['MX2T3_GDS0_SFC_1','MN2T3_GDS0_SFC_1']
    dict01 = {}
    # 遍历文件
    ecvaluelist = []
    maxtemplist=[]
    mintemplist=[]
    # 遍历所有的文件，
    for rootpath, dirs, files in os.walk(allpath):
        for file in files:
            if file[:3] == 'sfc' and file[-5:] == '.grib':
                inputfile = os.path.join(rootpath, file)
                inputfile2 = inputfile.replace('sfc', 'pl')
                inputfile3=inputfile.replace('sfc','3h')
                sfcfile = Nio.open_file(inputfile, 'r')
                plfile = Nio.open_file(inputfile2, 'r')
                h3file=Nio.open_file(inputfile3,'r')
                # 参数0是指第0个时次的预报,这里只是一个文件的2000个站的列表。
                print ll, sfc_varinames1, sfcfile,inputfile, stationdict,stationlist, dict01
                GetStationsAndOnetimesFromEC(ll,max_varinames,h3file,sfc_varinames1, sfcfile,pl_varinames,plfile,inputfile,
                                             ecvaluelist, stationdict,
                                                 stationlist, dict01,
                                                    maxtemplist,
                                             mintemplist)
    ecvaluelist=numpy.array(ecvaluelist)
    maxtemplist=numpy.array(maxtemplist)
    #为了统计准确率，提前分割数据集
    mintemplist=numpy.array(mintemplist)
    class_train,class_test=train_test_split(ecvaluelist,test_size=0.33,random_state=7)
    max_train, max_test,min_train,min_test = train_test_split(maxtemplist,mintemplist, test_size=0.33,random_state=7)
    #数据训练前进行标准化
    ecvaluelist=ecvaluelist.astype('float32')
    maxtemplist=maxtemplist.astype('float32')
    mintemplist=mintemplist.astype('float32')
    print ecvaluelist.shape,maxtemplist.shape
    #x_scaled = preprocessing.scale(ecvaluelist)
    scaler=preprocessing.StandardScaler.fit(ecvaluelist)
    x_scaled=scaler.transform(ecvaluelist)
    ecvaluelist = x_scaled
    scaler_file=os.path.join(outpath,'max_scaler' + str(ll) + '.model')
    joblib.dump(scaler,scaler_file)
    # xgboost，训练集和预测集分割
    x_train, x_test, y_train, y_test = train_test_split(ecvaluelist, maxtemplist,test_size=0.33, random_state=7)
    u_train, u_test, v_train, v_test=train_test_split(ecvaluelist, mintemplist,test_size=0.33, random_state=7)
    print len(x_train),len( x_test),len(y_train),len( y_test),len( u_train),len( u_test), len(v_train),len( v_test)
    xgbtrain = xgboost.DMatrix(x_train, label=y_train)
    xgbtest = xgboost.DMatrix(x_test, label=y_test)
    xgbtrain01 = xgboost.DMatrix(u_train, label=v_train)
    xgbtest01 = xgboost.DMatrix(u_test, label=v_test)
    # xgbtrain.save_binary('train.buffer')
    # print len(x_train),len(x_test),len(y_train),len(y_test)
    # print xgbtest
    # 特征选址
    print x_train.shape,x_test.shape
    ff,pp=f_regression(x_train,y_train)
    print ff,pp
    # 训练和验证的错误率
    watchlist = [(xgbtrain, 'xgbtrain'), (xgbtest, 'xgbeval')]
    watchlist01 = [(xgbtrain01, 'xgbtrain01'), (xgbtest01, 'xgbeval01')]
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
        #'nthread': 3,  # cpu 线程数
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
        #'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
    plst = list(params.items())
    plst01=list(params01.items())
    num_rounds = 99999
    # early_stopping_rounds当设置的迭代次数较大时,early_stopping_rounds 可在一定的迭代次数内准确率没有提升就停止训练
    model = xgboost.train(plst, xgbtrain, num_rounds, watchlist,early_stopping_rounds=500)
    model01 = xgboost.train(plst01, xgbtrain01, num_rounds, watchlist01,early_stopping_rounds=500)
    # print model,watchlist
    preds = model.predict(xgbtest, ntree_limit=model.best_iteration)
    preds01 = model01.predict(xgbtest01, ntree_limit=model.best_iteration)
    model.save_model(os.path.join(outpath,'max_temp' + str(ll) + '.model'))
    model01.save_model(os.path.join(outpath,'min_temp' + str(ll) + '.model'))
    #print preds, preds01
    #训练后高温的RMSE，MAE
    mse=mean_squared_error(y_test,preds)
    rmse=math.sqrt(mse)
    mae=mean_absolute_error(y_test,preds,multioutput='uniform_average')
    print("训练后最高气温MSE: %.4f" % mse)
    print("训练后最高气温RMSE: %.4f" % rmse)
    print("训练后最高气温MAE: %.4f" % mae)
    #训练后最高气温2度以内的准确率
    n = 0
    for x, y in zip(y_test, preds):
        if abs(x - y) < 2:
            n = n + 1
    accuracy2_after=float(n)/float(len(y_test))
    print ("训练后最高气温2度的accuracy: %.4f" % accuracy2_after)
    n = 0
    for x, y in zip(y_test, preds):
        if abs(x - y) < 3:
            n = n + 1
    accuracy3_after=float(n)/float(len(y_test))
    print ("训练后最高气温3度的accuracy: %.4f" % accuracy3_after)
    #训练前高温的RMSE,MAE
    class_test=class_test.astype('float64')
    max_ec=(class_test[:,0])-273.15
    mse0=mean_squared_error(max_ec,y_test)
    rmse0=math.sqrt(mse0)
    mae0=mean_absolute_error(max_ec,y_test,multioutput='uniform_average')
    print("训练前最高气温MSE: %.4f" % mse0)
    print("训练前最高气温RMSE: %.4f" % rmse0)
    print("训练前最高气温MAE: %.4f" % mae0)
    n = 0
    for x, y in zip(y_test, max_ec):
        if abs(x - y) < 2:
            n = n + 1
    accuracy2_before = float(n) / float(len(y_test))
    print ("训练前最高气温2度的accuracy: %.4f" % accuracy2_before)
    n = 0
    for x, y in zip(y_test, max_ec):
        if abs(x - y) < 3:
            n = n + 1
    accuracy3_before = float(n) / float(len(y_test))
    print ("训练前最高气温3度的accuracy: %.4f" % accuracy3_before)
    #训练后低温的RMSE|MAE
    min_mse=mean_squared_error(v_test,preds01)
    min_rmse=math.sqrt(min_mse)
    min_mae=mean_absolute_error(v_test,preds01,multioutput='uniform_average')
    print("训练后最低气温MSE: %.4f" % min_mse)
    print("训练后最低气温RMSE: %.4f" % min_rmse)
    print("训练后最低气温MAE: %.4f" % min_mae)
    #训练后最低气温2度以内的准确率
    n = 0
    for x, y in zip(v_test, preds01):
        if abs(x - y) < 2:
            n = n + 1
    min_accuracy2_after=float(n)/float(len(v_test))
    print ("训练后最低气温2度的accuracy: %.4f" % min_accuracy2_after)
    n = 0
    for x, y in zip(v_test, preds01):
        if abs(x - y) < 3:
            n = n + 1
    min_accuracy3_after=float(n)/float(len(v_test))
    print ("训练后最低气温3度的accuracy: %.4f" % min_accuracy3_after)
    #训练前低温的RMSE,MAE
    min_ec=class_test[:,1]-273.15
    min_mse0=mean_squared_error(min_ec,v_test)
    min_rmse0=math.sqrt(min_mse0)
    min_mae0=mean_absolute_error(min_ec,v_test,multioutput='uniform_average')
    print("训练前最高气温MSE: %.4f" % min_mse0)
    print("训练前最高气温RMSE: %.4f" % min_rmse0)
    print("训练前最高气温MAE: %.4f" % min_mae0)
    n = 0
    for x, y in zip(v_test, min_ec):
        if abs(x - y) < 2:
            n = n + 1
    min_accuracy2_before = float(n) / float(len(v_test))
    print ("训练前最高气温2度的accuracy: %.4f" % min_accuracy2_before)
    n = 0
    for x, y in zip(v_test, min_ec):
        if abs(x - y) < 3:
            n = n + 1
    min_accuracy3_before = float(n) / float(len(v_test))
    print ("训练前最高气温3度的accuracy: %.4f" % min_accuracy3_before)
    print  str(rmse)+','+str(mae)+','+str(accuracy2_after)+','+str(accuracy3_after)+','+str(rmse0)+','+str(mae0)+','+str(accuracy2_before)+','+str(accuracy3_before)+','+str(min_rmse)+','+str(min_mae)+','+str(min_accuracy2_after)+','+str(min_accuracy3_after)+','+str(min_rmse0)+','+str(min_mae0)+','+str(min_accuracy2_before)+','+str(min_accuracy3_before)
    maxfile = os.path.join(outpath, 'maxt' + str(ll) + '.csv')
    maxtfw = open(maxfile, 'w')
    for pp in range(len(y_test)):
        maxtfw.write(str(max_ec[pp]) +',' + str(preds[pp]) + ',' + str(y_test[pp]))
        maxtfw.write('\n')
    maxtfw.close()
    # 输出降水量的预报
    minfile = os.path.join(outpath, 'mint' + str(ll) + '.csv')
    mintfw = open(minfile, 'w')
    for qq in range(len(v_test)):
        mintfw.write(str(min_ec[qq]) + ',' + str(preds01[qq]) + ',' + str(v_test[qq]))
        mintfw.write('\n')
    mintfw.close()
if __name__ == '__main__':
    starttime = datetime.datetime.now()
    outpath = '/Users/yetao.lu/Desktop/mos/anonymous'
    #outpath = '/home/wlan_dev/model'
    ll=1
    stationdict = {}
    # 站点列表数据
    stationlist = []
    csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
    #csvfile = '/home/wlan_dev/t_p_station_cod.csv'
    fileread = open(csvfile, 'r')
    firstline = fileread.readline()
    while True:
        line = fileread.readline()
        perlist = line.split(',')
        if len(perlist) >= 4:
            stationlist.append(perlist)
        if not line or line == '':
            break
    #print stationlist
    if ll <= 48:
        # 处理站点3h最高气温、最低气温实况数据
        maxmin3hcsv = '/Users/yetao.lu/Desktop/mos/data/temmaxmin'
        #maxmin3hcsv = '/home/wlan_dev/precsv'
        stationdict = temp3hmaxminTodict(maxmin3hcsv)
    else:
        # 处理站点6小时最高气温、最低气温实况数据
        maxmin6hcsv='/Users/yetao.lu/Desktop/mos/data/temmaxmin6h'
        #maxmin6hcsv = '/home/wlan_dev/pre6h'
        stationdict = temp6hmaxminTodict(maxmin6hcsv)
    print len(stationdict)
    allpath = '/Users/yetao.lu/Documents/testdata/'
    #allpath = '/mnt/data/MOS/'
    modelprocess(stationdict,stationlist,ll,allpath)
    endtime = datetime.datetime.now()
    # print len(dict)
    print(endtime - starttime).seconds
