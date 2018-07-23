#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/30
description:  对降水先进行晴雨训练，然后再进行降水量进行训练，该程序对降水和其他多个因子进行训练

"""
import Nio, datetime, os, xgboost, numpy, math
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import accuracy_score
from sklearn import preprocessing


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
                print pstationfilepath
                pfileread = open(pstationfilepath, 'r')
                while True:
                    pline = pfileread.readline()
                    plinearray = pline.split(',')
                    #print plinearray
                    if len(plinearray) > 4:
                        pdictid = plinearray[0] + plinearray[1]
                        ppdatetime = datetime.datetime.strptime(plinearray[1],
                                                                '%Y-%m-%d %H:%M:%S')
                        ptimestring = datetime.datetime.strftime(ppdatetime,
                                                                 '%Y%m%d%H%M%S')
                        pdictid = plinearray[0] + '_' + ptimestring
                        #print pdictid,plinearray[5]
                        if float(plinearray[5]) <> 999999 or float(
                                plinearray[5]) <> None or float(
                                plinearray[5]) <> 999998:
                            station6hdict[pdictid] = float(plinearray[5])
                    if not pline:
                        break
    print len(station6hdict)
    return station6hdict
def GetOnetimeFromEC(n, sfc_varinames,sfc_varinames1, sfc_file,pl_varinames,pl_file, indexlat, indexlon):
    vstring = []
    levelArray = pl_file.variables['lv_ISBL1']
    #cp,tp两个变量比较特殊，是累计，需要减去前一个时次的
    for i in range(len(sfc_varinames)):
        variArray = sfc_file.variables[sfc_varinames[i]]
        latlonArray = variArray[n]
        platlonArray=variArray[n-1]
        vstring.append(1000*(latlonArray[indexlat][indexlon]-platlonArray[indexlat][indexlon]))
        vstring.append(1000*(latlonArray[indexlat][indexlon + 1]-platlonArray[indexlat][indexlon + 1]))
        vstring.append(1000*(latlonArray[indexlat + 1][indexlon + 1]-platlonArray[indexlat + 1][indexlon + 1]))
        vstring.append(1000*(latlonArray[indexlat + 1][indexlon]-platlonArray[indexlat + 1][indexlon]))
        vstring.append(1000*(latlonArray[indexlat - 1][indexlon - 1]-platlonArray[indexlat - 1][indexlon - 1]))
        vstring.append(1000*(latlonArray[indexlat - 1][indexlon]-platlonArray[indexlat - 1][indexlon]))
        vstring.append(1000*(latlonArray[indexlat - 1][indexlon + 1]-platlonArray[indexlat - 1][indexlon + 1]))
        vstring.append(1000*(latlonArray[indexlat - 1][indexlon + 2]-platlonArray[indexlat - 1][indexlon + 2]))
        vstring.append(1000*(latlonArray[indexlat][indexlon + 2]-platlonArray[indexlat][indexlon + 2]))
        vstring.append(1000*(latlonArray[indexlat + 1][indexlon + 2]-platlonArray[indexlat + 1][indexlon + 2]))
        vstring.append(1000*(latlonArray[indexlat + 2][indexlon + 2]-platlonArray[indexlat + 2][indexlon + 2]))
        vstring.append(1000*(latlonArray[indexlat + 2][indexlon + 1]-platlonArray[indexlat + 2][indexlon + 1]))
        vstring.append(1000*(latlonArray[indexlat + 2][indexlon]-platlonArray[indexlat + 2][indexlon]))
        vstring.append(1000*(latlonArray[indexlat + 2][indexlon - 1]-platlonArray[indexlat + 2][indexlon - 1]))
        vstring.append(1000*(latlonArray[indexlat + 1][indexlon - 1]-platlonArray[indexlat + 1][indexlon - 1]))
        vstring.append(1000*(latlonArray[indexlat][indexlon - 1]-platlonArray[indexlat][indexlon - 1]))
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
def GetStationsAndOnetimesFromEC(i, sfc_varinames,sfc_varinames1, sfc_file,pl_varinames,pl_file,inputfile,
                                 stationsVlist, stationdict,
                                 stationlist, dict01, trainclasfierlist,
                                 rainvaluelist, rainllabelist):
    print 'stationlist:',len(stationlist)
    for j in range(len(stationlist)):
        trainclasfier=0
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
        # 根据站号+实况数据的索引来获取实况数据
        kid = stationlist[j][0] + '_' + fdateStr
        trainlebel = stationdict.get(kid)
        #print kid,trainlebel
        # 判断该实况数据是否是有效值（不为99999或者None）,若有效再计算16个格点值，将其一起添加到训练样本
        if trainlebel <> None and trainlebel < 9999:
            latitude = float(stationlist[j][4])
            longitude = float(stationlist[j][5])
            # #    #首先计算经纬度对应格点的索引，
            indexlat = int((60  - latitude) / 0.125)
            indexlon = int((longitude - 60) / 0.125)
            #print latitude,longitude,indexlat,indexlon
            # 则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
            perstationlist = GetOnetimeFromEC(i, sfc_varinames,sfc_varinames1, sfc_file,pl_varinames,pl_file,
                                              indexlat, indexlon)
            #print dictid,perstationlist,kid,trainlebel
            dict01[dictid] = perstationlist
            if trainlebel == 0:
                trainclasfier = 0
            elif trainlebel == 999990:
                trainclasfier = 2
            elif trainlebel > 999990 and trainlebel<999990*2:
                trainclasfier = 1
                trainlebel = trainlebel - 999990
                rainvaluelist.append(perstationlist)
                rainllabelist.append(trainlebel)
            else:
                trainclasfier = 1
                # 只对降水量进行训练的话，另存list
                rainvaluelist.append(perstationlist)
                rainllabelist.append(trainlebel)
            stationsVlist.append(perstationlist)
            trainclasfierlist.append(trainclasfier)
            #print 'stationsVlist',len(stationsVlist),'stationdict',len(stationdict)
# EC格点数据的获取
def modelprocess(stationdict, stationlist, ll,allpath):
    sfc_varinames = ['CP_GDS0_SFC','TP_GDS0_SFC']
    sfc_varinames1=['10U_GDS0_SFC','10V_GDS0_SFC','2T_GDS0_SFC','2D_GDS0_SFC']
    pl_varinames=['R_GDS0_ISBL','U_GDS0_ISBL','V_GDS0_ISBL']
    dict01 = {}
    # 遍历文件
    stationsVlist = []
    trainclasfierlist = []
    rainvaluelist = []
    rainllabelist = []
    # 遍历所有的文件，
    for rootpath, dirs, files in os.walk(allpath):
        for file in files:
            if file[:3] == 'sfc' and file[-5:] == '.grib':
                inputfile = os.path.join(rootpath, file)
                inputfile2 = inputfile.replace('sfc', 'pl')
                sfcfile = Nio.open_file(inputfile, 'r')
                plfile = Nio.open_file(inputfile2, 'r')
                # 参数0是指第0个时次的预报,这里只是一个文件的2000个站的列表。
                #print ll, sfc_varinames, sfcfile,inputfile, stationsVlist, stationdict,stationlist, dict01,trainclasfierlist,rainvaluelist, rainllabelist
                GetStationsAndOnetimesFromEC(ll, sfc_varinames,sfc_varinames1, sfcfile,pl_varinames,plfile,
                                                 inputfile, stationsVlist, stationdict,
                                                 stationlist, dict01,
                                                 trainclasfierlist,
                                                 rainvaluelist, rainllabelist)
    # 晴雨训练集
    #print stationsVlist
    stationArray = numpy.array(stationsVlist)
    #print stationArray
    #晴雨预测集
    trainclasfierArray = numpy.array(trainclasfierlist)
    #降水训练集
    rainvalueArray=numpy.array(rainvaluelist)
    #print rainvalueArray
    #降水预测集
    rainllabelArray=numpy.array(rainllabelist)
    #print rainllabelArray
    #先释放一些内存
    dict01={}
    stationsVlist = []
    trainclasfierlist = []
    rainvaluelist = []
    rainllabelist = []
    #为了统计准确率，提前分割降水数据集
    class_train,class_test=train_test_split(stationArray,test_size=0.33,random_state=7)
    a_train, a_test = train_test_split(rainvalueArray, test_size=0.33,random_state=7)
    #数据训练前进行标准化
    stationArray=stationArray.astype('float32')
    rainvalueArray=rainvalueArray.astype('float32')
    print stationArray.shape,rainvalueArray.shape
    x_scaled = preprocessing.scale(stationArray)
    stationArray = x_scaled
    x_scaled01=preprocessing.scale(rainvalueArray)
    rainvalueArray=x_scaled01
    # xgboost，训练集和预测集分割
    x_train, x_test, y_train, y_test = train_test_split(stationArray, trainclasfierArray,test_size=0.33, random_state=7)
    u_train, u_test, v_train, v_test=train_test_split(rainvalueArray, rainllabelArray,test_size=0.33, random_state=7)
    print len(x_train),len( x_test),len(y_train),len( y_test),len( u_train),len( u_test), len(v_train),len( v_test)
    xgbtrain = xgboost.DMatrix(x_train, label=y_train)
    xgbtest = xgboost.DMatrix(x_test, label=y_test)
    xgbtrain01 = xgboost.DMatrix(u_train, label=v_train)
    xgbtest01 = xgboost.DMatrix(u_test, label=v_test)
    # xgbtrain.save_binary('train.buffer')
    # print len(x_train),len(x_test),len(y_train),len(y_test)
    # print xgbtest
    # 训练和验证的错误率
    watchlist = [(xgbtrain, 'xgbtrain'), (xgbtest, 'xgbeval')]
    watchlist01 = [(xgbtrain01, 'xgbtrain01'), (xgbtest01, 'xgbeval01')]
    params = {
        'booster': 'gbtree',
        'objective': 'multi:softmax',  # 分类
        'num_class':3,#分3类
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
        #'nthread': 20,  # cpu 线程数
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
        #'nthread': 20,  # cpu 线程数
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
    model.save_model(os.path.join(outpath,'pre' + str(ll) + '.model'))
    model01.save_model(os.path.join(outpath,'rain' + str(ll) + '.model'))
    #print preds, preds01
    na=0
    nb=0
    nc=0
    nd=0
    na01=0
    nb01=0
    nc01=0
    nd01=0
    n=len(preds)
    classfile=os.path.join(outpath,'class'+str(ll)+'.csv')
    classfw=open(classfile,'w')
    #取EC训练数据的检验集降水列,16列中的第一列，
    classecll=class_test[:, 0]
    #转成01标识的晴雨
    y_classorigin=[]
    for rr in range(len(classecll)):
        if classecll[rr]==0:
            y_classorigin.append(0)
        else:
            y_classorigin.append(1)
    #训练前的准确率
    #print type(y_test)
    accuracy_before=accuracy_score(y_classorigin,y_test)
    print("训练前的准确率Accuracy: %.2f%%" % (accuracy_before * 100.0))
    # 分类预报准确率
    accuracy_after = accuracy_score(y_test, preds)
    print("训练后的准确率Accuracy: %.2f%%" % (accuracy_after* 100.0))
    for nn in range(len(preds)):
        classec=0
        if float(classecll[nn])==0:
            classec=0
        else:
            classec=1
        if classec==0 and y_test[nn]==0:
            nd=nd+1
        elif y_test[nn]==0 and classec>0:
            nb=nb+1
        elif y_test[nn]>0 and  classec==0:
            nc=nc+1
        elif y_test[nn]>0 and  classec>0:
            na=na+1
        if y_test[nn]>0 and preds[nn]>0:
            na01=na01+1
        elif y_test[nn]==0 and preds[nn]>0:
            nb01=nb01+1
        elif y_test[nn]>0 and preds[nn]==0:
            nc01=nc01+1
        elif y_test[nn]==0 and preds[nn]==0:
            nd01=nd01+1
        classfw.write(str(classec)+','+str(preds[nn])+','+str(y_test[nn]))
        classfw.write('\n')
    classfw.close()
    print '各种评分数据统计如下：'
    print n,na,nb,nc,nd,na01,nb01,nc01,nd01
    #降水TS评分
    ts=float(na)/float(na+nb+nc)
    ts01=float(na01)/float(na01+nb01+nc01)
    print('训练前TS评分：%.4f'% ts)
    print('训练后TS评分：%.4f' % ts01)
    bias=float(na+nb)/float(na+nc)
    bias01=float(na01+nb01)/float(na01+nc01)
    print('训练前BIAS评分：%.4f'% bias)
    print('训练后BIAS评分：%.4f' % bias01)
    f=float((na+nb))*float(na+nc)/float(n)
    f01=float((na01+nb01))*float(na01+nc01)/float(n)
    ets=float(na-f)/float(na+nb+nc-f)
    ets01=float(na01-f01)/float(na01+nb01+nc01-f01)
    print('训练前ETS评分：%.4f'% ets)
    print('训练后ETS评分：%.4f' % ets01)
    print str(ts)+','+str(ts01)+','+str(bias)+','+str(bias01)+','+str(ets)+','+str(ets01)
    predsfile = os.path.join(outpath, 'prepreds' + str(ll) + '.csv')
    predsfw = open(predsfile, 'w')
    for pp in range(len(y_test)):
        predsfw.write(str(classecll[pp]) +','+str(y_classorigin[pp])+ ',' + str(preds[pp]) + ',' + str(y_test[pp]))
        predsfw.write('\n')
    predsfw.close()
    # 对模型2训练结果进行统计--回归准确率
    # 回归准确率
    #y_test = y_test.astype('float32')
    mse = mean_squared_error(v_test, preds01)
    rmse = math.sqrt(mse)
    mae = mean_absolute_error(v_test, preds01, multioutput='uniform_average')
    #print preds01
    print("训练后RMSE: %.4f" % rmse)
    print("训练后MAE: %.4f" % mae)
    # 和EC中原始数据对比获取均方误差
    #print type(a_test)
    y_origin = a_test[:, 0]
    #print len(u_test),len(v_test)
    mse0 = mean_squared_error(y_origin, v_test)
    rmse0 = math.sqrt(mse0)
    mae0 = mean_absolute_error(y_origin, v_test, multioutput='uniform_average')
    print("训练前RMSE: %.4f" % rmse0)
    print("训练前MAE: %.4f" % mae0)
    # 输出降水量的预报
    rainfile = os.path.join(outpath, 'rainpreds' + str(ll) + '.csv')
    rainfw = open(rainfile, 'w')
    for qq in range(len(v_test)):
        rainfw.write(str(y_origin[qq]) + ',' + str(preds01[qq]) + ',' + str(v_test[qq]))
        rainfw.write('\n')
    rainfw.close()
    # 打印所有变量
    print str(n) + ',' + str(na) + ',' + str(nb) + ',' + str(nc) + ',' + str(
        nd) + ',' + str(na01) + ',' + str(nb01) + ',' + str(nc01) + ',' + str(
        nd01) + ',' + str(accuracy_before) + ',' + str(
        accuracy_after) + ',' + str(ts) + ',' + str(ts01) + ',' + str(
        bias) + ',' + str(bias01) + ',' + str(ets) + ',' + str(
        ets01) + ',' + str(rmse0) + ',' + str(mae0) + ',' + str(
        rmse) + ',' + str(mae) + ',' + str(len(u_train)) + ',' + str(
        len(u_test))
if __name__ == '__main__':
    starttime = datetime.datetime.now()
    #outpath = '/Users/yetao.lu/Desktop/mos/anonymous'
    outpath = '/home/wlan_dev/model'
    ll=60
    stationdict = {}
    # 站点列表数据
    stationlist = []
    #csvfile = '/Users/yetao.lu/Desktop/mos/t_p_station_cod.csv'
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
    #降水数据累计
    if ll <= 48:
        # 处理站点3h降水实况数据
        #tempcsv = '/Users/yetao.lu/Desktop/mos/data/precsv'
        tempcsv = '/home/wlan_dev/precsv'
        stationdict = pre3hTodict(tempcsv)
    else:
        # 处理站点6小时降水数据
        #precsv='/Users/yetao.lu/Desktop/mos/data/pre6h'
        precsv = '/home/wlan_dev/pre6h'
        stationdict = pre6hTodict(precsv)
    #allpath = '/Users/yetao.lu/Documents/testdata/'
    allpath = '/mnt/data/MOS/'
    modelprocess(stationdict,stationlist,ll,allpath)
    endtime = datetime.datetime.now()
    # print len(dict)
    print(endtime - starttime).seconds
