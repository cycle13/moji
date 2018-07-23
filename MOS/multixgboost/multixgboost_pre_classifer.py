#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/30
description:  对降水先进行晴雨训练，然后再进行降水量进行训练

"""
import Nio, datetime, os,string,logging,xgboost,numpy,math,sys,multiprocessing
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import accuracy_score
from sklearn import preprocessing
starttime=datetime.datetime.now()
def pre3hTodict(tempcsv):
    stationdict={}
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
                        pdatetime = datetime.datetime.strptime(linearray[1], '%Y-%m-%d %H:%M:%S')
                        timestring = datetime.datetime.strftime(pdatetime, '%Y%m%d%H%M%S')
                        sdictId = linearray[0] + '_' + timestring
                        if float(linearray[5]) <> 999999 or float(linearray[5]) <> None or float(linearray[5]) <> 999998:
                            stationdict[sdictId] = float(linearray[5])
                    if not line:
                        break
    return stationdict
def pre6hTodict(precsv):
    station6hdict={}
    for prootpath,pdirs,pstationfile in os.walk(precsv):
        for ii in range(len(pstationfile)):
            if pstationfile[ii][-4:]=='.csv':
                pstationfilepath=os.path.join(prootpath,pstationfile[ii])
                pfileread=open(pstationfilepath,'r')
                while True:
                    pline=pfileread.readline()
                    plinearray=pline.split(',')
                if len(plinearray)>4:
                    pdictid=plinearray[0]+plinearray[1]
                    ppdatetime=datetime.datetime.strptime(plinearray[1],'%Y-%m-%d %H:%M:%S')
                    ptimestring=datetime.datetime.strftime(ppdatetime,'%Y%m%d%H%M%S')
                    pdictid=plinearray[0]+'_'+ptimestring
                    if float(plinearray[5])<>999999 or float(plinearray[5])<>None or float(plinearray[5])<>999998:
                        station6hdict[pdictid]=float(plinearray[5])
                if not pline:
                    break
    return station6hdict
def GetOnetimeFromEC(n,sfc_varinames,sfc_file,indexlat,indexlon):
    vstring=[]
    for i in range(len(sfc_varinames)):
        variArray=sfc_file.variables[sfc_varinames[i]]
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
    return vstring
def GetStationsAndOnetimesFromEC(i,sfc_varinames,sfc_file,inputfile,stationsVlist,trainlebellist,stationdict,stationlist,dict01,trainclasfierlist,rainvaluelist,rainllabelist):
    #print 'stationlist:',len(stationlist)
    for j in range(len(stationlist)):
        # 根据文件名来建立索引获取实况数据气温的值
        strarray = inputfile.split('_')
        odatetime = datetime.datetime.strptime((strarray[1] + strarray[2][:2]),
                                               '%Y%m%d%H')
        if i<=48:
            fdatetime = odatetime + datetime.timedelta(hours=i*3)
        else:
            fdatetime=odatetime+datetime.timedelta(hours=48*3+(i-48)*6)
        fdateStr = datetime.datetime.strftime(fdatetime, '%Y%m%d%H%M%S')
        # 这里只能是起报时间加预报时效，不能用预报时间。因预报时间有重复
        dictid = stationlist[j][0] + '_' + strarray[1] + strarray[2][
        :2] + '_' + str(i)
        #根据站号+实况数据的索引来获取实况数据
        kid = stationlist[j][0] + '_' + fdateStr
        trainlebel = stationdict.get(kid)
        #判断该实况数据是否是有效值（不为99999或者None）,若有效再计算16个格点值，将其一起添加到训练样本
        if trainlebel<>None and trainlebel<9999:
            latitude=float(stationlist[j][4])
            longitude=float(stationlist[j][5])
            # #    #首先计算经纬度对应格点的索引，
            indexlat = int((60+0.125/2 - latitude) / 0.125)
            indexlon = int((longitude+0.125/2 - 60) / 0.125)
            #print latitude,longitude,indexlat,indexlon
            # 则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
            perstationlist=GetOnetimeFromEC(i,sfc_varinames,sfc_file,indexlat,indexlon)
            #print dictid,perstalist,kid,trainlebel
            dict01[dictid] = perstationlist
            if trainlebel == 0:
                trainclasfier = 0
            elif trainlebel==999990:
                trainclasfier=2
            elif trainlebel>999990:
                trainclasfier = 1
                trainlebel=trainlebel-999990
            else:
                trainlebel=1
                #只对降水量进行训练的话，另存list
                rainvaluelist.append(perstationlist)
                rainllabelist.append(trainlebel)
            stationsVlist.append(perstationlist)
            trainlebellist.append(trainlebel)
            trainclasfierlist.append(trainclasfier)
    #print 'stationsVlist',len(stationsVlist),'trainlebellist',len(trainlebellist),'stationdict',len(stationdict)
#EC格点数据的获取
def modelprocess(stationdict,stationlist,ll):
    logger=logging.getLogger('ll')
    logger.setLevel(logging.DEBUG)
    fh=logging.FileHandler(os.path.join(outpath,'pre'+str(ll)+'.log'))
    fh.setLevel(logging.DEBUG)
    consolehand=logging.StreamHandler()
    consolehand.setLevel(logging.DEBUG)
    formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    consolehand.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(consolehand)
    sys.stdout=open(os.path.join(outpath,'rain'+str(ll)+'.out'),'w')
    sfc_varinames = ['CP_GDS0_SFC']
    dict01={}
    allpath = '/moji/meteo/cluster/data/MOS/'
    # 遍历文件
    stationsVlist = []
    trainlebellist=[]
    trainclasfierlist=[]
    rainvaluelist=[]
    rainllabelist=[]
    #遍历所有的文件，
    for rootpath, dirs, files in os.walk(allpath):
        for file in files:
            if file[:3] == 'sfc' and file[-5:] == '.grib':
                inputfile = os.path.join(rootpath, file)
                sfcfile=Nio.open_file(inputfile,'r')
                #参数0是指第0个时次的预报,这里只是一个文件的2000个站的列表。
                try:
                    GetStationsAndOnetimesFromEC(ll,sfc_varinames,sfcfile,inputfile,stationsVlist,trainlebellist,stationdict,stationlist,dict01,trainclasfierlist,rainvaluelist,rainllabelist)
                except Exception,ex:
                    logger.info(ex)
    #训练集
    stationArray=numpy.array(stationsVlist)
    #预测集
    trainlebelArray=numpy.array(trainlebellist)
    a_train,a_test=train_test_split(stationArray,test_size=0.33,random_state=7)
    #数据训练前进行标准化
    x_scaled=preprocessing.scale(stationArray)
    stationArray=x_scaled
    #xgboost，训练集和预测集分割
    x_train,x_test,y_train,y_test,u_train,u_test,v_train,v_test=train_test_split(stationArray,trainlebelArray,rainvaluelist,rainllabelist,test_size=0.33,random_state=7)
    xgbtrain=xgboost.DMatrix(x_train,label=y_train)
    xgbtest=xgboost.DMatrix(x_test,label=y_test)
    xgbtrain01=xgboost.DMatrix(u_train,label=v_train)
    xgbtest01=xgboost.DMatrix(u_test,label=v_test)
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
    'eta': 0.1, # 如同学习率
    'seed':1000,
    'nthread':3,# cpu 线程数
    #'eval_metric': 'auc'
    'scale_pos_weight':1
    }
    plst=list(params.items())
    num_rounds=9999
    #early_stopping_rounds当设置的迭代次数较大时,early_stopping_rounds 可在一定的迭代次数内准确率没有提升就停止训练
    model=xgboost.train(plst,xgbtrain,num_rounds,watchlist,early_stopping_rounds=500)
    model01=xgboost.train(plst,xgbtrain01,num_rounds,watchlist,early_stopping_rounds=500)
    #print model,watchlist
    preds=model.predict(xgbtest,ntree_limit=model.best_iteration)
    preds01=model01.predict(xgbtest01,ntree_limit=model.best_iteration)
    # 将预测结果写入文件，方式有很多，自己顺手能实现即可
    # numpy.savetxt('submission_xgb_MultiSoftmax.csv',numpy.c_[range(1,len(test)+1),preds],
    #                 delimiter=',',header='ImageId,Label',comments='',fmt='%d')
    print preds,preds01
    #分类准确率
    #accuracy=accuracy_score(v_test,preds01)
    accuracy=accuracy_score(v_test,preds01)
    print("分类后的准确率Accuracy: %.2f%%" % (accuracy * 100.0))
    #回归准确率
    y_test=y_test.astype('float32')
    mse=mean_squared_error(v_test,preds01)
    rmse=math.sqrt(mse)
    mae=mean_absolute_error(v_test,preds01,multioutput='uniform_average')
    print("训练后RMSE: %.4f" % rmse)
    print("训练后MAE: %.4f" % mae)
    #和EC中原始数据对比获取均方误差
    y_origin=a_test[:,0]
    mse0=mean_squared_error(v_test,v_train)
    rmse0=math.sqrt(mse0)
    mae0=mean_absolute_error(v_test,v_train,multioutput='uniform_average')
    print("RMSE: %.4f" % rmse0)
    print("MAE: %.4f" % mae0)
    print rmse0,mae0,rmse,mae,len(u_train),len(v_train)
    model.save_model('/home/wlan_dev/model/pre'+str(ll)+'.model')
    model01.save_model('/home/wlan_dev/model/rain'+str(ll)+'.model')
    predsfile=os.path.join(outpath,'prepreds'+str(ll)+'.csv')
    predsfw=open(predsfile,'w')
    for pp in range(len(y_test)):
        predsfw.write(str(y_origin)+','+str(preds)+','+str(y_test))
        predsfw.write('\n')
    predsfw.close()
    #输出降水量的预报
    rainfile=os.path.join(outpath,'rainpreds'+str(ll)+'.csv')
    rainfw=open(rainfile,'w')
    for qq in range(len(v_test)):
        rainfw.write(str(v_train)+','+str(preds01)+','+str(v_test))
        rainfw.write('\n')
    rainfw.close()
    
if __name__=='__main__':
    stationdict = {}
    # 站点列表数据
    stationlist = []
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
    print len(stationdict), len(stationlist)
    outpath='/home/wlan_dev/model'
    pool = multiprocessing.Pool()
    results = []
    for ii in range(0,4):
        if ii<=48:
            # 处理站点3h降水实况数据
            tempcsv = '/home/wlan_dev/precsv'
            stationdict=pre3hTodict(tempcsv)
        else:
            # 处理站点6小时降水数据
            precsv = '/home/wlan_dev/pre6h'
            stationdict=pre6hTodict(precsv)
        result = pool.apply_async(modelprocess, args=(stationdict, stationlist, ii))
        results.append(result)
    pool.close()
    pool.join()
endtime=datetime.datetime.now()
#print len(dict)
print(endtime-starttime).seconds
