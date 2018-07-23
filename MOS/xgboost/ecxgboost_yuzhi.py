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
    #print 'stationlist:',len(stationlist)
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
        # 判断该实况数据是否是有效值（不为99999或者None）,若有效再计算16个格点值，将其一起添加到训练样本
        if trainlebel <> None and trainlebel < 9999:
            latitude = float(stationlist[j][4])
            longitude = float(stationlist[j][5])
            # #    #首先计算经纬度对应格点的索引，
            indexlat = int((60 + 0.125 / 2 - latitude) / 0.125)
            indexlon = int((longitude + 0.125 / 2 - 60) / 0.125)
            # print latitude,longitude,indexlat,indexlon
            # 则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
            perstationlist = GetOnetimeFromEC(i, sfc_varinames,sfc_varinames1, sfc_file,pl_varinames,pl_file,
                                              indexlat, indexlon)
            # print dictid,perstalist,kid,trainlebel
            dict01[dictid] = perstationlist
            if trainlebel == 0 or trainlebel==999999:
                trainclasfier = 0
            else:
                trainclasfier = 1
            stationsVlist.append(perstationlist)
            trainclasfierlist.append(trainclasfier)
            #print 'stationsVlist',len(stationsVlist),'trainlebellist',len(trainlebellist),'stationdict',len(stationdict)
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
                #print ll, sfc_varinames, sfcfile,inputfile, stationsVlist,trainlebellist, stationdict,stationlist, dict01,trainclasfierlist,rainvaluelist, rainllabelist
                GetStationsAndOnetimesFromEC(ll, sfc_varinames,sfc_varinames1, sfcfile,pl_varinames,plfile,
                                                 inputfile, stationsVlist, stationdict,
                                                 stationlist, dict01,
                                                 trainclasfierlist,
                                                 rainvaluelist, rainllabelist)
    # 晴雨训练集
    #print stationsVlist
    stationArray = numpy.array(stationsVlist)
    trainclasfierArray = numpy.array(trainclasfierlist)
    #取EC训练数据的检验集降水列,16列中的第一列，
    classecll=stationArray[:, 0]
    n=len(classecll)
    resultfile=os.path.join(outpath,'yuzhi'+str(ll)+'.csv')
    fw=open(resultfile,'w')
    for uu in range(100):
        na = 0
        nb = 0
        nc = 0
        nd = 0
        #转成01标识的晴雨
        y_classorigin=[]
        for rr in range(len(classecll)):
            if classecll[rr]<=0.1*(uu):
                y_classorigin.append(0)
            else:
                y_classorigin.append(1)
        #训练前的准确率
        accuracy_before=accuracy_score(y_classorigin,trainclasfierArray)
        print("训练前的准确率Accuracy: %.2f%%" % (accuracy_before * 100.0))
        #print y_classorigin,trainclasfierArray
        for nn in range(len(y_classorigin)):
            #print y_classorigin[nn],trainclasfierArray[nn]
            if y_classorigin[nn] == 0 and trainclasfierArray[nn] == 0:
                nd = nd + 1
            elif y_classorigin[nn] == 0 and trainclasfierArray[nn] ==1:
                nb = nb + 1
            elif y_classorigin[nn] == 1 and trainclasfierArray[nn] == 0:
                nc = nc + 1
            elif y_classorigin[nn] ==1 and trainclasfierArray[nn] == 1:
                na = na + 1
        print '各种评分数据统计如下：'
        print na,nb,nc,nd
        #降水TS评分
        ts=float(na)/float(na+nb+nc)
        print('训练前TS评分：%.4f'% ts)
        bias=float(na+nb)/float(na+nc)
        print('训练前BIAS评分：%.4f'% bias)
        f=float((na+nb))*float(na+nc)/float(n)
        ets=float(na-f)/float(na+nb+nc-f)
        print('训练前ETS评分：%.4f'% ets)
        print str(n)+','+str(na)+','+str(nb)+','+str(nc)+','+str(nd)+','+str(ts)+','+str(bias)+','+str(ets)+','+str(accuracy_before)
        # 打印所有变量
        fw.write(str(0.1*uu)+','+str(n)+','+str(na)+','+str(nb)+','+str(nc)+','+str(nd)+','+str(ts)+','+str(bias)+','+str(ets)+','+str(accuracy_before))
        fw.write('\n')
    fw.close()
if __name__ == '__main__':
    starttime = datetime.datetime.now()
    #outpath = '/Users/yetao.lu/Desktop/mos/anonymous'
    outpath = '/home/wlan_dev/model'
    ll=1
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
    print(endtime - starttime).seconds
    