#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/9
description:
"""
import MySQLdb,os,json,logging,datetime,shutil
#遍历文件夹下及子文件夹下所有文件
filelist=[]
def gci(filepath):
    #遍历filepath下所有文件，包括子目录
    files = os.listdir(filepath)
    for fi in files:
        fi_d = os.path.join(filepath,fi)
        if os.path.isdir(fi_d):
            gci(fi_d)
        else:
            filefullpath=os.path.join(filepath,fi_d)
            filelist.append(filefullpath)
            #print filefullpath
    return filelist
jsonfilelist=gci('/Users/yetao.lu/moslivedata')
db=MySQLdb.connect('192.168.1.84','admin','moji_China_123','moge')
cursor = db.cursor()
#print len(jsonfilelist)
for i in range(len(jsonfilelist)):
    jsonfile=jsonfilelist[i]
    print jsonfile
    if jsonfile[-4:]=='json':
        fileRead = open(jsonfile, 'r')
        jsonstring = fileRead.read()
        # print jsonstring
        # jsonstring=jsonstring.encode("utf-8")
        # print jsonstring
        # data=json.dumps(jsonstring)
        datastring = json.loads(jsonstring)
        # print datastring['DS']
        datalist = datastring['DS']
        L=[]
        sqllist=[]
        sql1=''
        for i in range(len(datalist)):
            prelist = []
            sql1 = ''
            perline=datalist[i]
            station_name=perline['Station_Name']
            prelist.append(station_name)
            province=perline['Province']
            prelist.append(province)
            city=perline['City']
            prelist.append(city)
            cnty=perline['Cnty']
            prelist.append(cnty)
            town=perline['Town']
            prelist.append(town)
            stationid=perline['Station_Id_d']
            prelist.append(stationid)
            lat=perline['Lat']
            prelist.append(lat)
            lon=perline['Lon']
            prelist.append(lon)
            alti=perline['Alti']
            prelist.append(alti)
            PRS_Sensor_Alti=perline['PRS_Sensor_Alti']
            prelist.append(PRS_Sensor_Alti)
            Station_Type=perline['Station_Type']
            prelist.append(Station_Type)
            Station_levl=perline['Station_levl']
            prelist.append(Station_levl)
            year=perline['Year']
            prelist.append(year)
            mon=perline['Mon']
            prelist.append(mon)
            day=perline['Day']
            prelist.append(day)
            hour=perline['Hour']
            prelist.append(hour)
            prs=perline['PRS']
            prelist.append(prs)
            PRS_Sea=perline['PRS_Sea']
            prelist.append(PRS_Sea)
            PRS_Max=perline['PRS_Max']
            prelist.append(PRS_Max)
            PRS_Max_OTime=perline['PRS_Max_OTime']
            prelist.append(PRS_Max_OTime)
            PRS_Min=perline['PRS_Min']
            prelist.append(PRS_Min)
            PRS_Min_OTime=perline['PRS_Min_OTime']
            prelist.append(PRS_Min_OTime)
            TEM=perline['TEM']
            prelist.append(TEM)
            TEM_Max=perline['TEM_Max']
            prelist.append(TEM_Max)
            TEM_Max_OTime=perline['TEM_Max_OTime']
            prelist.append(TEM_Max_OTime)
            TEM_Min=perline['TEM_Min']
            prelist.append(TEM_Min)
            TEM_Min_OTime=perline['TEM_Min_OTime']
            prelist.append(TEM_Min_OTime)
            DPT=perline['DPT']
            prelist.append(DPT)
            RHU=perline['RHU']
            prelist.append(RHU)
            RHU_Min=perline['RHU_Min']
            prelist.append(RHU_Min)
            RHU_Min_OTIME=perline['RHU_Min_OTIME']
            prelist.append(RHU_Min_OTIME)
            VAP=perline['VAP']
            prelist.append(VAP)
            PRE_1h=perline['PRE_1h']
            prelist.append(PRE_1h)
            PRE_3h=perline['PRE_3h']
            prelist.append(PRE_3h)
            PRE_6h=perline['PRE_6h']
            prelist.append(PRE_6h)
            PRE_12h=perline['PRE_12h']
            prelist.append(PRE_12h)
            PRE_24h=perline['PRE_24h']
            prelist.append(PRE_24h)
            PRE=perline['PRE']
            prelist.append(PRE)
            EVP_Big=perline['EVP_Big']
            prelist.append(EVP_Big)
            WIN_D_Avg_10mi=perline['WIN_D_Avg_10mi']
            prelist.append(WIN_D_Avg_10mi)
            WIN_S_Avg_10mi=perline['WIN_S_Avg_10mi']
            prelist.append(WIN_S_Avg_10mi)
            WIN_D_S_Max=perline['WIN_D_S_Max']
            prelist.append(WIN_D_S_Max)
            WIN_S_Max=perline['WIN_S_Max']
            prelist.append(WIN_S_Max)
            WIN_S_Max_OTime=perline['WIN_S_Max_OTime']
            prelist.append(WIN_S_Max_OTime)
            WIN_D_INST_Max=perline['WIN_D_INST_Max']
            prelist.append(WIN_D_INST_Max)
            WIN_S_Inst_Max=perline['WIN_S_Inst_Max']
            prelist.append(WIN_S_Inst_Max)
            WIN_S_INST_Max_OTime=perline['WIN_S_INST_Max_OTime']
            prelist.append(WIN_S_INST_Max_OTime)
            GST=perline['GST']
            prelist.append(GST)
            GST_Max=perline['GST_Max']
            prelist.append(GST_Max)
            GST_Max_Otime=perline['GST_Max_Otime']
            prelist.append(GST_Max_Otime)
            GST_Min=perline['GST_Min']
            prelist.append(GST_Min)
            GST_Min_OTime=perline['GST_Min_OTime']
            prelist.append(GST_Min_OTime)
            VIS_Min=perline['VIS_Min']
            prelist.append(VIS_Min)
            VIS_Min_OTime=perline['VIS_Min_OTime']
            prelist.append(VIS_Min_OTime)
            VIS=perline['VIS']
            prelist.append(VIS)
            CLO_Cov=perline['CLO_Cov']
            prelist.append(CLO_Cov)
            CLO_Cov_Low=perline['CLO_Cov_Low']
            prelist.append(CLO_Cov_Low)
            CLO_COV_LM=perline['CLO_COV_LM']
            prelist.append(CLO_COV_LM)
            CLO_Height_LoM=perline['CLO_Height_LoM']
            prelist.append(CLO_Height_LoM)
            WEP_Now=perline['WEP_Now']
            prelist.append(WEP_Now)
            Snow_Depth=perline['Snow_Depth']
            prelist.append(Snow_Depth)
            Snow_PRS=perline['Snow_PRS']
            prelist.append(Snow_PRS)
            vdate01=datetime.datetime(int(year),int(mon),int(day),int(hour),0,0)
            vdatestring=datetime.datetime.strftime(vdate01,'%Y-%m-%d %H:%M:%S')
            prelist.append(vdatestring)
            sql='insert into cimiss_mos (Station_Name,Province,City,Cnty,Town,Station_Id_d,Lat,Lon,Alti,PRS_Sensor_Alti,Station_Type,Station_levl,Year,Mon,Day,Hour,PRS,PRS_Sea,PRS_Max,PRS_Max_OTime,' \
                'PRS_Min,PRS_Min_OTime,TEM,TEM_Max,TEM_Max_OTime,TEM_Min,TEM_Min_OTime,DPT\
                    ,RHU,RHU_Min,RHU_Min_OTIME,VAP,PRE_1h,PRE_3h,PRE_6h,PRE_12h,PRE_24h,PRE,EVP_Big,WIN_D_Avg_10mi,WIN_S_Avg_10mi,WIN_D_S_Max,WIN_S_Max,WIN_S_Max_OTime,WIN_D_INST_Max,WIN_S_Inst_Max,' \
                'WIN_S_INST_Max_OTime,GST,GST_Max,GST_Max_Otime,GST_Min,GST_Min_OTime,VIS_Min,VIS_Min_OTime,VIS,CLO_Cov,CLO_Cov_Low,CLO_COV_LM,CLO_Height_LoM,WEP_Now,Snow_Depth,Snow_PRS,vdate' \
                ')VALUES("'+station_name+'","'+province+'","'+city+'","'+cnty+'","'+town+'",'+stationid+','+lat+','+lon+','+alti+','+PRS_Sensor_Alti+','+Station_Type+','+Station_levl+','+year+','+mon+','+day+','+hour+','+prs+','+PRS_Sea+','+PRS_Max+','+PRS_Max_OTime+','\
                +PRS_Min+','+PRS_Min_OTime+','+TEM+','+TEM_Max+','+TEM_Max_OTime+','+TEM_Min+','+TEM_Min_OTime+','+DPT+','\
                +RHU+','+RHU_Min+','+RHU_Min_OTIME+','+VAP+','+PRE_1h+','+PRE_3h+','+PRE_6h+','+PRE_12h+','+PRE_24h+','+PRE+','+EVP_Big+','+WIN_D_Avg_10mi+','+WIN_S_Avg_10mi+','+WIN_D_S_Max+','+WIN_S_Max+','+WIN_S_Max_OTime+','+WIN_D_INST_Max+','+WIN_S_Inst_Max+\
                ','+WIN_S_INST_Max_OTime+','+GST+','+GST_Max+','+GST_Max_Otime+','+GST_Min+','+GST_Min_OTime+','+VIS_Min+','+VIS_Min_OTime+','+VIS+','+CLO_Cov+','+CLO_Cov_Low+','+CLO_COV_LM+','+CLO_Height_LoM+','+WEP_Now+','+Snow_Depth+','+Snow_PRS+','+vdatestring+')'
            #print sql
            sql1='insert into cimiss_mos (Station_Name,Province,City,Cnty,Town,Station_Id_d,Lat,Lon,Alti,PRS_Sensor_Alti,Station_Type,Station_levl,Year,Mon,Day,Hour,PRS,PRS_Sea,PRS_Max,PRS_Max_OTime,' \
                'PRS_Min,PRS_Min_OTime,TEM,TEM_Max,TEM_Max_OTime,TEM_Min,TEM_Min_OTime,DPT\
                    ,RHU,RHU_Min,RHU_Min_OTIME,VAP,PRE_1h,PRE_3h,PRE_6h,PRE_12h,PRE_24h,PRE,EVP_Big,WIN_D_Avg_10mi,WIN_S_Avg_10mi,WIN_D_S_Max,WIN_S_Max,WIN_S_Max_OTime,WIN_D_INST_Max,WIN_S_Inst_Max,' \
                'WIN_S_INST_Max_OTime,GST,GST_Max,GST_Max_Otime,GST_Min,GST_Min_OTime,VIS_Min,VIS_Min_OTime,VIS,CLO_Cov,CLO_Cov_Low,CLO_COV_LM,CLO_Height_LoM,WEP_Now,Snow_Depth,Snow_PRS,vdate' \
                ')VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            sqllist.append(sql1)
            #print len(prelist)
            L.append(prelist)
            #cursor.execute(sql)
            if len(L)>5000:
                cursor.executemany(sql1,L)
                db.commit()
                L=[]
        cursor.executemany(sql1, L)
        db.commit()
        bakpath = '/Users/yetao.lu/moslivedatabak'
        shutil.move(jsonfile,os.path.join(bakpath,jsonfile[-19:]))
db.close()

