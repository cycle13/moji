#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/9
description:
"""
import MySQLdb,os,json
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
            print filefullpath
    return filelist
jsonfilelist=gci('/Users/yetao.lu/Documents/surf_data')
db=MySQLdb.connect('192.168.1.84','admin','moji_China_123','moge')
cursor = db.cursor()
print len(jsonfilelist)
for i in range(len(jsonfilelist)):
    jsonfile=jsonfilelist[i]
    print jsonfile[-4:]
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
        for i in range(len(datalist)):
            perline=datalist[i]
            station_name=perline['Station_Name']
            province=perline['Province']
            city=perline['City']
            cnty=perline['Cnty']
            town=perline['Town']
            stationid=perline['Station_Id_d']
            lat=perline['Lat']
            lon=perline['Lon']
            alti=perline['Alti']
            PRS_Sensor_Alti=perline['PRS_Sensor_Alti']
            Station_Type=perline['Station_Type']
            Station_levl=perline['Station_levl']
            year=perline['Year']
            mon=perline['Mon']
            day=perline['Day']
            hour=perline['Hour']
            prs=perline['PRS']
            PRS_Sea=perline['PRS_Sea']
            PRS_Max=perline['PRS_Max']
            PRS_Max_OTime=perline['PRS_Max_OTime']
            PRS_Min=perline['PRS_Min']
            PRS_Min_OTime=perline['PRS_Min_OTime']
            TEM=perline['TEM']
            TEM_Max=perline['TEM_Max']
            TEM_Max_OTime=perline['TEM_Max_OTime']
            TEM_Min=perline['TEM_Min']
            TEM_Min_OTime=perline['TEM_Min_OTime']
            DPT=perline['DPT']
            RHU=perline['RHU']
            RHU_Min=perline['RHU_Min']
            RHU_Min_OTIME=perline['RHU_Min_OTIME']
            VAP=perline['VAP']
            PRE_1h=perline['PRE_1h']
            PRE_3h=perline['PRE_3h']
            PRE_6h=perline['PRE_6h']
            PRE_12h=perline['PRE_12h']
            PRE_24h=perline['PRE_24h']
            PRE=perline['PRE']
            EVP_Big=perline['EVP_Big']
            WIN_D_Avg_10mi=perline['WIN_D_Avg_10mi']
            WIN_S_Avg_10mi=perline['WIN_S_Avg_10mi']
            WIN_D_S_Max=perline['WIN_D_S_Max']
            WIN_S_Max=perline['WIN_S_Max']
            WIN_S_Max_OTime=perline['WIN_S_Max_OTime']
            WIN_D_INST_Max=perline['WIN_D_INST_Max']
            WIN_S_Inst_Max=perline['WIN_S_Inst_Max']
            WIN_S_INST_Max_OTime=perline['WIN_S_INST_Max_OTime']
            GST=perline['GST']
            GST_Max=perline['GST_Max']
            GST_Max_Otime=perline['GST_Max_Otime']
            GST_Min=perline['GST_Min']
            GST_Min_OTime=perline['GST_Min_OTime']
            VIS_Min=perline['VIS_Min']
            VIS_Min_OTime=perline['VIS_Min_OTime']
            VIS=perline['VIS']
            CLO_Cov=perline['CLO_Cov']
            CLO_Cov_Low=perline['CLO_Cov_Low']
            CLO_COV_LM=perline['CLO_COV_LM']
            CLO_Height_LoM=perline['CLO_Height_LoM']
            WEP_Now=perline['WEP_Now']
            Snow_Depth=perline['Snow_Depth']
            Snow_PRS=perline['Snow_PRS']
            sql='insert into cimiss (Station_Name,Province,City,Cnty,Town,Station_Id_d,Lat,Lon,Alti,PRS_Sensor_Alti,Station_Type,Station_levl,Year,Mon,Day,Hour,PRS,PRS_Sea,PRS_Max,PRS_Max_OTime,' \
                'PRS_Min,PRS_Min_OTime,TEM,TEM_Max,TEM_Max_OTime,TEM_Min,TEM_Min_OTime,DPT\
                    ,RHU,RHU_Min,RHU_Min_OTIME,VAP,PRE_1h,PRE_3h,PRE_6h,PRE_12h,PRE_24h,PRE,EVP_Big,WIN_D_Avg_10mi,WIN_S_Avg_10mi,WIN_D_S_Max,WIN_S_Max,WIN_S_Max_OTime,WIN_D_INST_Max,WIN_S_Inst_Max,' \
                'WIN_S_INST_Max_OTime,GST,GST_Max,GST_Max_Otime,GST_Min,GST_Min_OTime,VIS_Min,VIS_Min_OTime,VIS,CLO_Cov,CLO_Cov_Low,CLO_COV_LM,CLO_Height_LoM,WEP_Now,Snow_Depth,Snow_PRS' \
                ')VALUES("'+station_name+'","'+province+'","'+city+'","'+cnty+'","'+town+'",'+stationid+','+lat+','+lon+','+alti+','+PRS_Sensor_Alti+','+Station_Type+','+Station_levl+','+year+','+mon+','+day+','+hour+','+prs+','+PRS_Sea+','+PRS_Max+','+PRS_Max_OTime+','\
                +PRS_Min+','+PRS_Min_OTime+','+TEM+','+TEM_Max+','+TEM_Max_OTime+','+TEM_Min+','+TEM_Min_OTime+','+DPT+','\
                +RHU+','+RHU_Min+','+RHU_Min_OTIME+','+VAP+','+PRE_1h+','+PRE_3h+','+PRE_6h+','+PRE_12h+','+PRE_24h+','+PRE+','+EVP_Big+','+WIN_D_Avg_10mi+','+WIN_S_Avg_10mi+','+WIN_D_S_Max+','+WIN_S_Max+','+WIN_S_Max_OTime+','+WIN_D_INST_Max+','+WIN_S_Inst_Max+\
                ','+WIN_S_INST_Max_OTime+','+GST+','+GST_Max+','+GST_Max_Otime+','+GST_Min+','+GST_Min_OTime+','+VIS_Min+','+VIS_Min_OTime+','+VIS+','+CLO_Cov+','+CLO_Cov_Low+','+CLO_COV_LM+','+CLO_Height_LoM+','+WEP_Now+','+Snow_Depth+','+Snow_PRS+')'
            print sql
            cursor.execute(sql)
            db.commit()
db.close()
