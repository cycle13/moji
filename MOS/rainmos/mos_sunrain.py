#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/28
description:
"""
import pandas,pymysql,numpy,logging.handlers,sys,os,datetime,pygrib
class mos_sunrain_pytorch():
    def __init__(self,logger):
        self.logger=logger
    def data_process(self):
        timedifferent=3
    #从数据库中获取所有站点的站号经度纬度和高程。
    def station_lat_lon(self):
        db = pymysql.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                             3307)
        cursor = db.cursor()
        sql="select v01000,v05001,v06001,v07001 from t_p_station_cod where v_valid='1'"
        cursor.execute(sql)
        pdata=cursor.fetchall()
        pstationlist=numpy.array(pdata)
        #print pstationlist,len(pstationlist)
        db.commit()
        db.close()
        latlon_dict={}
        height_dict={}
        for i in range(len(pstationlist)):
            stationid=pstationlist[i][0]
            latlon_dict[stationid]=[pstationlist[i][1],pstationlist[i][2]]
            height_dict[stationid]=pstationlist[i][3]
        return latlon_dict,height_dict
    def get16raster(self,index_lat,index_lon,variable_value):
        train_data=[]
        train_data.append(variable_value[:,index_lat,index_lon])
        train_data.append(variable_value[:, index_lat, index_lon+1])
        train_data.append(variable_value[:, index_lat+1, index_lon+1])
        train_data.append(variable_value[:, index_lat+1, index_lon])
        train_data.append(variable_value[:, index_lat-1, index_lon-1])
        train_data.append(variable_value[:, index_lat-1, index_lon])
        train_data.append(variable_value[:, index_lat-1, index_lon+1])
        train_data.append(variable_value[:, index_lat-1, index_lon+2])
        train_data.append(variable_value[:, index_lat, index_lon+2])
        train_data.append(variable_value[:, index_lat+1, index_lon+2])
        train_data.append(variable_value[:, index_lat+2, index_lon+2])
        train_data.append(variable_value[:, index_lat+2, index_lon+1])
        train_data.append(variable_value[:, index_lat+2, index_lon])
        train_data.append(variable_value[:, index_lat+2, index_lon-1])
        train_data.append(variable_value[:, index_lat+1, index_lon-1])
        train_data.append(variable_value[:, index_lat, index_lon-1])
        return train_data
    #3小时模型训练，预报时间周期为48个时次。
    def process_station_live(self,start_time,end_time,time_diff):
        latlondict,heightdict=self.station_lat_lon()
        db = pymysql.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                             3307)
        cursor = db.cursor()
        timetostring=lambda x:datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S')
        sql_live="SELECT A.v01000, A.v_PRE_1h, A.vdate FROM t_r_hourly_surf_live_ele as A WHERE A.vdate>='%s' AND A.vdate<='%s' and v_PRE_1h<>'999999'"%(timetostring(start_time),timetostring(end_time))
        print sql_live
        cursor.execute(sql_live)
        data_live=cursor.fetchall()
        cols=['stationid','pre','vdate']
        data_df=pandas.DataFrame(list(data_live),columns=cols)
        #去除掉重复项，
        data_df=data_df.drop_duplicates(['stationid','vdate'])
        #构建透视表
        data_table=pandas.pivot_table(data_df,index=['vdate'],columns=['stationid'],values='pre')
        #print data_table
        #删除掉空值nan
        #data_table=data_table.dropna(axis=1)
        #填充空值:纵向上用缺失值上面的值替换缺失值
        data_table=data_table.fillna(axis=0,method='ffill')
        #填充完后，如果还有缺失值，填充0
        data_table=data_table.fillna(0)
        #按照透视表列（站号）的顺序排列站点的经度纬度
        latlonlist=[]
        heightlist=[]
        for station in data_table.columns:
            #判断站号是否在数据库读取的站点字典里,不在的话删除透视表中的列
            if station in latlondict.keys():
                #print station,latlondict[station],heightdict[station]
                #批量转换数据类型
                latlonlist.append(list(map(float,latlondict[station])))
                heightlist.append(float(heightdict[station]))
            else:
                data_table.drop(station,axis=1)
        #labels存储降水
        labels=[]
        indexs=[]
        #合并3小时的降水,每播数据是48个时次，共计预报144个小时
        for i in range(0,len(data_table.index)-time_diff,time_diff):
            j=i+time_diff
            labels.append(data_table.iloc[i:j,:].sum())
            #索引用
            indexs.append(data_table.index[j])
        label_dateframe=pandas.DataFrame(labels,index=indexs,columns=data_table.columns)
        #print label_dateframe
        #根据实况、经纬度来读取EC预报数据
        return label_dateframe,latlonlist,heightlist
    #以一次EC预报作为一个样本，即每次预报的48个时次的逐3小时为一个样本，(当然是一个站点)
    def ecmwf_grib_for_train(self,sfc_file,pl_file,start_time,end_time,time_diff,cellsize):
        labels,latlonlist,heightlist=self.process_station_live(start_time,end_time,time_diff)
        print datetime.datetime.now()
        sfc_gribs=pygrib.open(sfc_file)
        pl_gribs=pygrib.open(pl_file)
        print sfc_file,pl_file
        #sfc_gbs=sfc_gribs.select()
        #print sfc_gbs
        # for grb in sfc_gbs:
        #     print grb
        #筛选因子，先取最邻近点进行测试，后面有时间再测试16个点
        sfc_variables=['10 metre U wind component','10 metre V wind component','2 metre temperature','2 metre dewpoint temperature','Low cloud cover','Medium cloud cover','High cloud cover','Total cloud cover','Mean sea level pressure','Sea surface temperature','Geopotential','Surface pressure']
        #一个是总降水，一个是对流层降水，两个要素都是累计变量。
        #sfc_features=['Total precipitation','Convective precipitation']
        tp_grbs = sfc_gribs.select(name='Total precipitation')[:49]
        cp_grbs=sfc_gribs.select(name='Convective precipitation')[:49]
        #都是累计降水，后一个时次减去前一个时次，改为非累计降水
        tp_values=[]
        cp_values=[]
        print len(tp_grbs)
        for tp_grb in tp_grbs:
            tp_values.append(tp_grb.values)
        for cp_grb in cp_grbs:
            cp_values.append(cp_grb.values)
        tp_values=numpy.array(tp_values)
        cp_values=numpy.array(cp_values)
        tp_per_values=[]
        cp_per_values=[]
        tp_rain_values=[]
        cp_rain_values=[]
        for i in range(0,len(tp_values)-1,1):
            tp_per_values=tp_values[i+1]-tp_values[i]
            cp_per_values=cp_values[i+1]-cp_values[i]
            tp_rain_values.append(tp_per_values)
            cp_rain_values.append(cp_per_values)
        tp_rain_values=numpy.array(tp_rain_values)
        cp_rain_values=numpy.array(cp_rain_values)
        tp_traindata=[]
        cp_traindata=[]
        #降水要素：总降水和对流层降水的16个格点值
        for latlon in latlonlist:
            #print latlon
            lat,lon=latlon
            index_lat=int((60-lat)/cellsize)
            index_lon=int((lon-60)/cellsize)
            tp_data=self.get16raster(index_lat,index_lon,tp_rain_values)
            cp_data=self.get16raster(index_lat,index_lon,cp_rain_values)
            #print lat,lon,index_lat,index_lon,len(tp_data),len(cp_data)
            tp_traindata.append(tp_data)
            cp_traindata.append(cp_data)
        tp_traindata=numpy.array(tp_traindata)
        cp_traindata=numpy.array(cp_traindata)
        #print 'tp_traindata.shape:',tp_traindata.shape,cp_traindata.shape
        #其他要素的样本取值
        other_data=[]
        for j in range(len(sfc_variables)):
            variable=sfc_variables[j]
            #每个要素共65个时次，取逐3小时的48个时次
            var_values=[]
            grbs=sfc_gribs.select(name=variable)[1:49]
            #每个要素的值矩阵
            for grb in grbs:
                grb_value=grb.values
                var_values.append(grb_value)
            var_values=numpy.array(var_values)
            #print 'var_values:',var_values.shape
            #所有站点的EC的值的矩阵。
            stations_data=[]
            for latlon in latlonlist:
                lat,lon=latlon
                index_lat = int((60 - lat) / cellsize)
                index_lon = int((lon - 60) / cellsize)
                t_data=self.get16raster(index_lat,index_lon,var_values)
                stations_data.append(t_data)
            stations_data=numpy.array(stations_data)
            print stations_data.shape
            other_data.append(stations_data)
            #对要素进行列相加合并
        other_data=numpy.array(other_data)
        print other_data.shape,tp_traindata.shape,cp_traindata.shape
        train001=numpy.concatenate((other_data[:]),axis=1)
        train_sfc=numpy.concatenate((tp_traindata,cp_traindata,train001),axis=1)
        print train001.shape,train_sfc.shape
        #pl高空层的数据
        pl_levels=['500','700','850']
        pl_variables=['Relative humidity','U component of wind','V component of wind','Geopotential','Temperature']
        for grb in pl_gribs:
            print grb
        #单独取降水。后期验证用
        rain_data=[]
        #根据经度纬度索引用最邻近法取降水值
        for latlon in latlonlist:
            lat,lon=latlon
            indexlat=int((60+cellsize/2-lat)/cellsize)
            indexlon=int((lon+cellsize/2-60)/cellsize)
            rain_data.append(tp_rain_values[:,indexlat,indexlon])
        rain_data=numpy.array(rain_data)
        
            
        

    def get_ecfile(self,path,start_time,end_time,time_diff):
        for root,dirs,files in os.walk(path):
            for file in files:
                if file[:3]=='sfc':
                    sfc_file=os.path.join(root,file)
                    pl_filename=file.replace('sfc','pl')
                    pl_file=os.path.join(root,pl_filename)
                    if os.path.exists(pl_file):
                        self.ecmwf_grib_for_train(sfc_file,pl_file,start_time,end_time,time_diff,0.1)
if __name__ == "__main__":
    #加日志
    logger = logging.getLogger('learing.logger')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    file_handler = logging.FileHandler("/Users/yetao.lu/Desktop/mos/tmp/learning.log")
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值

    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    db = pymysql.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor = db.cursor()
    mosrain=mos_sunrain_pytorch(logger)
    start='2017-01-01 12:00:00'
    end='2017-01-10 12:00:00'
    start_time=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    end_time=datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
    #mosrain.process_station_live(start_time,end_time,3)
    path='/Users/yetao.lu/Desktop/mos/temp'
    time_diff=3
    print datetime.datetime.now()
    mosrain.get_ecfile(path,start_time,end_time,time_diff)
    print datetime.datetime.now()
    