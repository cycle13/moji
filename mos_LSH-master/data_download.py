# encoding: utf-8
import pymysql
import numpy as np
import pandas as pd
import json
import os
import datetime
import time
import pygrib
import time
# import netcdf4
import torch
from torch.utils.data import DataLoader, Dataset
import os

#grib_paths = '/home/meteo/shihan.liu/grib_data'
grib_paths='/Users/yetao.lu/Desktop/mos/temp'

conn = pymysql.connect(host='172.16.8.28',
                       port=3307,
                       user='mojiro',
                       passwd='moji_RO_123',
                       db='moge',
                       charset='utf8')


def get_train_data(start_time, end_time, pl_grbs, sfc_grbs):
    time_diff = 3  # 时间间隔为3小时
    city_dic = get_city_lon_lat()
    timeTostr = lambda x: datetime.datetime.strftime(x, '%Y-%m-%d %H:%M:%S')
    sql = "SELECT A.v01000, A.v_PRE_1h, A.vdate FROM t_r_hourly_surf_live_ele as A WHERE A.vdate>='%s' AND A.vdate<='%s' AND A.v_PRE_1h != 999999" %\
          (timeTostr(start_time), timeTostr(end_time))
    print sql
    cur = conn.cursor()
    print '下载观测数据。。。'
    cur.execute(sql)
    print '下载完成!'
    data = cur.fetchall()
    #print data
    cols = ['station_id', 'v_rain', 'vdate']
    data_df = pd.DataFrame(list(data), columns=cols)
    #去除重复项
    data_df = data_df.drop_duplicates(['station_id', 'vdate'])
    #构建透视表
    data_df = pd.pivot_table(data_df, index=['vdate'], columns=['station_id'], values='v_rain')
    #print data_df
    data_df = data_df.dropna(axis=1)  # 删除有缺失的时间序列
    # 这里需要加上一个判断 确定最后的时间段
    # print data_df
    # 删除没有city_dict里面不包含的station_id
    lon_lats = []  # lon_lat 排列
    print 'data_df.columns:',data_df.columns
    for station in data_df.columns:
        if station in city_dic:
            lon_lats.append(list(map(float, city_dic[station])))
        else:
            if station in data_df.columns:
                data_df = data_df.drop(station, axis=1)
            else:
                continue
    #print lon_lats
    l = len(data_df.index)
    #print l
    labels = []
    indexs = []

    for i in range(1, l-time_diff, time_diff):
        j = i + 3
        labels.append(data_df.iloc[i:j, :].sum())
        indexs.append(data_df.index[i-1])
    #print 'labels',labels
    #print 'indexs',indexs
    label_df = pd.DataFrame(labels, index=indexs, columns=data_df.columns)
    #print 'label_df',label_df
    return read_grib(pl_grbs, sfc_grbs, lon_lats, label_df)


    # for station in data_df.columns:
    #     if station in city_dic:
    #         lon, lat = map(float, city_dic[station])
    #         data = read_grib(pl_grbs, lon, lat)


def get_city_lon_lat():  # lon经度    lat维度
    if True:
        print '下载站点对应经纬度文件。。。'
        cur = conn.cursor()
        sql = "SELECT A.v01000, A.lon, A.lat FROM station_moji_city as A"
        cur.execute(sql)
        data = cur.fetchall()
        print '下载完成!'
        city_dic = {}
        for line in data:
            if not line[0] in city_dic:
                city_dic[line[0]] = [line[1], line[2]]
            else:
                print '重复出现相同城市'
        with open('./city_dict.json', 'w') as f:
            json.dump(city_dic, f)
        return city_dic
    # else:
    #     with open('./city_dict.json', 'r') as f:
    #         city_dic = json.load(f)
    #     return city_dic


def latlons2index_Nearest(lon_lat):  # lon lat在前面要加上判定
    # ec数据的经纬度是N:10-60 E:60-160
    if len(lon_lat) != 2:
        raise ValueError('lon lat not match!')
    lon, lat = lon_lat
    if lon < 60 or lon > 160 or lat < 10 or lat > 60:
        raise ValueError('lon lat not in china! ')
    lon *= 10
    lon += 0.5
    lat *= 10
    lat += 0.5
    lon_nearst = int(lon)
    lat_nearst = int(lat)
    #print '=======',lon,lat,lon_nearst,lat_nearst
    lon_index = lon_nearst - 600
    lat_index = 600 - lat_nearst
    #print '::::::',lat_index,lon_index
    return [lat_index, lon_index]

def read_grib(pl_grbs, sfc_grbs, lon_lats, labels_df):  # 输入城市站点的经纬度来对应到当前站点的
    lon_lats_index = list(map(latlons2index_Nearest, lon_lats))

    pl_levels = [500, 700, 850, 925, 950, 1000]
    pl_names = ['Geopotential', 'Temperature', 'U component of wind', 'V component of wind', 'Specifc humidity',
             'Vertical velocity', 'Relative humidity']
    pl_feature_num = len(pl_levels) * len(pl_names)
    print len(pl_levels),len(pl_names),pl_feature_num
    pl_grb = pl_grbs.select()  # 得到了所有数据的一个列表
    print type(pl_grb),len(pl_grb),len(pl_grb)-pl_feature_num*16,
    data1 = []
    for p in range(len(pl_grb)):
        print pl_grb[p]
    for i in range(pl_feature_num, len(pl_grb)-pl_feature_num*16, pl_feature_num):  # 总共是读取48个时次
        time_values = []
        for j in range(pl_feature_num):
            img = pl_grb[i+j].values
            index_values = []
            for index in lon_lats_index:
                dy, dx = index
                index_values.append(img[dy, dx])
            time_values.append(index_values)
        data1.append(time_values)
    data1 = np.array(data1)
    print data1
    data1 = np.transpose(data1, (2, 0, 1))
    print data1
    # 去掉了累计降水和积雪和降水类型
    sfc_names = ['Sea surface temperature', 'Geopotential', 'Surface pressure', 'Convective precipitation',
                 'Mean sea level pressure', 'Total cloud cover', '10 metre U wind component',
                 '10 metre V wind component', '2 metre temperature', '2 metre dewpoint temperature',
                 'Low cloud cover', 'Medium cloud cover', 'High cloud cover', 'Skin temperature']

    sfc_grb_rain = sfc_grbs.select(name='Total precipitation')[:49]
    total_rain_values = []
    for rain in sfc_grb_rain:
        total_rain_values.append(rain.values)
    total_rain_values = np.array(total_rain_values)
    diff_values = []
    for j in range(len(total_rain_values)-1):
        diff_values.append(total_rain_values[j+1] - total_rain_values[j])
    diff_values = np.array(diff_values)
    rain_data = []
    for index in lon_lats_index:
        dy, dx = index
        rain_data.append(diff_values[:, dy, dx])
    rain_data = np.array(rain_data)
    rain_data = rain_data[..., np.newaxis]
    sfc_grb_other = sfc_grbs.select(name=sfc_names)
    sfc_feature_num = len(sfc_names)
    other_data = []
    for i in range(0, len(sfc_grb_other)-sfc_feature_num*17, sfc_feature_num):
        time_values = []
        for j in range(sfc_feature_num):
            img = sfc_grb_other[i+j].values
            index_values = []
            for index in lon_lats_index:
                dy, dx = index
                index_values.append(img[dy, dx])
            time_values.append(index_values)
        other_data.append(time_values)
    other_data = np.array(other_data)
    other_data = np.transpose(other_data, (2, 0, 1))
    data2 = np.concatenate((rain_data, other_data), axis=2)
    data = np.concatenate((data1, data2), axis=2)
    labels = labels_df.values
    labels = np.transpose(labels, (1, 0))
    labels = labels[..., np.newaxis]
    total_data = np.concatenate((data, labels), axis=2)
    print total_data.shape
    return total_data


if __name__ == '__main__':
    time_begin = '2016122512'
    time_end = '2016122512'
    time_begin = datetime.datetime.strptime(time_begin, '%Y%m%d%H')
    time_end = datetime.datetime.strptime(time_end, '%Y%m%d%H')
    day = 0
    data = None
    while time_begin <= time_end:

        time_begin_str = datetime.datetime.strftime(time_begin, '_%Y%m%d_%H')
        pl_inputfile = os.path.join(grib_paths, 'pl' + time_begin_str + '.grib')
        sfc_inputfile = os.path.join(grib_paths, 'sfc' + time_begin_str + '.grib')
        pl_grbs = pygrib.open(pl_inputfile)
        sfc_grbs = pygrib.open(sfc_inputfile)
        print sfc_grbs
        start_time_str = ''.join(pl_inputfile.split('.')[-2].split('_')[-2:])
        start_time = datetime.datetime.strptime(start_time_str, '%Y%m%d%H')
        end_time = start_time + datetime.timedelta(hours=145)  # mos统计三小时的有144个小时
        data_day = get_train_data(start_time, end_time, pl_grbs, sfc_grbs)
        print data_day
        if day == 0:
            data = data_day[:]
        else:
            data = np.concatenate((data, data_day), axis=0)
        time_begin = time_begin + datetime.timedelta(days=1)
        day += 1
        print data.shape
        print '已经完成%s天' % day

    np.save('data.npy', data)




