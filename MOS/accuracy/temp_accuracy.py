# !/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/6/21
description:
"""
import MySQLdb, datetime,math,logging,sys
'''
#计算24小时平均气温准确率，预报为逐3小时，取8个时次的平均值，
时次是取得逐小时的24个时次的平均值
计算24小时最高气温最低气温准确率
'''
def calculate_avg_temp_24h_from3h_accurity(db, initial, pdate, odate, ii):
    initialtime = datetime.datetime.strptime(initial, '%Y-%m-%d %H:%M:%S')
    year002 = initialtime.year
    month002 = initialtime.month
    day002 = initialtime.day
    hour002 = initialtime.hour
    cursor001 = db.cursor()
    #选择MOS统计之后的24小时气温、24小时气温最大值，24小时气温最小值
    sql = 'select city_id,initial_time,avg(temperature),max(temperature),min(temperature) from t_r_ec_city_forecast_ele_mos where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '" group by city_id'
    #print sql
    cursor001.execute(sql)
    rows = cursor001.fetchall()
    if rows==():
        logger.info('该时间段没有数据' +sql)
        return
    #print len(rows)
    sql_live = 'select v01000,AVG(TEM),MAX(TEM_Max),MIN(TEM_Min) from t_r_surf_hour_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and TEM<90 and TEM_Max<90 and TEM_Min<90 group by v01000;'
    #print sql_live
    cursor = db.cursor()
    cursor.execute(sql_live)
    rows_live = cursor.fetchall()
    #print len(rows_live)
    rowdict = {}
    nn = 0
    # 3度的平均气温准确率
    na_avg = 0
    # 3度的气温最大值准确率:用逐小时的气温最大值和预报最高气温计算
    na_max = 0
    # 3度的气温最小值准确率：用逐小时的气温最小值和预报最低气温计算
    na_min = 0
    #实况数据存入字典
    for row_live in rows_live:
        rowdict[row_live[0]] = row_live
    #预报站点在实况字典里，计算accu
    for row in rows:
        if row[0] in rowdict.keys():
            nn = nn + 1
            #print rowdict[row[0]]
            avg_temp_live = float(rowdict[row[0]][1])
            avg_temp = float(row[2])
            if abs(avg_temp_live - avg_temp) < 3:
                na_avg = na_avg + 1
            avg_mmax_live = float(rowdict[row[0]][2])
            max_temp = float(row[3])
            if abs(avg_mmax_live - max_temp) < 3:
                na_max = na_max + 1
            avg_mmin_live = float(rowdict[row[0]][3])
            min_temp = float(row[4])
            if abs(avg_mmin_live - min_temp) < 3:
                na_min = na_min + 1
    # 把一条SQL的值加载list中，然后把这个list合并到总的list中
    #print na_avg, na_max, na_min, na_mmax, na_mmin
    cursor = db.cursor()
    sql='replace into t_r_avg_temp_est_daily_mos(initial_time,forecast_days,initial_year,initial_month,initial_day,initial_hour,NN,na_avg,na_max,na_min)VALUES ("'+initial+'","'+str(ii+1)+'","'+str(year002)+'","'+str(month002)+'","'+str(day002)+'","'+str(hour002)+'","'+str(nn)+'","'+str(na_avg)+'","'+str(na_max)+'","'+str(na_min)+'")'
    #sql = 'insert into t_r_avg_temp_est_daily_mos(initial_time,forecast_days,initial_year,initial_month,initial_day,initial_hour,NN,na_avg,na_max,na_min,na_mmax,na_mmin)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    #print sql
    cursor.execute(sql)
    db.commit()

def calculate_temp3h_accurity(db,initial,pdate,odate,ii,logger):
    #72小时气温的准确率
    cursor001 = db.cursor()
    sql = 'select city_id,initial_time,temperature from t_r_ec_city_forecast_ele_mos where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '"'
    print sql
    cursor001.execute(sql)
    rows_f=cursor001.fetchall()
    #print rows_f,type(rows_f)
    if rows_f==():
        logger.info('该时间段没有数据' +sql)
        return
    #取逐小时的共3h的均值
    sql_live = 'select v01000,AVG(TEM) from t_r_surf_hour_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and TEM <80 group by v01000;'
    print sql_live
    cursor=db.cursor()
    cursor.execute(sql_live)
    rows_l=cursor.fetchall()
    dict_live={}
    nn=0
    na=0
    sum_mae=0
    sum_rmse=0
    #3h的气温存入字典
    for row in rows_l:
        dict_live[row[0]]=float(row[1])
    #判断预报站号在实况字典中，计算准确率
    for row_f in rows_f:
        if row_f[0] in dict_live.keys():
            nn=nn+1
            livetemp=float(dict_live[row_f[0]])
            foretemp=float(row_f[2])
            if abs(livetemp-foretemp)<3:
                na=na+1
            sum_mae=sum_mae+abs(livetemp-foretemp)
            sum_rmse=sum_rmse+pow(livetemp-foretemp,2)
    mae=sum_mae/nn
    rmse=math.sqrt(sum_rmse/nn)
    accu=na*100/nn
    sql_into='replace into t_r_3h_temp_accu_mos(initial_time,forecast_hours,temp_accu,temp_mae,temp_rmse)VALUES ("'+initial+'","'+str(ii)+'","'+str(accu)+'","'+str(mae)+'","'+str(rmse)+'")'
    print sql_into
    logger.info(sql_into)
    cursor=db.cursor()
    cursor.execute(sql_into)
    db.commit()

if __name__ == "__main__":
    starttime=datetime.datetime.now()
    #加日志
    logger = logging.getLogger('learing.logger')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    file_handler = logging.FileHandler("/home/wlan_dev/learning.log")
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值

    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    initial = '2018-07-01 00:00:00'
    initialtime = datetime.datetime.strptime(initial, '%Y-%m-%d %H:%M:%S')
    for p in range(62):
        initialtime001=initialtime+datetime.timedelta(hours=12*p)
        initial=datetime.datetime.strftime(initialtime001, '%Y-%m-%d %H:%M:%S')
        logger.info(initial)
        db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
        #db = MySQLdb.connect('192.168.10.84', 'admin', 'moji_China_123', 'moge')
        for n in range(10):
            print n
            pdatetime = initialtime001 + datetime.timedelta(days=n)
            pdate = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
            odatetime = initialtime001 + datetime.timedelta(days=n + 1)
            odate = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
            calculate_avg_temp_24h_from3h_accurity(db, initial, pdate, odate, n)
        for u in range(24):
            pdatetime=initialtime001+datetime.timedelta(hours=3*u)
            pdate = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
            odatetime=initialtime001+datetime.timedelta(hours=3*(u+1))
            odate = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
            calculate_temp3h_accurity(db, initial, pdate, odate, (u+1)*3,logger)
        db.close()
    endtime=datetime.datetime.now()
    logger.info((endtime-starttime).seconds)
