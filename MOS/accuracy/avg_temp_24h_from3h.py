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
    sql = 'select city_id,initial_time,avg(temperature),max(temperature),min(temperature),max(temp_max_6h),min(temp_min_6h) from t_r_ec_city_forecast_ele_mos where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '" group by city_id'
    #print sql
    cursor001.execute(sql)
    rows = cursor001.fetchall()
    if rows==():
        logger.info('该时间段没有数据' +sql)
        return
    #print len(rows)
    sql_live = 'select v01000,AVG(v_TEM),MAX(v_TEM_Max),MIN(v_TEM_Min) from t_r_hourly_surf_live_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and v_TEM<90 and v_TEM_Max<90 and v_TEM_Min<90 group by v01000;'
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
    # 3度的最高气温准确率：用逐小时最高气温最大值和预报最高气温计算
    na_mmax = 0
    # 3度的最低气温准确率用逐小时最低气温最小值和预报最低气温计算
    na_mmin = 0
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
            max_max = float(row[5])
            if abs(avg_mmax_live - max_max) < 3:
                na_mmax = na_mmax + 1
            min_min = float(row[6])
            if abs(avg_mmin_live - min_min) < 3:
                na_mmin = na_mmin + 1
    # 把一条SQL的值加载list中，然后把这个list合并到总的list中
    #print na_avg, na_max, na_min, na_mmax, na_mmin
    cursor = db.cursor()
    sql='replace into t_r_avg_temp_est_daily_mos_dem(initial_time,forecast_days,initial_year,initial_month,initial_day,initial_hour,NN,na_avg,na_max,na_min,na_mmax,na_mmin)VALUES ("'+initial+'","'+str(ii+1)+'","'+str(year002)+'","'+str(month002)+'","'+str(day002)+'","'+str(hour002)+'","'+str(nn)+'","'+str(na_avg)+'","'+str(na_max)+'","'+str(na_min)+'","'+str(na_mmax)+'","'+str(na_mmin)+'")'
    #sql = 'insert into t_r_avg_temp_est_daily_mos(initial_time,forecast_days,initial_year,initial_month,initial_day,initial_hour,NN,na_avg,na_max,na_min,na_mmax,na_mmin)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    #print sql
    cursor.execute(sql)
    db.commit()
def caculate_rainstate24h_from3h_accurity(db, initial, pdate, odate, ii,):
    #24小时里面：8个时次有一个是降水，则为预报有雨
    sql='select city_id,initial_time,MAX(rainstate) from t_r_ec_city_forecast_ele_mos where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '" group by city_id'
    #sql='select city_id,initial_time,MAX(rainstate) from t_r_ec_mos_city_forecast_ele where initial_time="2018-05-01 00:00:00" and forecast_time>"2018-05-01 00:00:00" and forecast_time<="2018-05-02 00:00:00" group by city_id'
    cursor_forecast=db.cursor()
    cursor_forecast.execute(sql)
    rows_f=cursor_forecast.fetchall()
    if rows_f==():
        logger.info('该时间段没有数据' +sql)
        return
    #24小时的逐小时降水量相加，大于999990的为实况有雨，=0.01的为微量降水，小于0.01的为无雨
    sql_live='select v01000,sum(v_PRE_1h) from t_r_hourly_surf_live_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and v_PRE_1h<9999 group by v01000;'
    cursor_live=db.cursor()
    cursor_live.execute(sql_live)
    rows_live=cursor_live.fetchall()
    #预报
    fraindict={}
    for row_f in rows_f:
        fraindict[row_f[0]]=int(row_f[2])
    #nn站点总数
    #na实况有雨预报有雨
    #nb实况无雨预报有雨
    #nc实况有雨预报无雨
    #nd实况无雨预报无雨
    nn=0
    na=0
    nb=0
    nc=0
    nd=0
    for row_live in rows_live:
        if float(row_live[1])==999999:
            continue
        else:
            nn = nn + 1
            if float(row_live[1])==999990:
                if fraindict[row_live[0]]==1:
                    na=na+1
                else:
                    nd=nd+1
            elif float(row_live[1])>=0.1 and float(row_live[1])<>999990 and float(row_live[1])<>999999:
                if fraindict[row_live[0]]==1:
                    na=na+1
                else:
                    nc=nc+1
            elif float(row_live[1])==0:
                if fraindict[row_live[0]]==1:
                    nb=nb+1
                else:
                    nd=nd+1
    pc=((na+nd)/nn)*100
    ts=(na/(na+nb+nc))*100
    f=(na+nb)*(nc+na)/nn
    ets=(na-f)/(na+nb+nc-f)*100
    bias=(na+nb)/(na+nc)
    pod=na/(na+nc)*100
    far=nb/(na+nb)*100
    mar=nc/(na+nc)*100
    initialtime = datetime.datetime.strptime(initial, '%Y-%m-%d %H:%M:%S')
    year001 = initialtime.year
    month001 = initialtime.month
    day001 = initialtime.day
    hour001 = initialtime.hour
    insql='replace into t_r_sun_rain_accu_daily_mos_dem(initial_time,forecast_days,initial_year,initial_month,initial_day,initial_hour,NA,NN,NB,NC,ND,pc,ts,f,ets,bias,pod,far,mar)VALUES ("'+initial+'","'+str(ii+1)+'","'+str(year001)+'","'+str(month001)+'","'+str(day001)+'","'+str(hour001)+'","'+str(na)+'","'+str(nn)+'","'+str(nb)+'","'+str(nc)+'","'+str(nd)+'","'+str(pc)+'","'+str(ts)+'","'+str(f)+'","'+str(ets)+'","'+str(bias)+'","'+str(pod)+'","'+str(far)+'","'+str(mar)+'")'
    cursor_rain=db.cursor()
    cursor_rain.execute(insql)
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
    sql_live = 'select v01000,AVG(v_TEM) from t_r_hourly_surf_live_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and v_TEM <80 group by v01000;'
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
    sql_into='replace into t_r_3h_temp_accu_mos_dem(initial_time,forecast_hours,temp_accu,temp_mae,temp_rmse)VALUES ("'+initial+'","'+str(ii)+'","'+str(accu)+'","'+str(mae)+'","'+str(rmse)+'")'
    print sql_into
    logger.info(sql_into)
    cursor=db.cursor()
    cursor.execute(sql_into)
    db.commit()
def calculate_rain3h_accurity(db,initial,pdate,odate,ii,logger):
    #取训练的72小时的逐3小时晴雨和降水量
    sql_rain3h='select city_id,initial_time,rainstate,precipitation from t_r_ec_city_forecast_ele_mos where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '" '
    print sql_rain3h
    #sql='select city_id,initial_time,rainstate,precipitation from t_r_ec_mos_city_forecast_ele where initial_time="2018-05-01 00:00:00" and forecast_time>"2018-05-01 00:00:00" and forecast_time<="2018-05-02 00:00:00" '
    cursor_forecast=db.cursor()
    cursor_forecast.execute(sql_rain3h)
    rows_f=cursor_forecast.fetchall()
    if rows_f==():
        logger.info('该时间段没有数据' +sql_rain3h)
        return
    #3小时的逐小时降水量相加，大于999990的为实况有雨，=0.01的为微量降水，小于0.01的为无雨
    sql_live='select v01000,sum(v_PRE_1h) from t_r_hourly_surf_live_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and v_PRE_1h<9999 group by v01000;'
    print sql_live
    cursor_live=db.cursor()
    cursor_live.execute(sql_live)
    rows_live=cursor_live.fetchall()
    #晴雨
    fraindict={}
    #降水量
    raindict={}
    for row_f in rows_f:
        fraindict[row_f[0]]=int(row_f[2])
        #raindict[row_f[0]]=float(row_f[3])
    nn=0
    na=0
    nb=0
    nc=0
    nd=0
    for row_live in rows_live:
        if float(row_live[1])==999999:
            continue
        else:
            nn = nn + 1
            if float(row_live[1])==999990:
                if fraindict[row_live[0]]==1:
                    na=na+1
                else:
                    nd=nd+1
            #实况有雨
            elif float(row_live[1])>=0.1 and float(row_live[1])<>999990 and float(row_live[1])<>999999:
                #预报有雨
                if fraindict[row_live[0]]==1:
                    na=na+1
                #预报无雨
                else:
                    nc=nc+1
            #实况无雨
            elif float(row_live[1])==0:
                #预报有雨
                if fraindict[row_live[0]]==1:
                    nb=nb+1
                #预报无雨
                else:
                    nd=nd+1
            print row_live[1],fraindict[row_live[0]],na,nb,nc,nd,nn
    accu=float(na+nd)*100/float(nn)
    sql='replace into t_r_3h_rain_accu_mos_dem(initial_time,forecast_hours,rain_accu)VALUES ("'+initial+'","'+str(ii)+'","'+str(accu)+'")'
    #print sql
    cursor=db.cursor()
    cursor.execute(sql)
    db.commit()
'''
计算6小时的最高气温最低气温指标
'''
def calculate_maxmintemp6h_accuracy(db, initial, pdate, odate, u):
    #预报的6h最高气温最低气温
    cursor001 = db.cursor()
    sql_mm6h = 'select city_id,initial_time,temp_max_6h,temp_min_6h from t_r_ec_city_forecast_ele_mos where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '" '
    print sql_mm6h
    cursor001.execute(sql_mm6h)
    rows_fore = cursor001.fetchall()
    if rows_fore==():
        logger.info('该时间段没有数据'+sql_mm6h)
        return
    #实况的6小时里逐小时最高气温最大值和最低气温最小值
    sql_live = 'select v01000,MAX(v_TEM_Max),MIN(v_TEM_Min) from t_r_hourly_surf_live_ele where vdate>"' + pdate + '" and vdate<="' + odate + '" and v_TEM_Max<80 and v_TEM_Min<80 group by v01000;'
    print sql_live
    cursor = db.cursor()
    cursor.execute(sql_live)
    rows_live = cursor.fetchall()
    print len(rows_live)
    nn=0
    na_max=0
    na_min=0
    fore_dict={}
    #预报存入字典
    for row_f in rows_fore:
        fore_dict[row_f[0]]=row_f
    #判断站点是否在预报字典中
    for row_live in rows_live:
        if row_live[0] in fore_dict.keys():
            nn=nn+1
            print row_live[1],row_live[2],fore_dict[row_live[0]][2],fore_dict[row_live[0]][3],fore_dict[row_live[0]]
            max_live=float(row_live[1])
            min_live=float(row_live[2])
            if fore_dict[row_live[0]][2]==None or fore_dict[row_live[0]][2]==None:
                continue
            max_fore=float(fore_dict[row_live[0]][2])
            min_fore=float(fore_dict[row_live[0]][3])
            print max_live,max_fore,min_live,min_fore
            if abs(max_live-max_fore)<=3:
                na_max=na_max+1
            if abs(min_live-min_fore)<=3:
                na_min=na_min+1
    if nn==0:
        return
    accuracy_max=float(na_max)*100/float(nn)
    accuracy_min=float(na_min)*100/float(nn)
    #print na_max,na_min,nn
    sql_mm='replace into t_r_max_min_temp_accu_mos_dem(initial_time,forecast_hours,max_temp_accu,min_temp_accu)VALUES ("'+initial+'","'+str(u)+'","'+str(accuracy_max)+'","'+str(accuracy_min)+'")'
    #print sql_mm
    cursor_mm=db.cursor()
    cursor_mm.execute(sql_mm)
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
    initial = '2018-05-01 00:00:00'
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
            caculate_rainstate24h_from3h_accurity(db, initial, pdate, odate, n)
        for u in range(24):
            pdatetime=initialtime001+datetime.timedelta(hours=3*u)
            pdate = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
            odatetime=initialtime001+datetime.timedelta(hours=3*(u+1))
            odate = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
            calculate_temp3h_accurity(db, initial, pdate, odate, (u+1)*3,logger)
            calculate_rain3h_accurity(db, initial, pdate, odate, (u+1)*3,logger)
        for e in range(12):
            pdatetime=initialtime001+datetime.timedelta(hours=6*e)
            pdate = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
            odatetime=initialtime001+datetime.timedelta(hours=6*(e+1))
            odate = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
            calculate_maxmintemp6h_accuracy(db, initial, pdate, odate, (e+1)*6)
        db.close()
    endtime=datetime.datetime.now()
    logger.info((endtime-starttime).seconds)
