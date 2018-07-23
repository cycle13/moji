#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/4/9
description:解析延庆J文件格式
"""
import os,datetime,MySQLdb,multiprocessing,threading,threadpool
#filename='/Users/yetao.lu/Desktop/冬奥会/data2/WY20180402/延庆站/J54406-200605.TXT'
def ReadJfile(filename):
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor=db.cursor()
    if filename[-3:] == 'TXT':
        fr = open(filename, 'r')
        firstline=fr.readline()
        #print firstline
        firstarray=firstline.split(' ')
        #print len(firstarray)
        stationid=firstarray[0]
        lat=float(firstarray[1][:4])*0.01
        lon=float(firstarray[2][:5])*0.01
        yearstr=int(firstarray[9])
        monstr=int(firstarray[10])
        #print yearstr,monstr
        pdatetime=datetime.datetime(yearstr,monstr,1,0,0,0)
        #print pdatetime
        other=fr.read()
        dataarray=other.split('=')
        #print dataarray[0],len(dataarray)
        plist=[]
        tlist=[]
        ulist=[]
        rlist=[]
        hlist=[]
        flist=[]
        wslist=[]
        wdlist=[]
        #遍历6个要素
        print len(dataarray),'--------'
        for k in range(len(dataarray)):
            featurearray=dataarray[k].split('\r\n')
            #print len(featurearray),featurearray
            if featurearray[0]=='P0':
                for j in range(1,len(featurearray)):
                    minvalue=featurearray[j].split()
                    plist.append(minvalue)
            elif featurearray[1]=='T0':
                for j in range(2,len(featurearray)):
                    minvalue=featurearray[j].split()
                    tlist.append(minvalue)
            elif featurearray[1]=='U0':
                for j in range(2,len(featurearray)):
                    minvalue=featurearray[j].split()
                    ulist.append(minvalue)
            elif featurearray[1]=='R0':
                for j in range(2,len(featurearray)):
                    minvalue=featurearray[j].split()
                    rlist.append(minvalue)
            elif featurearray[1]=='F0':
                for j in range(2,len(featurearray)):
                    minvalue=featurearray[j].split()
                    flist.append(minvalue)
        #print len(plist),len(tlist),len(ulist),len(rlist),len(flist)
        #print tlist
        for i in range(len(plist)):
            for j in range(60):
                endtimes=pdatetime+datetime.timedelta(minutes=i*60+j)
                timestring=datetime.datetime.strftime(endtimes,'%Y-%m-%d %H:%M:%S')
                if (plist[i][j] == '/////' or plist[i][j] == '/////,'):
                    prsvalue = 999999
                elif(len(plist[i][j])>5 or plist[i][j].find('/')==-1 or plist[i][j]!='/////' or plist[i][j]!='/////,'):
                    #print plist[i][j],type(plist[i][j])
                    plist[i][j]=plist[i][j][0:5]
                    prsvalue = float(plist[i][j]) * 0.1
                else:
                    prsvalue=float(plist[i][j])*0.1
                if(len(ulist[i][j])>2):
                    ulist[i][j]=ulist[i][j][0:2]
                    if (ulist[i][j] == '//' or ulist[i][j]=='%%'):
                        uvalue = 999999
                    else:
                        uvalue = float(ulist[i][j])
                elif(ulist[i][j]=='//' or ulist[i][j]=='%%'):
                    uvalue=999999
                else:
                    uvalue=float(ulist[i][j])
                if len(tlist[i][j])>4:
                    tlist[i][j]=tlist[i][j][:-1]
                if(tlist[i][j]=='////' or tlist[i][j].find('/')!=-1):
                    tvalue = 999999
                elif(tlist[i][j].find('-')!=-1):
                    #print tlist[i][j][0:1]
                    tvalue=-float(tlist[i][j][1:4])*0.1
                else:
                    tvalue=float(tlist[i][j][1:4])*0.1
                if(flist[i][j]=='//////' or flist[i][j]=='//////,'):
                    wsvalue=999999
                    wdvalue=999999
                else:
                    print flist[i][j],flist[i][j][0:3],flist[i][j][3:6]
                    wdvalue=int(flist[i][j][0:3])
                    wsvalue=float(flist[i][j][3:6])*0.1
                #print prsvalue,uvalue,tvalue,wsvalue,wdvalue
                year=endtimes.year
                mon=endtimes.month
                days=endtimes.day
                hours=endtimes.hour
                minute=endtimes.minute
                sql='insert into yanqingJ (times,prs,u,t,ws,wd,year,mon,days,hours,minute,stationid)VALUES("'+timestring+'","'+str(prsvalue)+'","'+str(uvalue)+'","'+str(tvalue)+'","'+str(wsvalue)+'","'+str(wdvalue)+'","'+str(year)+'","'+str(mon)+'","'+str(days)+'","'+str(hours)+'","'+str(minute)+'","'+str(stationid)+'")'
                print sql
                cursor.execute(sql)
                db.commit()
    db.close()
# if __name__ == "__main__":
#     filepath='/Users/yetao.lu/Desktop/冬奥会/data2/WY20180402/延庆站'
#     p = multiprocessing.Pool(processes=4)
#     for root,dirs,files in os.walk(filepath):
#         for file in files:
#             filename=os.path.join(root,file)
#             p.apply_async(ReadJfile,(filename,))
#         p.close()
#         p.join()
#多线程
if __name__ == "__main__":
    filepath='/Users/yetao.lu/Desktop/冬奥会/data2/WY20180402/延庆站'
    pool = threadpool.ThreadPool(40)
    listname=[]
    for root,dirs,files in os.walk(filepath):
        for file in files:
            filename=os.path.join(root,file)
            listname.append(filename)
    requests=threadpool.makeRequests(ReadJfile,listname)
    [pool.putRequest(req) for req in requests]
    pool.wait()

# if __name__ == "__main__":
#     filepath='/Users/yetao.lu/Desktop/冬奥会/data2/WY20180402/延庆站'
#     #filepath='/Users/yetao.lu/2017'
#     for root,dirs,files in os.walk(filepath):
#         for file in files:
#             filename=os.path.join(root,file)
#             ReadJfile(filename)
            
            
