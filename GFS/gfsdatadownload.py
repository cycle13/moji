#!/usr/bin/env python
#################################################################
# Python Script to retrieve 1 online Data file of 'ds084.1',
# total 190.47M. This script uses 'requests' to download data.
#
# Highlight this script by Select All, Copy and Paste it into a file;
# make the file executable and run it on command line.
#
# You need pass in your password as a parameter to execute
# this script; or you can set an environment variable RDAPSWD
# if your Operating System supports it.
#
# Contact rpconroy@ucar.edu (Riley Conroy) for further assistance.
#################################################################


import sys, os,datetime
import requests
os.environ['RDAPSWD']='wangtian19880520'
def check_file_status(filepath, filesize):
    sys.stdout.write('\r')
    sys.stdout.flush()
    size = int(os.stat(filepath).st_size)
    percent_complete = (size/filesize)*100
    sys.stdout.write('%.3f %s' % (percent_complete, '% Completed'))
    sys.stdout.flush()

if len(sys.argv) < 2 and not 'RDAPSWD' in os.environ:
    print('Usage: ' + sys.argv[0] + ' wangtian19880520')
    exit(1)
else:
    try:
        pswd = sys.argv[1]
    except:
        pswd = os.environ['RDAPSWD']

url = 'https://rda.ucar.edu/cgi-bin/login'
values = {'email' : 'wtcindy_yxf@sina.cn', 'passwd' : 'wangtian19880520', 'action' : 'login'}
# Authenticate
ret = requests.post(url,data=values)
if ret.status_code != 200:
    print('Bad Authentication')
    print(ret.text)
    exit(1)
dspath = 'http://rda.ucar.edu/data/ds084.1/'
filelist001 = [
'2018/20180101/gfs.0p25.2018010100.f000.grib2']
filelist=[]
start='2015-11-01 00:00:00'
startime=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
for i in range(152):
    startime001 = startime + datetime.timedelta(days=i)
    for j in range(4):
        startime002=startime001+datetime.timedelta(hours=j*6)
        yearstring = str(startime002.year)
        datestring=datetime.datetime.strftime(startime002,'%Y%m%d')
        datetimestring=datetime.datetime.strftime(startime002,'%Y%m%d%H')
        for k in range(13):
            kk=k*3
            if kk<10:
                kkstring='00'+str(kk)
            elif kk>=10:
                kkstring='0'+str(kk)
            filestring=yearstring+'/'+datestring+'/'+'gfs.0p25.'+datetimestring+'.f'+kkstring+'.grib2'
            print(kk,filestring)
            filelist.append(filestring)
for file in filelist:
    filename=dspath+file
    outpath='/home/wlan_dev/gfs'
    file_base = os.path.basename(file)
    filepath=os.path.join(outpath,file)
    print('Downloading',file_base)
    req = requests.get(filename, cookies = ret.cookies, allow_redirects=True, stream=True)
    filesize = int(req.headers['Content-length'])
    if not os.path.exists(file_base):
        with open(file_base, 'wb') as outfile:
            chunk_size=1048576
            for chunk in req.iter_content(chunk_size=chunk_size):
                outfile.write(chunk)
                if chunk_size < filesize:
                    check_file_status(file_base, filesize)
        print()
