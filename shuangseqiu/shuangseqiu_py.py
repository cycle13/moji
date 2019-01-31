#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/18
description:
"""

# -*- coding:utf-8 -*-
import re
import urllib
import time
import sys

datapath = sys.path[0]
print datapath
datasuffix = 'txt'
if (len(sys.argv)>1):
    datapath = sys.argv[1]
    datasuffix = sys.argv[2]

def getHtml(url):
    html = urllib.urlopen(url)
    return html.read()


html = getHtml("http://zx.500.com/ssq/")


reg =  ['<dt>([0-9]\d*).*</dt>']
reg.append('<li class="redball">([0-9]\d*)</li>')
reg.append('<li class="blueball">([0-9]\d*)</li>')

outstr = "";
for i in range(len(reg)):
    page = re.compile(reg[i])
    rs = re.findall(page,html)
    for j in range(len(rs)):
        outstr+= rs[j] + ","

#print time.strftime('%Y-%m-%d',time.localtime(time.time()))+":"+outstr[:-1]

with open(datapath+'/lot_500_ssq.'+datasuffix, 'a') as f:
    f.write(time.strftime('%Y-%m-%d',time.localtime(time.time()))+":"+outstr[:-1]+'\n')
