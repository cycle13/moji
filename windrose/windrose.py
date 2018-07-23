#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/26
description:
"""
import xlrd
import matplotlib as mpl
from matplotlib import pyplot as plt
import windrose
from windrose import WindroseAxes
import matplotlib.cm as cm
mpl.rcParams['font.sans-serif'] = ['SimHei']

def new_axes():
    fig = plt.figure(figsize=(4, 4), dpi=80, facecolor='w', edgecolor='w')
    rect = [0, 0, 1, 1]
    ax = WindroseAxes(fig, rect, axisbg='w')
    fig.add_axes(ax)
    return ax
#...and adjust the legend box
def set_legend(ax):
    l = ax.legend(shadow=False, bbox_to_anchor=[1, 0])
    plt.setp(l.get_texts(), fontsize=12)

for sn in range(2):     #2个sheet中都有数据，一次绘制多个风玫瑰图
    workspace=(r"……工作空间")
    mybook=xlrd.open_workbook(workspace+r'……文件名')#打开文件
    mysheet=mybook.sheet_by_index(sn)
    rows=mysheet.nrows

    ws=mysheet.col_values(5)   #风速
    wd=mysheet.col_values(6)   #风向
    ws.pop(0)
    ws.pop(0)
    wd.pop(0)
    wd.pop(0)

    sl=[0,0.2,1.5,3.3,5.4,7.9]     #风速重分类间断点
    ax = new_axes()
    ax.contourf(wd,ws,bins=sl,normed=True,cmap=cm.cool) #使用matplotlib内置colormap进行色彩分割
    ax.set_title(mysheet.name,fontsize=15,loc='right')

    set_legend(ax)
    plt.show()