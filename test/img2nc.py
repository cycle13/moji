# -*- coding: utf-8 -*-
# @Time    : 2018/2/5 9:28
# @Author  : Luming
# @File    : main.py
# @Software: PyCharm

from osgeo import gdal
import numpy as np
import netCDF4 as nc

def write_to_nc_wanmei(data,GeoInformation,nCols,nRows,file_name_path):

    lonS = np.linspace(70, 140, nCols)#lift,right,xsize
    latS = np.linspace(0, 60, nRows)#button,top,ysize
    da = nc.Dataset(file_name_path, 'w', format='NETCDF4')
    da.createDimension('lon', nCols)  # 创建坐标点
    da.createDimension('lat', nRows)  # 创建坐标点
    da.createVariable("lon", 'f', ("lon"))  # 添加coordinates  'f'为数据类型，不可或缺
    da.createVariable("lat", 'f', ("lat"))  # 添加coordinates  'f'为数据类型，不可或缺
    da.variables['lat'][:] = latS  # 填充数据
    da.variables['lon'][:] = lonS  # 填充数据

    da.createVariable('u', 'f8', ('lat', 'lon'))  # 创建变量，shape=(627,652)  'f'为数据类型，不可或缺
    da.variables['u'][:] = data  # 填充数据
    da.close()


if __name__ == "__main__":
    dataset = gdal.Open("/Users/yetao.lu/Downloads/Z_AGME_C_BABJ_20171020000000_P_CAGMSS_CGRM_LARI-DVS_CHN_L88_PD_000_24.img")
    nCols = dataset.RasterXSize  # 栅格数据集的x方向像素数
    nRows = dataset.RasterYSize  # 栅格数据集的y方向像素数
    n_Bands = dataset.RasterCount   # 波段数
    m_GeoInformation = dataset.GetGeoTransform()
    Band = dataset.GetRasterBand(1)
    print Band, nCols, nRows, m_GeoInformation
    
    raster = np.zeros((1401, 1201), dtype=np.float32)
    dataset.GetRasterBand(1).WriteArray(raster)
    write_to_nc_wanmei(Band,m_GeoInformation,nCols,nRows, '/Users/yetao.lu/Downloads/new1.nc')