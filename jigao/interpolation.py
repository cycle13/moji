#!/usr/local/moji/python/bin/python
# encoding:utf-8

"""
@Version:V0.1
@Author: Chunrui Guo
@Email:chunrui.guo@moji.com
@File:interpolation.py
@Time:2016/6/2 16:19
"""
import math


class Interpolation:
    """
    运用反距离平方加权的方法将格点数据插值到城市站点
    """
    def __init__(self):
        pass

    @staticmethod
    def get_nearest_point(_city_lon, _city_lat, _grid_longs, _grid_lats, _nx, _ny, _dx, _dy):
        r = math.sqrt(_dx*_dx + _dy*_dy)
        for i in range(_ny-2, -1, -1):
            for j in range(0, _nx-1):
                if _grid_longs[i+1][j+1] >= _city_lon >= _grid_longs[i][j] \
                        and _city_lat >= _grid_lats[i][j]:  # 找到城市位置所处的网格
                    # 计算城市位置附近四个格点与城市之间的距离
                    r1 = Interpolation.get_spherical_distance(_city_lat, _city_lon, _grid_lats[i][j], _grid_longs[i][j])
                    r2 = Interpolation.get_spherical_distance(_city_lat, _city_lon, _grid_lats[i+1][j], _grid_longs[i+1][j])
                    r3 = Interpolation.get_spherical_distance(_city_lat, _city_lon, _grid_lats[i][j+1], _grid_longs[i][j+1])
                    r4 = Interpolation.get_spherical_distance(_city_lat, _city_lon, _grid_lats[i+1][j+1], _grid_longs[i+1][j+1])
                    # 计算距离平方和
                    cq = (r - r1) ** 2 + (r - r2) ** 2 + (r - r3) ** 2 + (r - r4) ** 2
                    cq = 1.0 / cq
                    return i, j, r1, r2, r3, r4, cq, r

    @staticmethod
    def interpolate_grid_to_city(_var_grid, _mode, *_nearest_point_info):
        i, j, r1, r2, r3, r4, cq, r = _nearest_point_info
        if _mode == 0:
            # 反距离加权插值
            _v1 = _var_grid[i][j] * (r - r1) ** 2
            _v2 = _var_grid[i+1][j] * (r - r2) ** 2
            _v3 = _var_grid[i][j+1] * (r - r3) ** 2
            _v4 = _var_grid[i+1][j+1] * (r - r4) ** 2
            return cq * sum((_v1, _v2, _v3, _v4))
            # return var_value_of_city
        elif _mode == 1:
            # 临近点取值
            l = [r1, r2, r3, r4]
            k = l.index(min(l))
            if k == 0:
                return _var_grid[i][j]
            elif k == 1:
                return _var_grid[i+1][j]
            elif k == 2:
                return _var_grid[i][j+1]
            else:
                return _var_grid[i+1][j+1]

    @staticmethod
    def get_spherical_distance(_latA, _lonA, _latB, _lonB):
        earth_radius = 6370856
        sin_latA = math.sin(_latA * math.pi/180.0)
        cos_latA = math.cos(_latA * math.pi/180.0)
        sin_latB = math.sin(_latB * math.pi/180.0)
        cos_latB = math.cos(_latB * math.pi/180.0)
        cos_lonAB = math.cos(_lonA * math.pi/180.0 - _lonB * math.pi/180.0)
        distance_AB = earth_radius * math.acos(sin_latA * sin_latB + cos_latA * cos_latB * cos_lonAB)
        return distance_AB

