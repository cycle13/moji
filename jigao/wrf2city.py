#!/usr/local/moji/python/bin/python
# -*- coding: utf-8 -*-

"""
@Version: V0.1
@Author: Chunrui Guo
@Email:chunrui.guo@moji.com
@File:wrf2city.py
@Time:2016/5/30 14:31
"""
import math
import os
from datetime import datetime, timedelta
from DataAccess.wrfout_access import NCDataVisitor
from DataAccess.mysql_access import MysqlAccess
from algorithms.interpolation import Interpolation
#from multiprocessing import Process
from multiprocessing import Pool as ProcessPool


class WRF2CITY:
    """
    Derive city forecast from WRF model output.
    """
    db_connect_params = {
        "dbname": "wrf", "host": "192.168.1.26",
        "port": 3307, "user": "admin", "passwd": "moji"
    }

    # query_city_sql = "SELECT v_city_id, v_lon,v_lat FROM t_p_city"
    # query_city_sql = "select t.v_city_id cityid,ROUND(t.v_lon,7) v06001," \
    #                  "ROUND(t.v_lat,7) v05001 from (SELECT v_city_id, v_lon,v_lat FROM t_p_city " \
    #                  "UNION SELECT v01000,V06001,V05001 from t_p_stations_cod)t"
    query_city_sql = "select t.v_city_id cityid,ROUND(t.v_lon,7) v06001," \
                     "ROUND(t.v_lat,7) v05001 from (SELECT v_city_id, v_lon,v_lat FROM t_p_city " \
                     "union " \
                     "select s.V01000,s.V06001,s.V05001 from t_p_stations_cod s " \
                     "JOIN (select DISTINCT t0.v01000 from t_h_surf_ele t0 " \
                     "where t0.VDate between concat('2015-','%(sMonDay)s') " \
                     "and concat('2015-','%(eMonDay)s') " \
                     "and t0.V04001 = 2015 and t0.V04004 in (2,8,14,20))t1 " \
                     "on s.V01000 = t1.v01000)t"
    # query_city_sql = "SELECT v01000 cityid,round(V06001,7) v06001," \
    #                  "round(V05001,7) v05001 from t_p_stations_cod " \
    #                  "where v01000 in (54734,54823,54826,54857,54774)"

    # insert_city_sql = "REPLACE INTO t_r_city_forecast_hourly VALUES (%s)" % ','.join(['%s']*11)
    insert_city_sql = "insert ignore into t_r_city_forecast_hourly VALUES (%s)" % ','.join(['%s']*15)

    enable_process_per_city = True

    def __init__(self, _start_observe_time, _wrf_file):

        self.db_helper = MysqlAccess(self.db_connect_params)

        _file_name = os.path.basename(_wrf_file)

        self.start_time = datetime.strptime(_start_observe_time, "%Y%m%d%H")
        self.forecast_time = datetime.strptime(_file_name[11:24], "%Y-%m-%d_%H")

        self.wrf_visitor = NCDataVisitor(_wrf_file)
        self.dx = self.wrf_visitor.get_variable_by_attributes("DX")
        self.dy = self.wrf_visitor.get_variable_by_attributes("DY")
        self.nx = self.wrf_visitor.get_dimension_by_name("west_east")
        self.ny = self.wrf_visitor.get_dimension_by_name("south_north")
        self.grid_latitude = self.wrf_visitor.get_value_by_variable("XLAT", 0)
        self.grid_longitude = self.wrf_visitor.get_value_by_variable("XLONG", 0)

        # self.RH_2m = None
        # self.rain = None
        # self.wind_speed = None
        # self.wind_direction = None

        self.temp_2m, self.u_wind_10m, self.v_wind_10m, self.pressure = [None]*4
        self.RH_2m = [[0 for c in range(self.nx)] for r in range(self.ny)]
        self.rain = [[0 for c in range(self.nx)] for r in range(self.ny)]
        self.wind_speed = [[0 for c in range(self.nx)] for r in range(self.ny)]
        self.wind_direction = [[0 for c in range(self.nx)] for r in range(self.ny)]

    def calculate_relative_humidity(self):
        self.temp_2m = self.wrf_visitor.get_value_by_variable("T2", 0)
        qv_2m = self.wrf_visitor.get_value_by_variable("Q2", 0)
        self.pressure = self.wrf_visitor.get_value_by_variable("PSFC", 0)

        for i in range(0, self.ny):
            for j in range(0, self.nx):
                tmp1 = 10.0 * 0.6112 * math.exp(17.67 * (self.temp_2m[i][j] - 273.16) / (self.temp_2m[i][j] - 29.65))
                tmp2 = 0.622 * tmp1 / (0.01 * self.pressure[i][j] - (1 - 0.622) * tmp1)
                self.RH_2m[i][j] = round(100 * max([min([qv_2m[i][j]/tmp2, 1.0]), 0.0]), 2)

    def calculate_rain(self):
        _rain_c = self.wrf_visitor.get_value_by_variable("RAINC", 0)
        _rain_nc = self.wrf_visitor.get_value_by_variable("RAINNC", 0)

        for i in range(self.ny):
            for j in range(self.nx):
                self.rain[i][j] = _rain_c[i][j] + _rain_nc[i][j]

    def calculate_wind(self):
        self.u_wind_10m = self.wrf_visitor.get_value_by_variable("U10", 0)
        self.v_wind_10m = self.wrf_visitor.get_value_by_variable("V10", 0)

        for i in range(0, self.ny):
            for j in range(0, self.nx):
                _wind_speed = round(math.sqrt(self.u_wind_10m[i][j]**2 + self.v_wind_10m[i][j]**2), 2)
                _wind_direction = 270.0 - math.atan2(self.v_wind_10m[i][j], self.u_wind_10m[i][j]) * 180.0 / math.pi
                _wind_direction_round = round(math.fmod(_wind_direction, 360.0), 2)
                self.wind_speed[i][j], self.wind_direction[i][j] = _wind_speed, _wind_direction_round

    def derive_diagnostics(self):
        self.calculate_rain()
        self.calculate_relative_humidity()
        self.calculate_wind()

    def single_city_forecast(self, _city_line):

        city_record = [_city_line[0], self.start_time, self.forecast_time,
                       self.forecast_time.year, self.forecast_time.month,
                       self.forecast_time.day, self.forecast_time.hour]

        city_lon, city_lat = float(_city_line[1]), float(_city_line[2])

        grid_info = Interpolation.get_nearest_point(city_lon, city_lat,
                                                    self.grid_longitude, self.grid_latitude,
                                                    self.nx, self.ny, self.dx, self.dy)

        if grid_info is None:
            return city_record

        grid_vars = [
            self.temp_2m, self.pressure, self.RH_2m, self.u_wind_10m,
            self.v_wind_10m, self.wind_speed, self.wind_direction, self.rain
        ]

        _mode = 0

        for _index, _grid_value in enumerate(grid_vars):

            _city_var = Interpolation.interpolate_grid_to_city(_grid_value,
                                                               _mode, *grid_info)
            if _index == 0:
                city_record.append(round(_city_var - 273.16, 1))
            elif _index == 1:
                city_record.append(round(_city_var / 100.0, 1))
            elif _index == 6:
                city_record.append(round(_city_var, 0))
                # _mode = 1
            else:
                city_record.append(round(_city_var, 1))

        return city_record

    def make_city_forecast_data(self):
        # 从city表中获取城市经纬度信息
        _end_fcst_time = self.start_time + timedelta(days=6)
        _fcst_city_list = self.db_helper.query_data(self.query_city_sql % {
            'sMonDay': self.start_time.strftime('%m-%d'),
            'eMonDay': _end_fcst_time.strftime('%m-%d')
        })
        # _city_infos = self.db_helper.query_data(self.query_city_sql)

        self.derive_diagnostics()

        if self.enable_process_per_city:
            # sdt = datetime.now()
            # ver.1
            _daemon_process = []
            for _c_i in _fcst_city_list:
                _p = Process(target=self.save_forecast_data, args=(_c_i,))
                # _p.daemon = True
                _daemon_process.append(_p)
                _p.start()
            for _d_p in _daemon_process:
                _d_p.join()
            #
            pp = ProcessPool(20)
            pp.map(cpu_calc,_fcst_city_list)
            pp.close()
            pp.join()
            # edt = datetime.now()
            # print "finished file[%s] and elapsed(%s)" % (self.forecast_time.strftime('%Y%m%d%H'), edt - sdt)
        else:
            results = [self.single_city_forecast(_c_i) for _c_i in _fcst_city_list]
            _city_data_table = filter(lambda x:len(x) == 15, results)
            self.db_helper.insert_or_update_data(self.insert_city_sql, *_city_data_table)

    def save_forecast_data(self, _city_location):
        _city_forecast_data = self.single_city_forecast(_city_location)
        if len(_city_forecast_data) == 15:
            self.db_helper.insert_or_update_data(self.insert_city_sql, _city_forecast_data)
        

if __name__ == "__main__":

    # db_connect_params = {
    #    "dbname": "wrf", "host": "192.168.1.26",
    #    "port": 3307, "user": "admin", "passwd": "moji"
    # }

    # a = WRF2CITY("2016-01-20", u"E:\wrfout_files", "wrfout_d01_2016-01-20_01",  param, False)
    # t_start = datetime.datetime.now()
    # #a.get_city_forecast()
    # t_end = datetime.datetime.now()
    # print t_end-t_start

    # print WRF2CITY.wrf_out_root_dir

    t_start = datetime.now()
    # _db_helper = MysqlAccess(db_connect_params)
    # _query_city_sql = "SELECT v_city_id, v_lon,v_lat FROM t_p_city limit 20"

    WRF2CITY.enable_process_per_city = False
    b = WRF2CITY("2016080700", 'wrfout_d01_2016-08-07_12.out')  # "wrfout_d01_2016-06-18_13:00:00")
    # city_locations = _db_helper.query_data(_query_city_sql)
    print b.enable_process_per_city
    # _data_rows = b.make_city_forecast_data()
    #
    # if _data_rows is not None and len(_data_rows) > 0:
    #     _sql_insert_format = "REPLACE INTO t_r_city_forecast_hourly VALUES (%s)"
    #     sql_str = _sql_insert_format % ','.join(['%s']*11)
    #     # _db_helper.insert_or_update_data(sql_str, *_data_rows)
    #     print "inserted datarow count:", len(_data_rows)
    #
    # b.make_city_forecast_data()

    t_end = datetime.now()
    print "Total Elapsed:", t_end - t_start

