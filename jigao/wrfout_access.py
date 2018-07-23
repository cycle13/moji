#!/usr/local/moji/python/bin/python
# encoding:utf-8

"""
@Version: V0.1
@Author: Chunrui Guo
@Email:chunrui.guo@moji.com
@File:wrfout_data_Reader.py
@Time:2016/5/31 10:17
@Modify: Gao Ji
@Time:2016/7/1 10:53
"""
from netCDF4 import Dataset


class NCDataVisitor:
    """
    nc  file reader/writer
    """
    def __init__(self, _file_name):
        # self.file_name = _file_name
        self.root_group = Dataset(_file_name, 'r')

    def __del__(self):
        self.root_group.close()

    def get_variable_by_attributes(self, _attr_name):
        var = self.root_group.getncattr(_attr_name)
        return var

    def get_dimension_by_name(self, _dim_name):
        dim_dic = self.root_group.dimensions[_dim_name]
        return dim_dic.size

    def get_value_by_variable(self, _var_name, lev):
        var = self.root_group.variables[_var_name]
        return var[0, lev, :, :] if var.ndim > 3 else var[0, :, :]

    def get_global_attributes(self):
        _attr_keys = self.root_group.ncattrs()
        attr_dict = dict.fromkeys(_attr_keys)
        for k in attr_dict:
            attr_dict[k] = self.root_group.getncattr(k)
        return attr_dict

    @staticmethod
    def write_data(_file_name, _dims, _vars, _attr_dict):
        _ds = Dataset(_file_name, 'w', format='NETCDF3_CLASSIC')
        if isinstance(_attr_dict, dict):
            _ds.setncatts(_attr_dict)

        for _d, _size in _dims:
            _ds.createDimension(_d, _size)

        for _i in _vars:
            _v = _ds.createVariable(_i['name'], _i['type'], _i['dims'])
            if 'attr' in _i:
                _v.setncatts(_i['attr'])
            _v[0, :, :] = _i['data']
        _ds.sync()
        _ds.close()

