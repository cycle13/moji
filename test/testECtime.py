#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/12
description:
"""
import eccodes
file_path = '/Users/yetao.lu/Desktop/mos/mosdata/d/D1D04180000041812001.grib'
def cli(file_path):
    with open(file_path, 'rb') as f:
        handle = eccodes.codes_grib_new_from_file(f, headers_only=False)
        while handle is not None:
            date = eccodes.codes_get(handle, "dataDate")
            type_of_level = eccodes.codes_get(handle, "typeOfLevel")
            level = eccodes.codes_get(handle, "level")
            values = eccodes.codes_get_array(handle, "values")
            value = values[-1]
            values_array = eccodes.codes_get_values(handle, "values")
            value_array = values[-1]

            print(date, type_of_level, level, value)

            eccodes.codes_release(handle)
            handle = eccodes.codes_grib_new_from_file(f, headers_only=False)


if __name__ == "__main__":
    cli(file_path)
    
