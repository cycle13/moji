#
# Copyright 2005-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

from __future__ import print_function
import traceback
import sys
from eccodes import *

INPUT = '/Users/yetao.lu/Desktop/mos/temp/D1D04180000041812001.grib'
VERBOSE = 1  # verbose error reporting


def example():
    points = ((30, -20), (13, 234))
    
    f = open(INPUT)
    gid = codes_grib_new_from_file(f)
    print(gid)
    for lat, lon in points:
        nearest = codes_grib_find_nearest(gid, lat, lon)[0]
        print(lat, lon)
        print(nearest.lat, nearest.lon, nearest.value, nearest.distance,
              nearest.index)
        
        four = codes_grib_find_nearest(gid, lat, lon, is_lsm=False, npoints=4)
        for i in range(len(four)):
            print("- %d -" % i)
            print(four[i])
        
        print("-" * 100)
    
    codes_release(gid)
    f.close()


def main():
    try:
        example()
    except CodesInternalError as err:
        if VERBOSE:
            traceback.print_exc(file=sys.stderr)
        else:
            sys.stderr.write(err.msg + '\n')
        
        return 1


if __name__ == "__main__":
    sys.exit(main())