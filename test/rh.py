#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/22
description:
"""

import math
def convertSHUtoRH(t1, p1, q1):

    a = 17.67 * t1 / (t1 + 243.4)
    
    E= 6.112 * (math.pow(2.718281828459, a))
    
    e = q1 * p1/ (0.622 + 0.378 * q1)
    
    RH = e / E
    print t1,p1,q1,a, e, E, RH
    return RH
if __name__ == "__main__":
    print ''
    a=convertSHUtoRH(25,1000,0.018)
    b=convertSHUtoRH(30,900,0.018)
    print a,b