#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/4/17
description:
"""

from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
print("hello world from process ", rank)
