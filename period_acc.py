#!/usr/bin/env python3

# -*- encoding: utf-8 -*-
#
# Copyright 2020 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#
"""Send metric to influxdb
"""
import os
import datetime
import dateutil.parser
import h5py
import osacc_functions as oaf
from dateutil.relativedelta import *


if __name__ == '__main__':

    ev = oaf.get_conf()
    filename = oaf.get_hdf_filename(ev)
    print(80 * '=')
    print('Filename:', filename)
    hourdt = 3600 / ev['delta_time']
    #my_ini = datetime.datetime(ev['year_ini'], ev['month_ini'], 1, 0, 0, 0)
    my_ini = datetime.datetime(2020, 7, 1, 0, 0, 0)
    last_month = datetime.datetime.now()
    last_month = last_month + relativedelta(months=-1)
    last_month = last_month + relativedelta(day=31)
    with h5py.File(filename, 'r') as f:
        while(my_ini <= last_month):
            print(80*'+')
            my_end = my_ini + relativedelta(months=+1)
            ti = oaf.to_secepoc(my_ini)
            tf = oaf.to_secepoc(my_end)
            ts = f['date'][:]
            idx_start = oaf.time2index(ev, ti, ts)
            idx_end = oaf.time2index(ev, tf, ts)
            print(my_ini, my_end)
            print(ti, tf)
            print(idx_start, idx_end)
            my_ini = my_end

            hdrline = 'project'
            for mtr in oaf.METRICS:
                hdrline = hdrline + ',' + mtr + '*hour'
            print(hdrline)
            for group in f:
                if group == "date":
                    continue
                csvline = group
                dgroup = f[group]
                for mtr in oaf.METRICS:
                    summtr = 0
                    for i in range(idx_start, idx_end):
                        data = dgroup[mtr]
                        summtr = summtr + (data[i]/hourdt)
                    csvline = csvline + ',' + str(summtr)
                print(csvline)
