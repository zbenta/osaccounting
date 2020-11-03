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


if __name__ == '__main__':

    ev = oaf.get_conf()
    filename = oaf.get_hdf_filename(ev)
    print(80 * '=')
    print('Filename:', filename)
    my_ini = datetime.datetime(ev['year_ini'], ev['month_ini'], 1, 0, 0, 0)
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    with h5py.File(filename, 'r') as f:
        while(my_ini <= last_month):
            print(80*'+')
            my_end = my_ini.month + 1
            ti = oaf.to_secepoc(my_ini)
            tf = oaf.to_secepoc(my_end)
            ts = f['date'][:]
            idx_start = oaf.time2index(ev, ti, ts)
            idx_end = oaf.time2index(ev, tf, ts)
            print(my_ini, my_end)
            print(ti, ts)
            print(idx_start, idx_end)
            my_ini = my_end

            # for group in f:
            #     if group == "date":
            #         continue
            #     dgroup = f[group]
