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


def get_last(ev, client, group):
    """Get last metric timestamp from DB
    :param ev: Configuration options list
    :param client: InfluxDB client
    :param group: project
    :return (datetime) last timestamp in seconds to epoc
    """
    qry_str = 'SELECT last(vcpus) FROM cloud_acc WHERE project=$proj'
    bind_params = {'proj': group}
    last_ts = client.query(qry_str, bind_params=bind_params)
    ti = ev['secepoc_ini']
    if last_ts:
        time_stamp = last_ts.get_points()
        for t in time_stamp:
            a = dateutil.parser.parse(t["time"])
            ti = oaf.to_secepoc(a)
    return ti


if __name__ == '__main__':

    ev = oaf.get_conf()
    filename = oaf.get_hdf_filename(ev)
    print(80 * '=')
    print('Filename:', filename)
    batch_size = 5000
    to_ns = 1000*1000*1000
    with h5py.File(filename, 'r') as f:
        tf = f.attrs['LastRun']
        ts = f['date'][:]
        len_ds = len(ts)
        for group in f:
            if group == "date":
                continue

            ti = get_last(ev, client, group)
            idx_start = oaf.time2index(ev, ti, ts)
            idx_end = oaf.time2index(ev, tf, ts)
            dgroup = f[group]
            for i in range(idx_start, idx_end+1):
                a = (i-idx_start) % batch_size
                if not a:
                    data_met = list()

                make_mtr = "cloud_acc,project=" + group + " "
                for mtr in oaf.METRICS:
                    data = dgroup[mtr]
                    q_name = "q_" + mtr
                    q_value = dgroup.attrs[q_name]
                    make_mtr = make_mtr + mtr + "=" + str(data[i]) + ","
                    make_mtr = make_mtr + q_name + "=" + str(q_value) + ","

                infl_proj = make_mtr.rstrip(",")
                infl_proj = infl_proj + " " + str(int(ts[i]*to_ns))
                data_met.append(infl_proj)
                b = (i+1-idx_start) % batch_size
                if not b or (i == idx_end):
                    client.write_points(data_met, batch_size=batch_size,
                                        protocol='line')

