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
from influxdb import InfluxDBClient
import osacc_functions as oaf

def get_influxclient(ev):
    """Get InfluxDB Client
    :return: InfluxDB Client
    """
    dbhost = ev['dbhost']
    dbport = ev['dbport']
    dbuser = ev['dbuser']
    dbpass = ev['dbpass']
    dbname = ev['dbname']
    bssl = ev['ssl']
    bverify_ssl = ev['verify_ssl']
    client = InfluxDBClient(dbhost, dbport, dbuser, dbpass,
                            ssl=bssl, verify_ssl=bverify_ssl)
    check_conn = client.ping()
    if check_conn:
        print(check_conn)
    client.create_database(dbname)
    client.switch_database(dbname)
    return client

def get_last(ev, client):
    """Get last metric timestamp from DB
    :param client: InfluxDB client
    :return (datetime) last timestamp in seconds to epoc
    """
    last_ts = client.query('select last(vcpus) from cloud_acc')
    ti = ev['secepoc_ini']
    if last_ts:
        time_stamp = last_ts.get_points()
        for t in time_stamp:
            print(t["time"])
            a = dateutil.parser.parse(t["time"])
            ti = oaf.to_secepoc(a)

    print(ti)
    return ti


if __name__ == '__main__':

    ev = oaf.get_conf()
    client = get_influxclient(ev)
    ti = get_last(ev, client)
    filename = oaf.get_hdf_filename(ev)
    print(80 * '=')
    print('Filename:', filename)

    batch_size = 5000
    to_ns = 1000*1000*1000
    with h5py.File(filename, 'r') as f:
        tf = f.attrs['LastRun']
        ts = f['date'][:]
        idx_start = oaf.time2index(ev, ti, ts)
        idx_end = oaf.time2index(ev, tf, ts)
        len_ds = len(ts)
        print(80 * '=')
        print("LastRun - tf:", oaf.to_isodate(tf))
        print("idx start:", idx_start)
        print("idx end:", idx_end)
        print("Start Time:", oaf.to_isodate(ts[idx_start]))
        print("End Time:", oaf.to_isodate(ts[idx_end]))
        for group in f:
            if group == "date":
                continue
            print("Group:", group)
            for i in range(idx_start, idx_end+1):
                a = (i-idx_start) % batch_size
                if not a:
                    data_met = list()

                make_mtr = "cloud_acc,project=" + group + " "
                for mtr in oaf.METRICS:
                    data = f[group][mtr]
                    make_mtr = make_mtr + mtr + "=" + str(data[i]) + ","

                infl_proj = make_mtr.rstrip(",")
                infl_proj = infl_proj + " " + str(int(ts[i]*to_ns))
                data_met.append(infl_proj)
                b = (i+1-idx_start) % batch_size
                if not b or (i == idx_end):
                    print(i, data_met[0])
                    client.write_points(data_met, batch_size=batch_size,
                                        protocol='line')

