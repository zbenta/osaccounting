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
    return InfluxDBClient(dbhost, dbport, dbuser, dbpass, dbname, ssl=bssl, verify_ssl=bverify_ssl)

if __name__ == '__main__':

    ev = oaf.get_conf()
    client = get_influxclient(ev)
    print(client.get_list_database())

    print(80 * "=")
    filename = oaf.get_hdf_filename(ev)
    print("Filename:", filename)
    with h5py.File(filename, 'r') as f:
        ti = f.attrs['LastRun']
        ts = f['date'][:]
        len_ds = len(ts)
        for group in f:
            if group == "date":
                continue
            print("Group:", group)
            for metric in oaf.METRICS:
                graph_list = list()
                print("Metric:", metric)
                data = f[group][metric]
                metric_str = ev['graph_ns'] + "." + str(group) + "." + str(metric)
                for i in range(len_ds):
                    graph_string = metric_str + " " + str(data[i]) + " " + str(int(ts[i])) + "\n"
                    value = int(data[i])
                    timestamp = int(ts[i])
                    metric = str(metric_str)
                    graph_ds = (metric, (timestamp, value))
                    graph_list.append(graph_ds)

