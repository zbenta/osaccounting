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
    client = InfluxDBClient(dbhost, dbport, dbuser, dbpass, dbname, ssl=bssl, verify_ssl=bverify_ssl)
    check_conn = client.ping()
    if check_conn:
        print(check_conn)
    return client

def get_last(ev):
    """Get last metric timestamp from DB
    :param ev: configuration options
    :return (datetime) last timestamp in seconds to epoc
    """
    #TODO: to be implemented
    ti = datetime.datetime.utcnow()
    return ti

def create_dict(proj):
    """Create dict of metrics
    :param proj: project name
    :return (dict) dict of metric and timestamp
    """
    infl_proj = {"measurement": proj, 'metric': dict()}
    for mtr in oaf.METRICS:
        infl_proj['metric'][mtr] = ""
        infl_proj['time'] = 0

    return infl_proj

if __name__ == '__main__':

    ev = oaf.get_conf()
    client = get_influxclient(ev)
    # ti = get_last(ev)
    filename = oaf.get_hdf_filename(ev)
    influx_list = list()
    print(80 * '=')
    print('Filename:', filename)

    with h5py.File(filename, 'r') as f:
        tf = f.attrs['LastRun']
        ts = f['date'][:]
        # idx_start = oaf.time2index(ev, ti, ts)
        # idx_end = oaf.time2index(ev, tf, ts)
        idx_start = 621000
        idx_end = 621010
        len_ds = len(ts)
        for group in f:
            if group == "date":
                continue
            print(80 * '=')
            print("Group:", group)
            for i in range(idx_start, idx_end+1):
                make_mtr = "cloud_acc,project=" + group + " "
                for mtr in oaf.METRICS:
                    print("Metric:", mtr)
                    data = f[group][mtr]
                    make_mtr = make_mtr + mtr + "=" + str(data[i]) + ","

                infl_proj = make_mtr.rstrip(",")
                infl_proj = infl_proj + " " + str(ts[i])
                print(infl_proj)

    # print(influx_list)


# "{measurement},location={location},fruit={fruit},id={id} x={x},y={y},z={z}i {timestamp}"
#             .format(measurement=measurement_name,
#                     location=random.choice(location_tags),
#                     fruit=random.choice(fruit_tags),
#                     id=random.choice(id_tags),
#                     x=round(random.random(),4),
#                     y=round(random.random(),4),
#                     z=random.randint(0,50),
#                     timestamp=data_start_time)

    # with h5py.File(filename, 'r') as f:
    #     ti = f.attrs['LastRun']
    #     ts = f['date'][:]
    #     len_ds = len(ts)
    #     for group in f:
    #         if group == "date":
    #             continue
    #         print("Group:", group)
    #         for metric in oaf.METRICS:
    #             graph_list = list()
    #             print("Metric:", metric)
    #             data = f[group][metric]
    #             metric_str = ev['graph_ns'] + "." + str(group) + "." + str(metric)
    #             for i in range(len_ds):
    #                 graph_string = metric_str + " " + str(data[i]) + " " + str(int(ts[i])) + "\n"
    #                 value = int(data[i])
    #                 timestamp = int(ts[i])
    #                 metric = str(metric_str)
    #                 graph_ds = (metric, (timestamp, value))
    #                 graph_list.append(graph_ds)

