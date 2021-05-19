#!/usr/bin/env python3

# -*- encoding: utf-8 -*-
#
# Copyright 2020 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#         Zacarias Benta <zacarias@lip.pt>
#         Miguel Viana   <mviana@lip.pt>
#
"""Send metric to elasticsearch
"""
import os
import datetime
import dateutil.parser
import h5py
import json
from elasticsearch import Elasticsearch
import osacc_functions as oaf

def get_elasticsearchclient(ev):
    """Get ElasticSearch Client
    :return: ElasticSearch Client
    """
    es_host = ev['eshost']
    es_apikey = ev['esapikey']
    client = Elasticsearch([es_host],api_key=es_apikey)
    return client

def get_last(ev, client, group):
    """Get last metric timestamp from ElasticSearch through HTTP API
    :param ev: Configuration options list
    :param client: ElasticSearch client
    :param group: project
    :return (datetime) last timestamp in seconds to epoc
    """
    es_index = ev['esindex']
    query={"size": 1, "sort": { "@date": "desc"}, "query": {"match_all": {}}}
    res= client.search(index=es_index, body=query)
    last_ts=res['hits']['hits'][0]['_source']['@date']
    last_ts=int(int(last_ts)/1000)
    ti = ev['secepoc_ini']
    if last_ts:
            ti = last_ts
    return ti

if __name__ == '__main__':

    ev = oaf.get_conf()
    client = get_elasticsearchclient(ev)
    filename = oaf.get_hdf_filename(ev)
    print(80 * '=')
    print('Filename:', filename)
    batch_size = 5000
    to_ns = 1000*1000*1000
    es_index = ev['esindex']
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

            if idx_start == idx_end:
                continue

            for i in range(idx_start, idx_end+1):
                a = (i-idx_start) % batch_size
                if not a:
                    data_met = list()
                make_mtr={}
                make_mtr["@date"]=str(int(ts[i]*1000))
                make_mtr["project"]=group
                for mtr in oaf.METRICS:
                    data = dgroup[mtr]
                    q_name = "q_" + mtr
                    q_value = dgroup.attrs[q_name]

                    make_mtr[mtr]=data[i]
                    make_mtr[q_name] = q_value

                res= client.index(index=es_index, body=make_mtr)
                print(res)
                #b = (i+1-idx_start) % batch_size


