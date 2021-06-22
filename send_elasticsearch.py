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
import requests
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
    query={"size": 1, "sort": { "@date": "desc"}, "query": {"match": {"project.keyword": group}}}
    res= client.search(index=es_index, body=query)
    print(len(res['hits']['hits']))
    if len(res['hits']['hits'])>0 : 
        last_ts=res['hits']['hits'][0]['_source']['@date']
        last_ts=int(int(last_ts)/1000)
        print(group, last_ts)
    elif len(res['hits']['hits'])==0 :
        #last_ts=959771979      
        last_ts=1275304779      
    if last_ts:
            ti = last_ts
    #ti=1619899577
    return ti

if __name__ == '__main__':

    ev = oaf.get_conf()
    client = get_elasticsearchclient(ev)
    filename = oaf.get_hdf_filename(ev)
    print(80 * '=')
    print('Filename:', filename)
    batch_size = 100000
    to_ns = 1000*1000*1000
    es_index = ev['esindex']
    index_json='{ "index":{ "_index": "' + str(es_index) + '" } }'
    with h5py.File(filename, 'r') as f:
        tf = f.attrs['LastRun']
        ts = f['date'][:]
        len_ds = len(ts)
        for group in f:
            print("GROUP NAME:",group)
            if group == "date":
                continue
            ti = get_last(ev, client, group)
            print("TI:",ti)
            idx_start = oaf.time2index(ev, ti, ts)
            idx_end = oaf.time2index(ev, tf, ts)
            dgroup = f[group]

            if idx_start == idx_end:
                continue
            bulk=""
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
                bulk += index_json + '\n'
                bulk += str(make_mtr) + '\n'
                #res= client.index(index=es_index, body=make_mtr)
                b = (i+1-idx_start) % batch_size
                if not b or i == idx_end:
                    
                    bulk = bulk.replace("\'","\"")
                    retval = requests.post('https://MY_ELASTICSEARCH_HOST:MY_PORT/_bulk', data=bulk, headers={'Content-type': 'application/json'}, auth=('MY_USERNAME', 'MY_PASSWORD'))
                    print('Current index is %d', i)
                    print(retval)
                    print(80 * '=')
                    bulk=""
