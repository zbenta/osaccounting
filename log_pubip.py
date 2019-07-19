# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#

"""Produce list of VMs with corresponding Public IP

    Dictionaries (data structures) returned from the query to database tables
    - projects  (DB=keystone, TABLE=project)
    - instances (DB=nova,     TABLE=instances)
    - instance_info_caches (DB=nova,     TABLE=instance_info_caches)
    project = { "description": None,
                "enabled": None,
                "id": None,
                "name": None
               }
    instances = {
                }
"""
from __future__ import print_function
import pprint
import json
from elasticsearch import Elasticsearch
from osacc_functions import *

t_inst_info = ["uuid", "hostname", "created_at", "deleted_at", "deleted",
               "key_name", "project", "user", "net_info"]
doctype = 'vm_info'
esbody = {doctype: {
              "properties": {
                  "uuid": {
                      "type": "text"
                      },
                  "hostname": {
                      "type": "text"
                  },
                  "created_at": {
                      "type": "date"
                  },
                  "deleted_at": {
                      "type": "date"
                  },
                  "deleted": {
                      "type": "int"
                  },
                  "key_name": {
                      "type": "text"
                  },
                  "project": {
                      "type": "text"
                  },
                  "user": {
                      "type": "text"
                  },
                  "net_info": {
                      "type": "ip"
                  },
              }
            }
         }

def get_out_info():
    """ SQL query
    SELECT instances.uuid,instances.hostname,instances.created_at,
        instances.deleted_at,instances.deleted,instances.key_name,
        instance_info_caches.network_info,keystone.project.name,
        keystone.user.extra
    FROM instances
    INNER JOIN instance_info_caches ON instances.id = instance_info_caches.id
    INNER JOIN keystone.project ON instances.project_id = keystone.project.id
    INNER JOIN keystone.user ON instances.user_id = keystone.user.id;

    :return list with VMs information
    """
    ev = get_conf()
    vm_list = list()
    out_info = dict()
    # List of collumns of table: nova.instance_info_caches
    tstr_inst_info = ("instances.uuid,instances.hostname,instances.created_at,"
                      "instances.deleted_at,instances.deleted,"
                      "instances.key_name,keystone.project.name,"
                      "keystone.user.extra,instance_info_caches.network_info")
    query = ' '.join((
        "SELECT " +  tstr_inst_info,
        "FROM instances ",
        "INNER JOIN instance_info_caches ON instances.id = instance_info_caches.id ",
        "INNER JOIN keystone.project ON instances.project_id = keystone.project.id ",
        "INNER JOIN keystone.user ON instances.user_id = keystone.user.id"
    ))
    inst_info = get_table_rows('nova', query, t_inst_info)
    for inst in inst_info:
        net_info = json.loads(inst['net_info'])
        if net_info:
            print(3*'-')
            for l in range(len(net_info)):
                for n in range(len(net_info[l]['network']['subnets'])):
                    for k in range(len(net_info[l]['network']['subnets'][n]['ips'])):
                        nip = len(net_info[l]['network']['subnets'][n]['ips'][k]['floating_ips'])
                        if nip:
                            out_info['net_info'] = net_info[l]['network']['subnets'][n]['ips'][k]['floating_ips'][0]['address']

            for key in t_inst_info:
                if key != 'net_info':
                    out_info[key] = inst[key]

            vm_list.append(out_info)

    return vm_list


def get_es_conn():
    ev = get_conf()
    try:
        es = Elasticsearch([{'host':ev['eshost'],'port': ev['esport']}])
        print('Connected {}'.format(es))
        return es
    except Exception as ex:
        print('Error {}'.format(ex))
        return None


def es_insert(vm_info):
    esconn = get_es_conn()
    sidx = esconn.indices.create(index='pub_ips', ignore=400)
    print('{}'.format(sidx))
    for vm in vm_info:
        print(80*'-')
        pprint.pprint(vm)
        print(5*'=')
        s = esconn.search(index='pub_ips', body={"query": {"match": vm}})
        print('{}'.format(s))


if __name__ == '__main__':
    vm_info = get_out_info()
    #es_insert(vm_info)
    #pprint.pprint(vm_info)
