#!/usr/bin/env python3

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

osinfo =
[
    {
        "timestamp": "timestamp",
        "project_id": "string",
        "project_name": "string",
        "project_description": "string",
        "users": [
            {
                "id": "string",
                "name": "string",
                "email": "string",
                "description": "string"
            },
        ],
        "servers": [
            {
                "uuid": "string",
                "hostname": "string",
                "created_at": "timestamp",
                "key_name": "string",
                "fixed_ips": [],
                "floating_ips": []
            },
        ],
        "storage": [
            {
                "type": "<volume|snapshot|image>",
                "id": "string",
                "name": "string",
                "size": "int (GB)",
                "status": "string",
                "created_at": "datetime"
            },
        ],
    },
]
"""
import json
import pprint
from osacc_functions import *

t_inst_info = ["uuid", "hostname", "created_at", "deleted_at", "deleted",
               "key_name", "project", "project_description"]

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
                      "instances.key_name,keystone.project.name,keystone.project.description")
    query = ' '.join((
        "SELECT " +  tstr_inst_info,
        "FROM instances ",
        "INNER JOIN keystone.project ON instances.project_id = keystone.project.id ",
        "INNER JOIN keystone.user ON instances.user_id = keystone.user.id ",
        "WHERE instances.deleted=\'0\'"
    ))
    inst_info = get_table_rows('nova', query, t_inst_info)
    for inst in inst_info:
        sel_col = ["network_info"]
        qry_net = 'SELECT %s FROM instance_info_caches WHERE instance_uuid=\"%s\"' % (sel_col[0], inst["uuid"])
        print(80*"=")
        pprint.pprint(inst)
        print(10*"-")
        net_json = get_table_rows('nova', qry_net, sel_col)
        net_info = json.loads(net_json[0]["network_info"])

        if net_info:
            print(3*'-')
            print(net_info[0]['network']['subnets'])
            for n in range(len(net_info[0]['network']['subnets'])):
                out_info['fixed_ips'] = list()
                out_info['floating_ips'] = list()
                for k in range(len(net_info[0]['network']['subnets'][n]['ips'])):
                    nip = len(net_info[0]['network']['subnets'][n]['ips'][k]['floating_ips'])
                    out_info['fixed_ips'].append(net_info[0]['network']['subnets'][n]['ips'][k]['address'])
                    if nip:
                        out_info['floating_ips'].append(net_info[0]['network']['subnets'][n]['ips'][k]['floating_ips'][0]['address'])
                    print(3*'-')
                    print(out_info)

        #     for key in t_inst_info:
        #         if key != 'net_info':
        #             out_info[key] = inst[key]

        #     vm_list.append(out_info)

    return vm_list


if __name__ == '__main__':
    vm_info = get_out_info()
    print(80*"O")
    pprint.pprint(vm_info)
