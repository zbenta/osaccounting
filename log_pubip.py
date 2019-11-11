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
    project = { "description": None,
                "enabled": None,
                "id": None,
                "name": None
               }
    instances = {
                }
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
    print(query)
    inst_info = get_table_rows('nova', query, t_inst_info)
    for inst in inst_info:
        print(80*"-")
        pprint.pprint(inst)

    # for inst in inst_info:
    #     net_info = json.loads(inst['net_info'])
    #     if net_info:
    #         print(3*'-')
    #         for l in range(len(net_info)):
    #             for n in range(len(net_info[l]['network']['subnets'])):
    #                 for k in range(len(net_info[l]['network']['subnets'][n]['ips'])):
    #                     nip = len(net_info[l]['network']['subnets'][n]['ips'][k]['floating_ips'])
    #                     if nip:
    #                         out_info['net_info'] = net_info[l]['network']['subnets'][n]['ips'][k]['floating_ips'][0]['address']

    #         for key in t_inst_info:
    #             if key != 'net_info':
    #                 out_info[key] = inst[key]

    #         vm_list.append(out_info)

    return vm_list


if __name__ == '__main__':
    vm_info = get_out_info()
    #pprint.pprint(vm_info)
