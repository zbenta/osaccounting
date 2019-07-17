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
from osacc_functions import *

if __name__ == '__main__':
    ev = get_conf()
    # List of collumns of table: nova.instance_info_caches
    tstr_inst_info = 'created_at,id,network_info,deleted'
    t_inst_info = tstr_inst_info.split(",")
    dbtable = 'instance_info_caches'
    query = ' '.join((
        "SELECT " + tstr_inst_info,
        "FROM " + dbtable
    ))
    inst_info = get_table_rows('nova', query, t_inst_info)
    for inst in inst_info:
        print(80*'-')
        print(inst['id'])
        net_info = json.loads(inst['network_info'])
        if net_info:
            print(3*'-')
            pprint.pprint(net_info)
