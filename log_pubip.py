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
    ''' SQL queries
    select floating_ip_address,floating_port_id from neutron.floatingips;
    select id,device_id from neutron.ports;
    select id,uuid,hostname,user_id,project_id,created_at,deleted_at,key_name from nova.instances;
    select id,name from keystone.project;
    select id,extra from keystone.user;
    
    Just for future reference
    SELECT neutron.floatingips.floating_ip_address,nova.instances.uuid,nova.instances.hostname,nova.instances.created_at,keystone.project.name
    FROM neutron.floatingips
    INNER JOIN neutron.ports ON neutron.floatingips.floating_port_id = neutron.ports.id
    INNER JOIN nova.instances ON neutron.ports.device_id = nova.instances.uuid
    INNER JOIN keystone.project ON nova.instances.project_id = keystone.project.id
    INNER JOIN keystone.user ON nova.instances.user_id = keystone.user.id
    '''
    ev = get_conf()
    tstr_fips = 'floating_ip_address,floating_port_id'
    t_fips = ['fip', 'devid']
    dbtable = 'floatingips'
    query = ' '.join((
        "SELECT floating_ip_address,ports.device_id",
        "FROM floatingips",
        "INNER JOIN neutron.ports ON floating_port_id = ports.id"
    ))
    net_fips = get_table_rows('neutron', query, t_fips)
    for fip in net_fips:
        pprint.pprint(fip)
