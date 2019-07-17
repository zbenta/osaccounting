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
        inst_id = inst['id']
        net_info = json.loads(inst['network_info'])
        if net_info:
            print(3*'-')
            for l in range(len(net_info)):
                for n in range(len(net_info[l]['network']['subnets'])):
                    for k in range(len(net_info[l]['network']['subnets'][n]['ips'])):
                        nip = len(net_info[l]['network']['subnets'][n]['ips'][k]['floating_ips'])
                        if nip:
                            fip = net_info[l]['network']['subnets'][n]['ips'][k]['floating_ips'][0]['address']
                            print(fip)

        tstr_instances = 'created_at,id,user_id,project_id,key_name,hostname,host,deleted'
        t_instances = tstr_inst_info.split(",")
        dbtable = 'instances'
        condition = "(id = '" + str(inst_id) + "' )"
        query = ' '.join((
            "SELECT " + tstr_inst_info,
            "FROM " + dbtable,
            "WHERE " + condition
        ))
        inst_info = get_table_rows('nova', query, t_instances)
