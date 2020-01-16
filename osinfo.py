#!/usr/bin/env python3

# -*- encoding: utf-8 -*-
#
# Copyright 2019 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#

"""Produce list of VMs with corresponding Public IP

    Dictionaries (data structures) returned from the query to database tables
    - projects  (DB=keystone, TABLE=project)
    - instances (DB=nova,     TABLE=instances)
    - instance_info_caches (DB=nova,     TABLE=instance_info_caches)

osinfo_json =
[
    {
        "timestamp": "timestamp",
        "project_id": "string",
        "project_name": "string",
        "project_description": "string",
        "users": [
            {
                "id": "string",
                "username": "string",
                "email": "string",
                "description": "string",
                "created_at": "timestamp",
                "created": bool
            },
        ],
        "servers": [
            {
                "uuid": "string",
                "hostname": "string",
                "description": "string",
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
                "display_description": "string",
                "size": "int (GB)",
                "status": "string",
                "created_at": "datetime"
            },
        ],
    },
]
"""
import sys
import json
import csv
import pprint
import datetime
from osacc_functions import *


def create_proj():
    proj_info = {
        "timestamp": None,
        "project_id": None,
        "project_name": None,
        "project_description": None,
        "users": list(),
        "servers": list(),
        "storage": list()
        }
    return proj_info

def create_user():
    user_info = {
            "id": None,
            "username": None,
            "email": None,
            "description": None,
            "created_at": None,
            "created": None
        }
    return user_info

def create_server():
    server_info = {
            "uuid": None,
            "hostname": None,
            "description": None,
            "created_at": None,
            "key_name": None,
            "fixed_ips": list(),
            "floating_ips": list()
        }
    return server_info

def create_storage():
    storage_info = {
            "type": None,
            "id": None,
            "name": None,
            "description": None,
            "size": None,
            "status": None,
            "created_at": None,
        }
    return storage_info

def get_servers(proj_id):
    """ Get list with information of all server for a given proj_id
    :return list with VMs information
    """
    # envirn = get_conf()
    vm_list = list()
    t_inst_info = ["uuid", "hostname", "created_at", "description", "key_name"]
    tstr_inst_info = "uuid,hostname,created_at,display_description,key_name"
    query = "SELECT %s FROM instances WHERE (deleted=\'0\' AND project_id=\'%s\')" % (tstr_inst_info, proj_id)
    inst_info = get_table_rows('nova', query, t_inst_info)
    for inst in inst_info:
        server_info = create_server()
        sel_col = ["network_info"]
        qry_net = 'SELECT %s FROM instance_info_caches WHERE instance_uuid=\"%s\"' % (sel_col[0], inst["uuid"])
        net_json = get_table_rows('nova', qry_net, sel_col)
        net_info = json.loads(net_json[0]["network_info"])
        server_info['uuid'] = inst['uuid']
        server_info['hostname'] = inst['hostname']
        server_info['created_at'] = to_secepoc(inst['created_at'])
        server_info['key_name'] = inst['key_name']
        server_info['description'] = inst['description']
        if net_info:
            for n in range(len(net_info[0]['network']['subnets'])):
                server_info['fixed_ips'] = list()
                server_info['floating_ips'] = list()
                for k in range(len(net_info[0]['network']['subnets'][n]['ips'])):
                    nip = len(net_info[0]['network']['subnets'][n]['ips'][k]['floating_ips'])
                    server_info['fixed_ips'].append(net_info[0]['network']['subnets'][n]['ips'][k]['address'])
                    if nip:
                        server_info['floating_ips'].append(net_info[0]['network']['subnets'][n]['ips'][k]['floating_ips'][0]['address'])

            vm_list.append(server_info)

    return vm_list

def get_storage(proj_id):
    """ Get list with information of all volumes and snapshots for a given proj_id
    :return list with Volume information
    """
    vol_list = list()

    # Volumes
    t_info = ["id", "name", "description", "size", "status", "created_at"]
    tstr_info = "id,display_name,display_description,size,status,created_at"
    query = "SELECT %s FROM volumes WHERE (deleted=\'0\' AND project_id=\'%s\')" % (tstr_info, proj_id)
    vol_info = get_table_rows('cinder', query, t_info)
    for vol in vol_info:
        info = create_storage()
        info['type'] = "volume"
        info['id'] = vol['id']
        info['name'] = vol['name']
        info['description'] = vol['description']
        info['size'] = vol['size']
        info['created_at'] = to_secepoc(vol['created_at'])
        info['status'] = vol['status']
        vol_list.append(info)

    # Snapshots
    t_info = ["id", "name", "description", "status", "created_at"]
    tstr_info = "id,display_name,display_description,status,created_at"
    query = "SELECT %s FROM volumes WHERE (deleted=\'0\' AND project_id=\'%s\')" % (tstr_info, proj_id)
    vol_info = get_table_rows('cinder', query, t_info)
    for vol in vol_info:
        info = create_storage()
        info['type'] = "snapshot"
        info['id'] = vol['id']
        info['name'] = vol['name']
        info['description'] = vol['description']
        info['created_at'] = to_secepoc(vol['created_at'])
        info['status'] = vol['status']
        vol_list.append(info)

    return vol_list

def get_users(proj_id, proj_name):
    """ Get list with information of all users for a given proj_id
    :return list with user information
    """
    user_list = list()

    # Users not created in keystone
    ev = get_conf()
    ufiledir = ev['ufile_dir']
    fileuser = ufiledir + "/" + "users-stratus-disabled.csv"
    with open(fileuser) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            info = create_user()
            if line_count == 0:
                line_count += 1
            if row["Project"] == proj_name:
                info["username"] = row["Name"]
                info["email"] = row["Email"]
                info["description"] = row["Description"]
                info["created"] = False
                line_count += 1
                user_list.append(info)

    # Users created in keystone
    t_info = ["id", "extra", "created_at"]
    tstr_info = "id,extra,created_at"
    query = "SELECT %s FROM user WHERE default_project_id=\'%s\'" % (tstr_info, proj_id)
    user_info = get_table_rows('keystone', query, t_info)
    for user in user_info:
        info = create_user()
        info['id'] = user['id']
        info['created_at'] = to_secepoc(user['created_at'])
        info["created"] = True
        user_json = json.loads(user['extra'])
        if 'email' in user_json.keys():
            info['username'] = user_json['email']
            info['email'] = user_json['email']

        if 'description' in user_json.keys():
            info['description'] = user_json['description']

        user_list.append(info)

    return user_list

if __name__ == '__main__':
    os_info_list = list()
    tstamp = now_acc()
    if len(sys.argv) < 2:
        print('Error json file not specified: %s <data.json>' %sys.argv[0])
        sys.exit(1)

    # proj_dict: project list from keystone database
    proj_dict = get_projects(tstamp, "init")
    for proj in proj_dict:
        proj_info = create_proj()
        proj_info["timestamp"] = tstamp
        proj_info["project_id"] = proj
        proj_info["project_name"] = proj_dict[proj][0]         # idx 0 - name
        proj_info["project_description"] = proj_dict[proj][1]  # idx 1 - description

        server_list = get_servers(proj_info["project_id"])
        proj_info["servers"] = server_list
        vol_list = get_storage(proj_info["project_id"])
        proj_info["storage"] = vol_list
        user_list = get_users(proj_info["project_id"], proj_info["project_name"])
        proj_info["users"] = user_list
        os_info_list.append(proj_info)

    json_file = sys.argv[1]
    with open(json_file, 'w') as outjson:
        json.dump(os_info_list, outjson)

    # pprint.pprint(os_info_list)
