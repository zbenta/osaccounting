# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Create initial hdf5 file to store accounting data for the first year

    Dictionaries (data structures) returned from the query to database tables
    - projects  (DB=keystone, TABLE=project)
    - instances (DB=nova,     TABLE=instances)
    - volumes   (DB=cinder,   TABLE=volumes)
    project = { "description": None,
                "enabled": None,
                "id": None,
                "name": None
               }
    instances = {
                }
    volumes = {
              }
"""

from osacc_functions import *

if __name__ == '__main__':
    year = YEAR_INI
    evr = get_env()
    projects = get_list_db("keystone", "project")
    instances = get_list_db("nova", "instances")
    instances_info = get_list_db("nova", "instance_info_caches")
    for inst in instances:
        p = filter(lambda pr: pr['id'] == inst['project_id'], projects)
        if not p:
            continue

        proj = p[0]
        print 80*'-'
        print 10*"x", " Instance ID = ", inst['uuid']
        print "ProjName = ", proj["name"], " VCPUs= ", inst['vcpus'], " Mem_MB= ", inst['memory_mb']
        print 'Inst Start = ', inst["created_at"], ' Inst End = ', inst["deleted_at"]
