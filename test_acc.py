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
    ev = get_conf()
    directory = os.path.dirname(ev['out_dir'])
    if not os.path.exists(directory):
        os.makedirs(directory, 0755)

    dt_ini = ev['secepoc_ini']
    projects = get_list_db(dt_ini, "keystone")
    instances = get_list_db(dt_ini, "nova")
    volumes = get_list_db(dt_ini, "cinder")
    time_array = time_series_ini()
    print 80*'='
    a = dict()
    for proj in projects:
        pname = proj['name']
        a[pname] = dict()
        print 20 * '-', pname
        for m in METRICS:
            a[pname][m] = numpy.zeros([time_array.size, ], dtype=int)

    for inst in instances:
        t_create = to_secepoc(inst["created_at"])
        t_final = now_acc()
        if inst["deleted_at"]:
            t_final = to_secepoc(inst["deleted_at"])

        idx_start, idx_end = dt_to_index(t_create, t_final, time_array)
        print 80 * '='
        print idx_start, idx_end
        print t_create, t_final
        print time_array[idx_start], time_array[idx_end]
        print t_create-time_array[idx_start], t_final-time_array[idx_end]
        if t_create-time_array[idx_start] >= 60.0:
            print "HHHEEELLLPPP dt_create = ", t_create-time_array[idx_start]
        if t_final-time_array[idx_end] <= -60.0:
            print "HHHEEELLLPPP dt_create = ", t_final-time_array[idx_end]

        p = filter(lambda pr: pr['id'] == inst['project_id'], projects)
        if not p:
            continue
