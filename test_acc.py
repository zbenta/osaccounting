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
    size_array = 0
    for year in get_years():
        size_array = size_array + time_series(year).size
        print time_series(year).size, size_array

    print 80*'='
    a = dict()
    for proj in projects:
        pname = proj['name']
        print 40 * '-'
        for m in METRICS:
            a[pname][m] = numpy.zeros([size_array, ], dtype=int)
            print pname, m, a[pname][m][:5]
