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
    db_list = get_list_db(dt_ini, "cinder")
    pprint.pprint(db_list)
