# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#

"""Create initial hdf5 files to store accounting data

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
    years = get_years()
    di = ev['secepoc_ini']
    df = to_secepoc(datetime.datetime(years[-1]+1, 1, 1, 0, 0, 0))
    time_array = time_series(di, df)
    projects = get_list_db(di, "keystone")
    a = dict()
    insert_projects(di, time_array, a)
    insert_instances(di, time_array, a)
    insert_volumes(di, time_array, a)

    directory = os.path.dirname(ev['out_dir'])
    if not os.path.exists(directory):
        os.makedirs(directory, 0755)

    years = get_years()
    for year in years:
        print 80*'='
        print year
        print ' '
        filename = create_hdf(year)
        with h5py.File(filename, 'r+') as f:
            ts = f['date'][:]
            idx_start, idx_end = dt_to_index(ts[0], ts[-1], time_array)
            for proj in projects:
                grp_name = proj['name']
                for metric in METRICS:
                    data_array = f[grp_name][metric]
                    print 'hdf data_array size = ', data_array.size
                    print 'idx_start = ', idx_start, ' idx_end = ', idx_end
                    data_array[:] = a[grp_name][metric][idx_start:idx_end+1]

            tnow = now_acc()
            f.attrs['LastRun'] = tnow
            f.attrs['LastRunUTC'] = str(to_isodate(tnow))
            if year < years[-1]:
                f.attrs['LastRun'] = ts[-1]
                f.attrs['LastRunUTC'] = str(to_isodate(ts[-1]))
