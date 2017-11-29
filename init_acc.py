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
    years = get_years(ev)
    di = ev['secepoc_ini']
    df = to_secepoc(datetime.datetime(years[-1]+1, 1, 1, 0, 0, 0))
    time_array_all = time_series(ev, di, df)
    state = "init"  # state is either "init" if first time accounting or "upd"
    projects_in = list()  # fill list of project ID when processing instances or volumes
    array_metrics = dict()  # array with metrics for each project
    p_dict = get_projects(di, df, state)
    process_inst(di, df, time_array_all, array_metrics, p_dict, projects_in, state)
    process_vol(di, df, time_array_all, array_metrics, p_dict, projects_in, state)
    directory = os.path.dirname(ev['out_dir'])
    if not os.path.exists(directory):
        os.makedirs(directory, 0755)

    for year in years:
        print 80*'='
        print year
        filename = create_hdf_year(ev, year)
        with h5py.File(filename, 'r+') as f:
            ts = f['date'][:]
            idx_start = time2index(ev, ts[0], time_array_all)
            idx_end = time2index(ev, ts[-1], time_array_all) + 1
            for proj_id in projects_in:
                create_proj_datasets(year, proj_id)
                grp_name = p_dict[proj_id][0]
                for metric in METRICS:
                    data_array = f[grp_name][metric]
                    data_array[:] = array_metrics[grp_name][metric][idx_start:idx_end]

            tnow = now_acc()
            f.attrs['LastRun'] = tnow
            f.attrs['LastRunUTC'] = str(to_isodate(tnow))
            if year < years[-1]:
                f.attrs['LastRun'] = ts[-1]
                f.attrs['LastRunUTC'] = str(to_isodate(ts[-1]))
