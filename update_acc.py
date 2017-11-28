# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Update accounting
"""

from osacc_functions import *

if __name__ == '__main__':
    ev = get_conf()
    y = datetime.datetime.utcnow()
    year_now = y.year
    di = to_secepoc(datetime.datetime(year_now[-1], 1, 1, 0, 0, 0))
    df = now_acc()
    filename = get_hdf_filename(year_now)
    if not exists_hdf(year_now):
        filename = create_hdf_year(year_now)

    with h5py.File(filename, 'r+') as f:
        di = f.attrs['LastRun']
        proj_hdf = f.keys()

    print "Timestamp Ini = ", di, " hdf groups/project = ", proj_hdf
    time_array_all = time_series(di, df)
    state = "upd"  # state is either "init" if first time accounting or "upd"
    projects_in = list()  # fill list of project ID when processing instances or volumes
    array_metrics = dict()  # array with metrics for each project
    process_inst(di, time_array_all, array_metrics, projects_in, state)
    process_vol(di, time_array_all, array_metrics, projects_in, state)
    p_dict = get_projects(di, state)
    with h5py.File(filename, 'r+') as f:
        ts = f['date'][:]
        idx_start = time2index(di, time_array_all)
        idx_end = time2index(df, time_array_all) + 1
        idx_start_ds = time2index(di, ts)
        idx_end_ds = time2index(df, ts) + 1
        for proj_id in projects_in:
            grp_name = p_dict[proj_id][0]
            if grp_name not in proj_hdf:
                create_proj_datasets(year_now, proj_id)

            for metric in METRICS:
                data_array = f[grp_name][metric]
                data_array[idx_start_ds:idx_end_ds] = array_metrics[grp_name][metric][idx_start:idx_end]

        f.attrs['LastRun'] = df
        f.attrs['LastRunUTC'] = str(to_isodate(df))
