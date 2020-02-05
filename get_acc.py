#!/usr/bin/env python3

# -*- encoding: utf-8 -*-
#
# Copyright 2020 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#

"""Create or update hdf5 files with accounting data

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
import os
import datetime
import h5py
import osacc_functions as oaf


if __name__ == '__main__':

    ev = oaf.get_conf()
    state = "upd"  # state is "upd" accouting by default
    di = ev['secepoc_ini']
    filename = oaf.get_hdf_filename(ev)
    year = ev['year_ini']
    if not os.path.exists(filename):
        state = "init"  # state is "init" if first time accounting
        if not os.path.exists(ev['out_dir']):
            os.makedirs(ev['out_dir'], 0o755)
        filename = oaf.create_hdf(ev, year)

    with h5py.File(filename, 'r+') as f:
        di = f.attrs['LastRun']
        proj_hdf = list(f.keys())

    df = oaf.now_acc()
    print(80*"-")
    print("Running update Openstack accounting: ", oaf.to_isodate(df))
    proj_hdf.remove("date")
    time_array_all = oaf.time_series(ev, di, df)
    projects_in = list()  # fill list of project ID when processing instances or volumes
    array_metrics = dict()  # array with metrics for each project
    p_dict = oaf.get_projects(di, state)
    oaf.process_inst(ev, di, df, time_array_all, array_metrics, p_dict, projects_in, state)
    oaf.process_vol(ev, di, df, time_array_all, array_metrics, p_dict, projects_in, state)
    with h5py.File(filename, 'r+') as f:
        ts = f['date'][:]
        idx_start = oaf.time2index(ev, di, time_array_all)
        idx_end = oaf.time2index(ev, df, time_array_all) + 1
        idx_start_ds = oaf.time2index(ev, di, ts)
        idx_end_ds = oaf.time2index(ev, df, ts) + 1
        print(80*"-")
        print("Size time_array_all: ", len(time_array_all))
        print("Size ts: ", len(ts))
        print("idx_start:", idx_start, "idx_end:", idx_start, "idx_start_ds:", idx_start_ds, "idx_end_ds:", idx_end_ds)
        print(80*"-")
        for proj_id in projects_in:
            grp_name = p_dict[proj_id][0]
            if grp_name not in proj_hdf:
                oaf.create_proj_datasets(ev, year, proj_id, p_dict)

            for metric in oaf.METRICS:
                data_array = f[grp_name][metric]
                data_array[idx_start_ds:idx_end_ds] = array_metrics[grp_name][metric][idx_start:idx_end]

        f.attrs['LastRun'] = ts[idx_end_ds - 1]
        f.attrs['LastRunUTC'] = str(oaf.to_isodate(ts[idx_end_ds - 1]))
