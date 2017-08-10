# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Reads and updates volumes datasets
   Old/deleted volumes are not accessible through the cinder API
   Connects to the cinder DB to read volume information and update
   the datasets of hdf5
"""

from osacc_functions import *

if __name__ == '__main__':
    evr = get_env()
    years = get_years()
    projects = get_projects()

    for year in years:
        size_a = size_array(year)
        with h5py.File(evr['out_dir'] + os.sep + str(year) + '.hdf', 'r+') as f:
            ts = f['date'][:]
            print to_isodate(ts[0])
            volumes = get_volumes(year)
            for vol in volumes:
                print 80*'|'
                t_create = to_secepoc(vol["created_at"])
                t_final = ts[size_a-1]
                if vol["deleted"]:
                    t_final = to_secepoc(vol["deleted_at"])
                if t_final > to_secepoc(datetime.datetime.utcnow()):
                    t_final = to_secepoc(datetime.datetime.utcnow())
                idx_start, idx_end = dt_to_indexes(t_create, t_final, year)
                proj = filter(lambda pr: pr['ID'] == vol['project_id'], projects)
                print 80*'-'
                print "ProjID= ", vol['project_id'], " SizeGB= ", vol['size'], "  --ProjID from filter= ", proj
                print 'IDX_Start= ', idx_start, ' IDX_End= ', idx_end
                print 'Vol_Start= ', to_isodate(t_create), ' Vol_End= ', to_isodate(t_final)
                print 'TS_Start= ', ts[idx_start], ' TS_End= ', ts[idx_end]

            #for proj in projects:
                # print 20*'-'
                # print proj
            #    grp_name = proj['Name']
            #    vol_array = f[grp_name]['volume_gb']
            #    for i in range(size_a):
            #        vol_array[i] = i*2
