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
    volumes = get_volumes()
    pprint.pprint(volumes)
    years = get_years()
    projects = get_projects()
    for year in years:
        size_a = size_array(year)
        with h5py.File(evr['out_dir'] + os.sep + str(year) + '.hdf', 'r+') as f:
            ts = f['date'][:]
            print ts[0:5]
            for proj in projects:
                print 20*'-'
                print proj
                grp_name = proj['Name']
                vol_array = f[grp_name]['volume_gb'][:]
                vcpus = f[grp_name]['vcpus'][:]
                for i in range(size_a):
                    f[grp_name]['volume_gb'][i] = i*2
