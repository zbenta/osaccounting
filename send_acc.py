# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Send accounting data to graphite
"""

from osacc_functions import *

if __name__ == '__main__':
    years = get_years()
    for year in years:
        print 80 * "="
        filename = get_hdf_filename(year)
        print " Filename = ", filename
        with h5py.File(filename, 'r') as f:
            ti = f.attrs['LastRun']
            ts = f['date'][:]
            for group in f:
                print "--> Group = ", group
                for m in METRICS:
                    print "--> Metric = ", m
                    data = f[group][m]
                    metric_str = group + "." + m
                    for i in range(4):
                        print metric_str, data[i], ts[i]

