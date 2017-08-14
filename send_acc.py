# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Send accounting data to graphite
"""

import socket
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
                if group == "date":
                    continue
                print "--> Group = ", group
                for m in METRICS:
                    print "--> Metric = ", m
                    data = f[group][m]
                    metric_str = GRAPH_NS + "." + group + "." + m
                    for i in range(20):
                        graph_string = metric_str + " " + str(data[i]) + " " + str(int(ts[i])) + "\n"
                        print graph_string

