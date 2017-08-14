# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Send accounting data to graphite
"""

import socket
import pickle
import struct
from osacc_functions import *

if __name__ == '__main__':
    env = get_env()
    carbon_server = env['carbon_server']
    carbon_port = 2004
    # years = get_years()
    years = [2017]
    delay = 10  # 10 seconds delay
    for year in years:
        print 80 * "="
        filename = get_hdf_filename(year)
        print " Filename = ", filename
        with h5py.File(filename, 'r') as f:
            ti = f.attrs['LastRun']
            ts = f['date'][:]
            graph_list = []
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
                        graph_ds = (metric_str, (int(ts[i]), data[i]))
                        graph_list.append(graph_ds)

            print graph_list
            package = pickle.dumps(graph_list, 2)
            size = struct.pack('!L', len(package))
            message = size + package
            sock = socket.socket()
            sock.connect((carbon_server, carbon_port))
            sock.sendall(message)
            time.sleep(delay)

            # sock.sendall(graph_string)
            sock.close()

