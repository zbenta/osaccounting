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
    carbon_port = int(env['carbon_port'])
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
            for group in f:
                if group == "date":
                    continue
                print "--> Group = ", group
                graph_list = []
                for m in METRICS:
                    # print "--> Metric = ", m
                    data = f[group][m]
                    metric_str = GRAPH_NS + "." + group + "." + m
                    for i in range(326819, 326979):
                    # for i in range(50):
                        # sock = socket.socket()
                        # sock.connect((carbon_server, carbon_port))
                        graph_string = metric_str + " " + str(data[i]) + " " + str(int(ts[i])) + "\n"
                        # sock.sendall(graph_string)
                        # sock.close()
                        graph_ds = (metric_str, (int(ts[i]), data[i]))
                        graph_list.append(graph_ds)

                pprint.pprint(graph_list)
                package = pickle.dumps(graph_list, protocol=1)
                size = struct.pack('!L', len(package))
                print "Size of pickle = ", len(package)
                message = size + package
                sock = socket.socket()
                sock.connect((carbon_server, carbon_port))
                sock.sendall(message)
                sock.close()
                # time.sleep(delay)


