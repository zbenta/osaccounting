# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Send accounting data to graphite
"""

import socket
import struct
import sys
try:
    import cPickle as pickle
except ImportError:
    import pickle

from osacc_functions import *

if __name__ == '__main__':
    env = get_env()
    carbon_server = env['carbon_server']
    carbon_port = int(env['carbon_port'])
    ini_list = 1000 # size of list to initialize
    years = get_years()
    # years = [2017]
    delay = 0  # seconds delay to close connection
    max_retries = 3  # number of retries for socket connect
    timeout = 3  # seconds between retries for socket connect
    for year in years:
        print 80 * "="
        filename = get_hdf_filename(year)
        print " Filename = ", filename
        with h5py.File(filename, 'r') as f:
            ti = f.attrs['LastRun']
            ts = f['date'][:]
            len_ds = len(ts)
            for group in f:
                if group == "date":
                    continue
                print "--> Group = ", group
                for m in METRICS:
                    graph_list = list()
                    print "--> Metric = ", m
                    data = f[group][m]
                    metric_str = env['graph_ns'] + "." + str(group) + "." + str(m)
                    for i in range(len_ds):
                        graph_string = metric_str + " " + str(data[i]) + " " + str(int(ts[i])) + "\n"
                        value = int(data[i])
                        timestamp = int(ts[i])
                        metric = str(metric_str)
                        graph_ds = (metric, (timestamp, value))
                        graph_list.append(graph_ds)
                        if i % ini_list == 0:
                            # pprint.pprint(graph_list)
                            package = pickle.dumps(graph_list, protocol=2)
                            size = struct.pack('!L', len(package))
                            print i, " Size of pickle = ", len(package), " ListSize = ", len(graph_list)
                            message = size + package
                            sock = socket.socket()
                            for j in range(max_retries):
                                try:
                                    sock.connect((carbon_server, carbon_port))
                                    sock.sendall(message)
                                    time.sleep(delay)
                                except IOError as e:
                                    print "Couldn't connect to %(server)s on port %(port)d. Retry: %(j)i" % {
                                        'server': carbon_server, 'port': carbon_port, 'j': j}
                                    continue
                                else:
                                    sock.close()
                                    break

                            # try:
                            #     sock.connect((carbon_server, carbon_port))
                            # except:
                            #     print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % {
                            #         'server': carbon_server, 'port': carbon_port}
                            #     sys.exit(1)

                            graph_list = list()

