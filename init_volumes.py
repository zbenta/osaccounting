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


import pprint
import datetime
import os
import h5py
import json
import time
import numpy
import MySQLdb

from create_hdf5 import to_secepoc, to_isodate, get_years

# Set the initial date to start the accounting -> 1st April 2016
DATEINI = datetime.datetime(2016, 4, 1, 0, 0, 0)
SECEPOC = time.mktime(DATEINI.utctimetuple())
# Interval of data points in seconds
DELTA = 3600.0


def get_env():
    """Get environment variables
    :returns dictionary with environment variables
    """
    ev = dict()
    ev['out_dir'] = os.environ['OUT_DIR']
    ev['mysql_user'] = os.environ['MYSQL_USER']
    ev['mysql_pass'] = os.environ['MYSQL_PASS']
    ev['mysql_host'] = os.environ['MYSQL_HOST']
    return ev


def db_conn():
    envr = get_env()
    return MySQLdb.connect(host=ev['mysql_host'],
                           user=ev['mysql_user'],
                           passwd=ev['mysql_pass'],
                           db="cinder")


if __name__ == '__main__':
    evr = get_env()
    conn = db_conn()
    metrics = ['vcpus', 'mem_mb', 'disk_gb', 'volume_gb']
    json_proj = evr['out_dir'] + os.sep + 'projects.json'
    with open(json_proj, 'r') as f:
        projects = json.load(f)

    # Set the list of years for testing purposes
    #years = [2016]

    # Get the list of years
    years = get_years()

    for year in years:
        ts = time_series(year)
        sa = size_array(year)

        # For testing purposes: indexes to check date intervals
        idx_i = sa-15
        idx_f = sa

        with h5py.File(evr['out_dir'] + os.sep + str(year) + '.hdf', 'w') as f:
            for proj in projects:
                grp_name = proj['Name']
                grp = f.create_group(grp_name)
                # Create the arrays for metrics
                a_vcpus = create_metric_array(year)
                a_mem_mb = create_metric_array(year)
                a_disk_gb = create_metric_array(year)
                a_volume_gb = create_metric_array(year)
                print 80*'-'
                print proj['Name']
                print 'Date= ', to_isodate(ts[0])
                nova = get_nova_client(proj['Name'])

                for i in range(sa):
                    aux = nova.usage.get(proj['ID'], to_isodate(ts[i]), to_isodate(ts[i]+DELTA))
                    usg = getattr(aux, "server_usages", [])
                    #print 5*'>'
                    #print 'index= ', i, ' DATE= ', to_isodate(ts[i]), 'EPOCH= ', ts[i]
                    #pprint.pprint(usg)
                    #print 5*'<'
                    for u in usg:
                        if u["state"] == "error":
                            continue
                        a_vcpus[i] = a_vcpus[i] + u["vcpus"]
                        a_mem_mb[i] = a_mem_mb[i] + u["memory_mb"]
                        a_disk_gb[i] = a_disk_gb[i] + u["local_gb"]
                        #print 'u[state]= ', u["state"], ' uvcpus= ',  u["vcpus"], ' a_vcpus[i]= ', a_vcpus[i]
                        #print 2*'_'

                res = grp.create_dataset('date', data=ts, compression="gzip")
                res = grp.create_dataset('vcpus', data=a_vcpus, compression="gzip")
                res = grp.create_dataset('mem_mb', data=a_mem_mb, compression="gzip")
                res = grp.create_dataset('disk_gb', data=a_disk_gb, compression="gzip")
                res = grp.create_dataset('volume_gb', data=a_volume_gb, compression="gzip")
                print 'Date: ', to_isodate(ts[sa-1])
                print 'VPUS: ', a_vcpus[idx_i:idx_f]
                print 'MEM: ', a_mem_mb[idx_i:idx_f]
                print 'Disk: ', a_disk_gb[idx_i:idx_f]
