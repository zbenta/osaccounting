# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Create initial hdf5 files to store accounting data

    The project data structure is
    project = { "Description": None,
                "Domain ID": None,
                "Enabled": None,
                "ID": None,
                "Name": None
               }
"""


import pprint
import datetime
import os
import h5py
import json
import time
import numpy

from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client
import novaclient.client
import cinderclient.client


# Set the initial date to start the accounting -> 1st April 2016
DATEINI = datetime.datetime(2016, 4, 1, 0, 0, 0)
SECEPOC = time.gmtime(DATEINI.timetuple())
# Interval of data points in seconds
DELTA = 3600.0

ksauth = dict()
ksauth['project_domain_name'] = os.environ['OS_PROJECT_DOMAIN_NAME']
ksauth['user_domain_name'] = os.environ['OS_USER_DOMAIN_NAME']
ksauth['project_name'] = os.environ['OS_PROJECT_NAME']
ksauth['username'] = os.environ['OS_USERNAME']
ksauth['password'] = os.environ['OS_PASSWORD']
ksauth['auth_url'] = os.environ['OS_AUTH_URL']
ksauth['identity_api_version'] = os.environ['OS_IDENTITY_API_VERSION']
ksauth['image_api_version'] = os.environ['OS_IMAGE_API_VERSION']
ksauth['cacert'] = os.environ['OS_CACERT']


def get_env():
    """Get environment variables
    :returns dictionary with environment variables
    """
    ev = dict()
    ev['out_dir'] = os.environ['OUT_DIR']
    return ev


def to_secepoc(date=DATEINI):
    """Converts datetime to seconds from epoc
    :param date: Date in datetime format
    :returns (float) seconds from epoch
    """
    return time.gmtime(date.utctimetuple())


def to_isodate(date):
    return datetime.datetime.utcfromtimestamp(date)


def time_series(year_l=2016):
    """Create a time array (of ints) in epoch format with interval
    of one hour for a given year
    :param year_l: Year
    :returns (numpy array) time_array
    """
    month = 1
    if year_l == 2016:
        month = 4
    di = to_secepoc(datetime.datetime(year_l, month, 1, 0, 0, 0))
    df = to_secepoc(datetime.datetime(year_l+1, 1, 1, 0, 0, 0))
    time_array = numpy.arange(di, df, DELTA)
    return time_array


def size_array(year_l=2016):
    """Number of data points is the size of the arrays for 1 year
    :param year_l: Year
    :return (int) size of arrays"""
    sizea = time_series(year_l)
    return sizea.size


def get_years():
    """List of years
    :returns (list) of years
    """
    tf = datetime.datetime.utcnow()
    return range(DATEINI.year, tf.year + 1)


def create_metric_array(year_l=2016):
    """Create array for a given metric
    :param year_l: Year to calculate the size of the array
    :return (numpy array) Array to hold the values of the metric"""
    sizea = size_array(year_l)
    return numpy.zeros([sizea, ], dtype=int)


def get_keystone_session(project_name=ksauth['project_name']):
    """Get keystone client session

    :param project_name: Project name
    """
    auth = v3.Password(auth_url=ksauth['auth_url'],
                       username=ksauth['username'],
                       password=ksauth['password'],
                       project_domain_name=ksauth['project_domain_name'],
                       project_name=project_name,
                       user_domain_name=ksauth['user_domain_name'])
    return session.Session(auth=auth)


def get_keystone_client():
    sess = get_keystone_session()
    return client.Client(session=sess)


def get_users():
    dusers = dict()
    ks = get_keystone_client()
    for usr in ks.users.list():
        dusers[usr.id] = usr.name
    return dusers


def get_nova_client(project_name):
    sess = get_keystone_session(project_name)
    return novaclient.client.Client(2, session=sess)


def get_cinder_client(project_name):
    sess = get_keystone_session(project_name)
    return cinderclient.client.Client(2, session=sess)


def get_last_run():
    last_run = datetime.datetime(2010, 1, 1, 0, 0, 0)
    return last_run

if __name__ == '__main__':
    evr = get_env()
    metrics = ['vcpus', 'mem_mb', 'disk_gb', 'volume_gb']
    json_proj = evr['out_dir'] + os.sep + 'projects.json'
    with open(json_proj, 'r') as f:
        projects = json.load(f)

    # Set the list of years for testing purposes
    years = [2016]

    # Get the list of years
    #years = get_years()

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
                    print 5*'>'
                    print 'index= ', i, ' DATE= ', to_isodate(ts[i]), 'EPOCH= ', ts[i]
                    #pprint.pprint(usg)
                    print 5*'<'
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
                print 'Date: ', to_isodate(ts[sa-1])
                print 'VPUS: ', a_vcpus[idx_i:idx_f]
                print 'MEM: ', a_mem_mb[idx_i:idx_f]
                print 'Disk: ', a_disk_gb[idx_i:idx_f]
