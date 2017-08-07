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
SECEPOC = time.mktime(DATEINI.timetuple())
# Interval of data points in seconds
DELTA = 3600

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
    return time.mktime(date.timetuple())


def to_isodate(date):
    return datetime.datetime.fromtimestamp(date)


def time_series(year=2016):
    """Create a time array (of ints) in epoch format with interval
    of one hour for a given year
    :param year: Year
    :returns (numpy array) time_array
    """
    di = to_secepoc(datetime.datetime(year, 1, 1, 0, 0, 0))
    df = to_secepoc(datetime.datetime(year+1, 1, 1, 0, 0, 0))
    n = (df - di)/DELTA
    time_array = numpy.arange(int(di), int(df), DELTA)
    return time_array


def size_array(year=2016):
    """Number of data points is the size of the arrays for 1 year
    :param year: Year
    :return (int) size of arrays"""
    sizea = time_series(year)
    return sizea.size


def set_hdf_fnames():
    """List of HDF5 filenames, are the <YEAR>.hdf
    :returns (list) of filename
    """
    tf = datetime.datetime.now()
    l = range(DATEINI.year, tf.year + 1)
    fname = []
    for i in l:
        fname.append(str(i) + '.hdf')
    return fname


def set_hdf_grp(proj, user, metric):
    """Creates the HDF5 group structure
    :param proj: project name
    :param user: user name
    :param metric: metric name
    :return hdf5 group
    """
    grp = proj + '/' + user + '/' + metric
    return grp


def create_metric_array(year=2016):
    """Create array for a given metric
    :param year: Year to calculate the size of the array
    :return (numpy array) Array to hold the values of the metric"""
    sizea = size_array(year)
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
    json_proj = evr['out_dir'] + os.sep + 'projects.json'
    with open(json_proj, 'r') as f:
        projects = json.load(f)

    fn = set_hdf_fnames()
    ts = time_series()
    sa = size_array()

    for proj in projects:
        # Create the arrays for metrics
        a_vcpus = create_metric_array()
        a_mem_mb = create_metric_array()
        a_disk_gb = create_metric_array()
        a_volume_gb = create_metric_array()
        print 80*'-'
        print proj['Name']
        nova = get_nova_client(proj['Name'])

        for i in range(4000):
            aux = nova.usage.get(proj['Name'], to_isodate(ts[i]), to_isodate(ts[i+1]))
            usg = getattr(aux, "server_usages", [])
            for u in usg:
                print i, to_isodate(ts[i]), to_isodate(ts[i+1]), u["vcpus"], u["memory_mb"]
                a_vcpus[i] += u["vcpus"]
                a_mem_mb[i] += u["memory_mb"]
                a_disk_gb[i] += u["local_gb"]
        print 'VPUS: ', a_vcpus
        print 'MEM: ', a_mem_mb
        print 'Disk: ', a_disk_gb



        # prints vars
    #print 'FILENAME: ', fn
    #print 'TIME SERIES: ', ts
    #print 'Array size: ', sa
    #for p in projects:
    #    print 80*'-'
    #    print p
