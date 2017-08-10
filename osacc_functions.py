# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Functions and utils for the Openstack accounting
"""


import pprint
import datetime
import os
import h5py
import json
import time
import numpy
import mysql.connector

from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client
import novaclient.client
import cinderclient.client


# Set the initial date to start the accounting -> 1st April 2016
DATEINI = datetime.datetime(2016, 4, 1, 0, 0, 0)
SECEPOC = time.mktime(DATEINI.utctimetuple())
# Interval of data points in seconds
DELTA = 3600.0*24.0

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
    ev['mysql_user'] = os.environ['MYSQL_USER']
    ev['mysql_pass'] = os.environ['MYSQL_PASS']
    ev['mysql_host'] = os.environ['MYSQL_HOST']
    return ev


def db_conn():
    ev = get_env()
    return mysql.connector.connect(host=ev['mysql_host'],
                                   user=ev['mysql_user'],
                                   passwd=ev['mysql_pass'],
                                   db="cinder")


def get_projects():
    """Get json with all projects
    :return (json dict) all projects
    """
    evr = get_env()
    json_proj = evr['out_dir'] + os.sep + 'projects.json'
    with open(json_proj, 'r') as f:
        projects = json.load(f)
    return projects


def get_volumes(year=2016):
    conn = db_conn()
    cursor = conn.cursor()
    ti = datetime.date(year, 1, 1)
    tf = datetime.date(year, 12, 31)
    dbtable = "volumes"
    sep = ","
    table_coll = ("created_at", "deleted_at", "deleted", "id", "user_id", "project_id", "size", "status")
    s = len(table_coll)
    qry = "SELECT " + sep.join(table_coll) + " FROM " + dbtable + " "
    cond_qry = "WHERE created_at BETWEEN %s AND %s"
    query = (qry + cond_qry)
    cursor.execute(query, (ti, tf))
    vols = cursor.fetchall()
    vols_list = []
    for v in vols:
        vd = dict()
        for i in range(s):
            vd[table_coll[i]] = v[i]
        vols_list.append(vd)
    return vols_list


def to_secepoc(date=DATEINI):
    """Converts datetime to seconds from epoc
    :param date: Date in datetime format
    :returns (float) seconds from epoch
    """
    return time.mktime(date.utctimetuple())


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
    :return (list) of years
    """
    tf = datetime.datetime.utcnow()
    return range(DATEINI.year, tf.year + 1)


def create_metric_array(year_l=2016):
    """Create array for a given metric
    :param year_l: Year to calculate the size of the array
    :return (numpy array) Array to hold the values of the metric
    """
    sizea = size_array(year_l)
    return numpy.zeros([sizea, ], dtype=int)


def dt_to_indexes(ti, tf, year):
    """For a given date in seconds to epoch return the
    corresponding index in the time_series
    :param ti: initial date in seconds to epoch in UTC
    :param tf: final date in seconds to epoch in UTC
    :param year: year
    :return (list int) list of indexes in time series corresponding to given time interval
    """
    ts = time_series(year)
    print '$$$$$$$$$$$$$$$$$$$ INSIDE FUNCTION $$$$$$$$$$$$$$$$$$$$$'
    print 'ti= ', ti, ' tf= ', tf, ' year= ', year
    print 'ts= ', ts[:]
    idxs = numpy.argwhere((ts >= ti))
    print 'idxs= ', idxs
    print '$$$$$$$$$$$$$$$$$$$ INSIDE FUNCTION $$$$$$$$$$$$$$$$$$$$$'
    return idxs


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

