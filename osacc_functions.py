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

# Set the initial date to start the accounting -> 1st March 2016
MONTH_INI = 3
YEAR_INI = 2016
DATEINI = datetime.datetime(YEAR_INI, MONTH_INI, 1, 0, 0, 0)
SECEPOC = time.mktime(DATEINI.utctimetuple())
# Interval of data points in seconds
DELTA = 3600.0*24.0
# List of metrics
METRICS = ['vcpus', 'mem_mb', 'disk_gb', 'volume_gb']


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


def create_hdf():
    """Initial creation of hdf5 files containing 1 group per project and datasets
    for each metric and for each project/group.
    The size of the datasets depend on the year.
    Attributes are set for each hdf5 group (project) with project ID and Description
    """
    evr = get_env()
    projects = get_list_db(2016, "keystone", "project")
    # Get the list of years
    years = get_years()
    for year in years:
        ts = time_series(year)
        with h5py.File(evr['out_dir'] + os.sep + str(year) + '.hdf', 'w') as f:
            f.create_dataset('date', data=ts, compression="gzip")
            for proj in projects:
                grp_name = proj['name']
                grp = f.create_group(grp_name)
                grp.attrs['ProjID'] = proj['id']
                grp.attrs['ProjDescription'] = proj['description']
                a = create_metric_array(year)
                for m in METRICS:
                    grp.create_dataset(m, data=a, compression="gzip")


def db_conn(database="nova"):
    ev = get_env()
    return mysql.connector.connect(host=ev['mysql_host'],
                                   user=ev['mysql_user'],
                                   passwd=ev['mysql_pass'],
                                   db=database)


def get_projects():
    """Get json with all projects
    :return (json dict) all projects
    """
    evr = get_env()
    json_proj = evr['out_dir'] + os.sep + 'projects.json'
    with open(json_proj, 'r') as f:
        projects = json.load(f)
    return projects


def get_list_db(year=2016, database="keystone", dbtable="project"):
    """Query keystone to get projects
    :param year: Year
    :param database: database to query
    :param dbtable: database table to query
    :return (json dict) all projects
    """

    # Query -- SELECT id,name,description,enabled
    #          FROM project
    #          WHERE (domain_id='default' and name!='admin' and name!='service');
    # do not take into account admin and service projects

    # TODO: variables that will come as arguments to the function
    table_str = "id,name,description,enabled"
    condition = "domain_id='default' AND name!='admin' AND name!='service'"

    conn = db_conn(database)
    cursor = conn.cursor()

    # Date intervals are need for volumes and instances, not needed for projects
    month = 1
    if year == 2016:
        month = 3
    ti = datetime.date(year, month, 1)
    tf = datetime.date(year, 12, 31)

    sep = ","
    table_coll = table_str.strip(sep).split(sep)
    s = len(table_coll)
    qry = "SELECT " + table_str + " FROM " + dbtable + " "
    cond_qry = "WHERE (" + condition + ")"
    query = (qry + cond_qry)

    # cursor.execute(query, (ti, tf))
    cursor.execute(query)

    rows = cursor.fetchall()
    rows_list = []
    for r in rows:
        rd = dict()
        for i in range(s):
            rd[table_coll[i]] = r[i]
        rows_list.append(rd)
    return rows_list


def get_instances(year=2016):
    conn = db_conn("nova")
    cursor = conn.cursor()
    month = 1
    if year == 2016:
        month = 3
    ti = datetime.date(year, month, 1)
    tf = datetime.date(year, 12, 31)
    dbtable = "instances"
    sep = ","
    table_str = "uuid,created_at,deleted_at,id,project_id,vm_state,memory_mb,vcpus,root_gb"
    table_coll = table_str.strip(sep).split(sep)
    s = len(table_coll)
    qry = "SELECT " + table_str + " FROM " + dbtable + " "
    cond_qry = "WHERE (vm_state != 'error' AND created_at BETWEEN %s AND %s)" % (ti, tf)
    query = (qry + cond_qry)
    cursor.execute(query)
    insts = cursor.fetchall()
    insts_list = []
    for v in insts:
        vd = dict()
        for i in range(s):
            vd[table_coll[i]] = v[i]
        insts_list.append(vd)

    print 80*"x"
    print "Function get_instances ", year, ti, tf
    print "insts="
    pprint.pprint(insts)
    print "---"
    print "insts_list="
    pprint.pprint(insts_list)
    print 80*"x"

    return insts_list


def get_volumes(year=2016):
    conn = db_conn("cinder")
    cursor = conn.cursor()
    month = 1
    if year == 2016:
        month = 3
    ti = datetime.date(year, month, 1)
    tf = datetime.date(year, 12, 31)
    dbtable = "volumes"
    sep = ","
    table_coll = ("created_at", "deleted_at", "deleted", "id", "user_id", "project_id", "size", "status")
    s = len(table_coll)
    qry = "SELECT " + sep.join(table_coll) + " FROM " + dbtable + " "
    cond_qry = "WHERE created_at BETWEEN %s AND %s" % (ti, tf)
    query = (qry + cond_qry)
    cursor.execute(query)
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
        month = 3
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
    :return (int, int) index start of interval and end of interval in time series
    """
    ts = time_series(year)
    idxs_i = numpy.argwhere((ts > ti))
    idx_ini = idxs_i[0][0] - 1
    idx_fin = size_array(year) - 1
    if tf < ts[-1]:
        idxs_f = numpy.argwhere((ts < tf))
        idx_fin = idxs_f[-1][0] + 1
    return idx_ini, idx_fin


def get_last_run():
    last_run = datetime.datetime(2010, 1, 1, 0, 0, 0)
    return last_run

