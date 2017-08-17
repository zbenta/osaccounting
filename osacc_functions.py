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
import time
import numpy
import mysql.connector

# Set the initial date to start the accounting -> 1st March 2016
MONTH_INI = 3
YEAR_INI = 2016
DATEINI = datetime.datetime(YEAR_INI, MONTH_INI, 1, 0, 0, 0)
SECEPOC = time.mktime(DATEINI.utctimetuple())
# Interval of data points in seconds
DELTA = 60.0
# List of metrics
METRICS = ['vcpus', 'mem_mb', 'disk_gb', 'volume_gb', 'ninstances', 'nvolumes', 'npublic_ips']
# Graphite namespace
GRAPH_NS = "os_accounting"


def get_env():
    """Get environment variables
    :returns dictionary with environment variables
    """
    ev = dict()
    ev['out_dir'] = os.environ['OUT_DIR']
    ev['mysql_user'] = os.environ['MYSQL_USER']
    ev['mysql_pass'] = os.environ['MYSQL_PASS']
    ev['mysql_host'] = os.environ['MYSQL_HOST']
    ev['carbon_server'] = os.environ['CARBON_SERVER']
    ev['carbon_port'] = os.environ['CARBON_PORT']
    return ev


def get_hdf_filename(year=YEAR_INI):
    """Get the HDF5 filename
    :param year: Year
    :return (string) filename
    """
    evr = get_env()
    return evr['out_dir'] + os.sep + str(year) + '.hdf'


def create_hdf(year=YEAR_INI):
    """Initial creation of hdf5 files containing 1 group per project and datasets
    for each metric and for each project/group.
    The size of the datasets depend on the year.
    Attributes are set for each hdf5 group (project) with project ID and Description
    One file is created per year
    :param year: Year
    :return (string) file_name
    """
    month = 1
    if year == YEAR_INI:
        month = MONTH_INI
    di = to_secepoc(datetime.datetime(year, month, 1, 0, 0, 0))
    projects = get_list_db("keystone", "project")
    ts = time_series(year)
    file_name = get_hdf_filename(year)
    with h5py.File(file_name, 'w') as f:
        f.create_dataset('date', data=ts, compression="gzip")
        f.attrs['LastRun'] = di
        for proj in projects:
            grp_name = proj['name']
            grp = f.create_group(grp_name)
            grp.attrs['ProjID'] = proj['id']
            grp.attrs['ProjDescription'] = proj['description']
            a = create_metric_array(year)
            for m in METRICS:
                grp.create_dataset(m, data=a, compression="gzip")
    return file_name


def exists_hdf(year=YEAR_INI):
    """Checks if hdf5 file exists
    :param year: Year
    :return (boolean) true is file exists or false if it doesn't"""
    return os.path.exists(get_hdf_filename(year))
    

def db_conn(database="nova"):
    ev = get_env()
    return mysql.connector.connect(host=ev['mysql_host'],
                                   user=ev['mysql_user'],
                                   passwd=ev['mysql_pass'],
                                   db=database)


def get_list_db(database="keystone", dbtable="project"):
    """Query keystone or nova or cinder to get projects or instances or volumes

    For projects (do not take into account admin and service projects)
    SELECT id,name,description,enabled
    FROM project
    WHERE (domain_id='default' and name!='admin' and name!='service')

    For instances
    SELECT uuid,created_at,deleted_at,id,project_id,vm_state,memory_mb,vcpus,root_gb
    FROM instances
    WHERE (vm_state != 'error')

    For volumes
    SELECT created_at,deleted_at,deleted,id,user_id,project_id,size,status
    FROM volumes

    :param database: database to query
    :param dbtable: database table to query
    :return (json dict) all projects
    """
    year = YEAR_INI
    month = MONTH_INI
    ti = datetime.date(year, month, 1)
    tf = datetime.date(year, 12, 31)

    table_str = "id,name,description,enabled"
    condition = "domain_id='default' AND name!='admin' AND name!='service'"
    if dbtable == "instances":
        table_str = "uuid,created_at,deleted_at,id,project_id,vm_state,memory_mb,vcpus,root_gb"
        condition = "vm_state != 'error' AND created_at BETWEEN %s AND %s"
    if dbtable == "volumes":
        table_str = "created_at,deleted_at,deleted,id,user_id,project_id,size,status"
        condition = "created_at BETWEEN %s AND %s"

    conn = db_conn(database)
    cursor = conn.cursor()
    sep = ","
    table_coll = table_str.strip(sep).split(sep)
    s = len(table_coll)
    qry = "SELECT " + table_str + " FROM " + dbtable + " "
    cond_qry = "WHERE (" + condition + ")"
    query = (qry + cond_qry)
    if dbtable == "project":
        cursor.execute(query)
    else:
        cursor.execute(query, (ti, tf))

    rows = cursor.fetchall()
    rows_list = []
    for r in rows:
        rd = dict()
        for i in range(s):
            rd[table_coll[i]] = r[i]
        rows_list.append(rd)

    return rows_list


def update_list_db(ti, database="keystone", dbtable="project"):
    """Query keystone or nova or cinder to get projects or instances or volumes

    DB = keystone: For projects (do not take into account admin and service projects)
    SELECT id,name,description,enabled
    FROM project
    WHERE (domain_id='default' and name!='admin' and name!='service')

    DB = nova: For instances
    SELECT uuid,created_at,deleted_at,id,project_id,vm_state,memory_mb,vcpus,root_gb
    FROM instances
    WHERE (vm_state != 'error' AND (created_at >= ti OR vm_state = 'active' ))

    DB = cinder: For volumes
    SELECT created_at,deleted_at,deleted,id,user_id,project_id,size,status
    FROM volumes
    WHERE created_at >= '2017-06-01' OR status != 'deleted'

    DB = neutron: For public IPs
    SELECT tenant_id,id,floating_ip_address,status
    FROM floatingips
    WHERE status='ACTIVE'

    :param ti: date start
    :param database: database to query
    :param dbtable: database table to query
    :return (json dict) all projects
    """
    tiso_i = to_isodate(ti)

    table_str = "id,name,description,enabled"
    condition = "domain_id='default' AND name!='admin' AND name!='service'"
    if dbtable == "instances":
        table_str = "uuid,created_at,deleted_at,id,project_id,vm_state,memory_mb,vcpus,root_gb"
        condition = "vm_state != 'error' AND (created_at >= %s OR vm_state = 'active' )"
    if dbtable == "volumes":
        table_str = "created_at,deleted_at,deleted,id,user_id,project_id,size,status"
        condition = "created_at >= %s OR status != 'deleted'"
    if dbtable == "floatingips":
        table_str = "tenant_id,id,floating_ip_address,status"
        condition = "status='ACTIVE'"

    conn = db_conn(database)
    cursor = conn.cursor()
    sep = ","
    table_coll = table_str.strip(sep).split(sep)
    s = len(table_coll)
    qry = "SELECT " + table_str + " FROM " + dbtable + " "
    cond_qry = "WHERE (" + condition + ")"
    query = (qry + cond_qry)
    if (dbtable == "project") or (dbtable == "floatingips"):
        cursor.execute(query)
    else:
        cursor.execute(query, (tiso_i,))

    rows = cursor.fetchall()
    rows_list = []
    for r in rows:
        rd = dict()
        for i in range(s):
            rd[table_coll[i]] = r[i]
        rows_list.append(rd)

    return rows_list


def now_acc():
    """
    :return: now in seconds from epoch
    """
    nacc = datetime.datetime.now()
    return to_secepoc(nacc)


def to_secepoc(date=DATEINI):
    """Converts datetime to seconds from epoc
    :param date: Date in datetime format
    :returns (float) seconds from epoch
    """
    return time.mktime(date.utctimetuple())


def to_isodate(date):
    return datetime.datetime.utcfromtimestamp(date)


def time_series(year_l=YEAR_INI):
    """Create a time array (of ints) in epoch format with interval
    of one hour for a given year
    :param year_l: Year
    :returns (numpy array) time_array
    """
    month = 1
    if year_l == YEAR_INI:
        month = MONTH_INI
    di = to_secepoc(datetime.datetime(year_l, month, 1, 0, 0, 0))
    df = to_secepoc(datetime.datetime(year_l+1, 1, 1, 0, 0, 0))
    time_array = numpy.arange(di, df, DELTA)
    return time_array


def size_array(year_l=YEAR_INI):
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


def create_metric_array(year_l=YEAR_INI):
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
    last_run = datetime.datetime(YEAR_INI, 1, 1, 0, 0, 0)
    return last_run

