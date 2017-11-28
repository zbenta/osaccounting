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
import time
import tzlocal
import os
import h5py
import numpy
import mysql.connector
import ConfigParser
import json
import math

# List of metrics
METRICS = ['vcpus', 'mem_mb', 'disk_gb', 'volume_gb', 'ninstances', 'nvolumes', 'npublic_ips']


def get_conf():
    """Get configuration options
    :returns dictionary with configuration options
    """
    parser = ConfigParser.SafeConfigParser(allow_no_value=True)
    parser.read('/etc/osacc.conf')
    ev = dict()
    ev['out_dir'] = parser.get('DEFAULT', 'OUT_DIR')
    ev['month_ini'] = parser.getint('DEFAULT', 'MONTH_INI')
    ev['year_ini'] = parser.getint('DEFAULT', 'YEAR_INI')
    ev['delta_time'] = parser.getfloat('DEFAULT', 'DELTA_TIME')
    ev['mysql_user'] = parser.get('mysql', 'MYSQL_USER')
    ev['mysql_pass'] = parser.get('mysql', 'MYSQL_PASS')
    ev['mysql_host'] = parser.get('mysql', 'MYSQL_HOST')
    dt_ini = datetime.datetime(ev['year_ini'], ev['month_ini'], 1, 0, 0, 0)
    ev['secepoc_ini'] = to_secepoc(dt_ini)

    # graphite section options are Optional
    if parser.has_option('graphite', 'CARBON_SERVER'):
        ev['carbon_server'] = parser.get('graphite', 'CARBON_SERVER')
        ev['carbon_port'] = parser.get('graphite', 'CARBON_PORT')
        ev['graph_ns'] = parser.get('graphite', 'GRAPH_NS')

    return ev


def get_years():
    """List of years
    :return (list) of years
    """
    ev = get_conf()
    tf = datetime.datetime.utcnow()
    return range(ev['year_ini'], tf.year + 1)


def get_hdf_filename(year):
    """Get the HDF5 filename
    :param year: Year
    :return (string) filename
    """
    ev = get_conf()
    return ev['out_dir'] + os.sep + str(year) + '.hdf'


def exists_hdf(year):
    """Checks if hdf5 file exists
    :param year: Year
    :return (boolean) true is file exists or false if it doesn't
    """
    return os.path.exists(get_hdf_filename(year))


def create_hdf_year(year):
    """Initial creation of hdf5 files containing the time_series dataset
    One file is created per year
    :param year: Year
    :return (string) file_name
    """
    ev = get_conf()
    di = to_secepoc(datetime.datetime(year, 1, 1, 0, 0, 0))
    df = to_secepoc(datetime.datetime(year+1, 1, 1, 0, 0, 0))
    if year == ev['year_ini']:
        di = ev['secepoc_ini']

    ts = time_series(di, df)
    file_name = get_hdf_filename(year)
    with h5py.File(file_name, 'w') as f:
        f.create_dataset('date', data=ts, compression="gzip")
        f.attrs['LastRun'] = di
        f.attrs['LastRunUTC'] = str(to_isodate(di))

    return file_name


def create_proj_datasets(year, proj_id):
    """Initial creation of metrics hdf5 dataset containing 1 group per project and datasets
    for each metric.
    Attributes are set for each hdf5 group (project) with project ID and Description
    The projects dictionary is
    proj_dict = { "proj_id": ["project name", "project description"], }
    proj_dict[proj_id][0] - gives the project name
    proj_dict[proj_id][1] - gives the project description
    :param year: Year
    :param proj_id: Project ID
    :return (string) file_name
    """
    ev = get_conf()
    di = to_secepoc(datetime.datetime(year, 1, 1, 0, 0, 0))
    df = to_secepoc(datetime.datetime(year+1, 1, 1, 0, 0, 0))
    if year == ev['year_ini']:
        di = ev['secepoc_ini']

    proj_dict = get_projects(di, "init")
    ts = time_series(di, df)
    file_name = get_hdf_filename(year)
    with h5py.File(file_name, 'r+') as f:
        grp_name = proj_dict[proj_id][0]
        grp = f.create_group(grp_name)
        grp.attrs['ProjID'] = proj_id
        grp.attrs['ProjDescription'] = proj_dict[proj_id][1]
        a = numpy.zeros([ts.size, ], dtype=int)
        for m in METRICS:
            grp.create_dataset(m, data=a, compression="gzip")

    return file_name


def to_secepoc(date):
    """Converts datetime to seconds from epoc
    :param date: Date in datetime format
    :returns (float) seconds from epoch
    """
    return time.mktime(date.utctimetuple())


def to_isodate(date):
    """Converts seconds from epoc to utc datetime
    :param date: Date in seconds to epoc format
    :returns (datetime) utc datetime
    """
    return datetime.datetime.utcfromtimestamp(date)


def time2index(ts, time_array):
    """
    Linear equation: index(t) = floor[1/Dt * (t-t0)]
    y = a + bx -> a = -t0/Dt  b = 1/Dt
    :param ts: timestamp in seconds to epoc
    :param time_array: array of timestamps
    :return: index
    """
    ev = get_conf()
    dt = ev['delta_time']
    t0 = time_array[0]
    index = int(math.floor(1.0 * (ts-t0)/dt))
    return index


def now_acc():
    """
    :return: now in seconds from epoch
    """
    nacc = datetime.datetime.utcnow()
    return to_secepoc(nacc)


def time_series(di, df):
    """Create a time array (of ints) in epoch format with interval
    of delta_time for all years
    :returns (numpy array) time_array
    """
    ev = get_conf()
    time_array = numpy.arange(di, df, ev['delta_time'])
    return time_array


def db_conn(database):
    ev = get_conf()
    return mysql.connector.connect(host=ev['mysql_host'],
                                   user=ev['mysql_user'],
                                   passwd=ev['mysql_pass'],
                                   db=database)


def get_list_db(ti, te, database, state):
    """Get the list of rows of table from database
    Query keystone, nova or cinder to get projects or instances or volumes
    For projects (do not take into account admin and service projects)
    DB = keystone -> Table = project
    DB = cinder   -> Table = volumes
    DB = nova     -> Table = instances and instance_info_caches
    :param ti: Initial Date Time in seconds to epoc
    :param te: End Date Time in seconds to epoc
    :param database: Database
    :param state: one of the two values - init (default), upd
    :return (json dict): List of rows of a table in the database
    """
    local_timezone = tzlocal.get_localzone()
    dtlocal_i = datetime.datetime.fromtimestamp(ti, local_timezone)
    dtlocal_e = datetime.datetime.fromtimestamp(te, local_timezone)

    # The condition is created_at >= date_time_local if in initialization
    # The condition is deleted_at < date_time_local if in update
    cnd_state = "created_at >= '%s'" % dtlocal_i
    cnd_state_nova = "instances.created_at >= '%s'" % dtlocal_i
    if state == "upd":
        cnd_state = "((deleted_at < '%s' AND deleted_at > '%s')" % (dtlocal_i, dtlocal_e)
        cnd_state_nova = "((instances.deleted_at < '%s' AND instances.deleted_at > '%s')" % (dtlocal_i, dtlocal_e)

    # Default to DB = keystone, dbtable = project
    dbtable = "project"
    table_str = "id,name,description,enabled"
    condition = "domain_id='default' AND name!='admin' AND name!='service'"
    if database == "cinder":
        dbtable = "volumes"
        table_str = "created_at,deleted_at,deleted,id,user_id,project_id,size,status"
        condition = cnd_state + " OR status != 'deleted'"

    table_coll = table_str.split(",")
    query = ' '.join((
        "SELECT " + table_str,
        "FROM " + dbtable,
        "WHERE " + condition
    ))
    if database == "nova":
        table_coll = ['uuid', 'created_at', 'deleted_at', 'id', 'project_id',
                      'vm_state', 'memory_mb', 'vcpus', 'root_gb', 'network_info']
        dbtable = "instances"
        table_str = "instances.uuid,instances.created_at,instances.deleted_at," \
                    "instances.id,instances.project_id,instances.vm_state,instances.memory_mb," \
                    "instances.vcpus,instances.root_gb,instance_info_caches.network_info"
        ijoin = "instance_info_caches ON uuid=instance_info_caches.instance_uuid"
        condition = "instances.vm_state != 'error' AND " \
                    "(" + cnd_state_nova + " OR instances.vm_state = 'active' )"
        query = ' '.join((
            "SELECT " + table_str,
            "FROM " + dbtable,
            "INNER JOIN " + ijoin,
            "WHERE " + condition
        ))

    print query
    return get_table_rows(database, query, table_coll)


def get_table_rows(database, query, table_coll):
    conn = db_conn(database)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    rows_list = []
    s = len(table_coll)
    for r in rows:
        rd = dict()
        for i in range(s):
            rd[table_coll[i]] = r[i]
        rows_list.append(rd)

    return rows_list


def get_projects(di, df, state):
    projects = get_list_db(di, df, "keystone", state)
    p_dict = dict()
    for proj in projects:
        p_dict[proj['id']] = [proj['name'], proj['description']]

    return p_dict


def process_inst(di, df, time_array, a, projects_in, state):
    """
    Process instances, create/update projects metrics arrays
    :param di:
    :param time_array:
    :param a: dictionary with array of metrics for each project
    :param projects_in:
    :param state:
    :return:
    """
    instances = get_list_db(di, df, "nova", state)
    print 80*"o"
    print "Instances selected from DB n = ", len(instances)
    pprint.pprint(instances)
    print 80*"o"
    p_dict = get_projects(di, df, state)
    for inst in instances:
        t_create = to_secepoc(inst["created_at"])
        t_final = now_acc()
        if inst["deleted_at"]:
            t_final = to_secepoc(inst["deleted_at"])

        idx_start = time2index(t_create, time_array)
        idx_end = time2index(t_final, time_array) + 1
        proj_id = inst['project_id']
        if proj_id not in p_dict:
            continue
        pname = p_dict[proj_id][0]
        if proj_id not in projects_in:
            projects_in.append(proj_id)
            a[pname] = dict()
            for m in METRICS:
                a[pname][m] = numpy.zeros([time_array.size, ], dtype=int)

        a[pname]['vcpus'][idx_start:idx_end] = a[pname]['vcpus'][idx_start:idx_end] + inst['vcpus']
        a[pname]['mem_mb'][idx_start:idx_end] = a[pname]['mem_mb'][idx_start:idx_end] + inst['memory_mb']
        a[pname]['disk_gb'][idx_start:idx_end] = a[pname]['disk_gb'][idx_start:idx_end] + inst['root_gb']
        a[pname]['ninstances'][idx_start:idx_end] = a[pname]['ninstances'][idx_start:idx_end] + 1
        net_info = json.loads(inst['network_info'])
        if net_info:
            for l in range(len(net_info)):
                for n in range(len(net_info[l]['network']['subnets'])):
                    for k in range(len(net_info[l]['network']['subnets'][n]['ips'])):
                        nip = len(net_info[l]['network']['subnets'][n]['ips'][k]['floating_ips'])
                        a[pname]['npublic_ips'][idx_start:idx_end] = a[pname]['npublic_ips'][idx_start:idx_end] + nip


def process_vol(di, df, time_array, a, projects_in, state):
    volumes = get_list_db(di, df, "cinder", state)
    p_dict = get_projects(di, df, state)
    for vol in volumes:
        t_create = to_secepoc(vol["created_at"])
        t_final = now_acc()
        if vol["deleted_at"]:
            t_final = to_secepoc(vol["deleted_at"])

        idx_start = time2index(t_create, time_array)
        idx_end = time2index(t_final, time_array) + 1
        proj_id = vol['project_id']
        if proj_id not in p_dict:
            continue
        pname = p_dict[proj_id][0]
        if proj_id not in projects_in:
            projects_in.append(proj_id)
            a[pname] = dict()
            for m in METRICS:
                a[pname][m] = numpy.zeros([time_array.size, ], dtype=int)

        a[pname]['volume_gb'][idx_start:idx_end] = a[pname]['volume_gb'][idx_start:idx_end] + vol['size']
        a[pname]['nvolumes'][idx_start:idx_end] = a[pname]['nvolumes'][idx_start:idx_end] + 1
