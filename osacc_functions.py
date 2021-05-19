# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Functions and utils for the Openstack accounting
"""

import datetime
import time
import tzlocal
import os
import h5py
import numpy
import mysql.connector
import configparser
import json
import math

# List of metrics
METRICS = ['vcpus', 'mem_mb', 'volume_gb',
           'ninstances', 'nvolumes', 'npublic_ips']


def get_conf():
    """Get configuration options
    :returns dictionary with configuration options
    """
    parser = configparser.SafeConfigParser(allow_no_value=True)
    parser.read('/etc/osacc.conf')
    ev = dict()
    ev['out_dir'] = parser.get('DEFAULT', 'OUT_DIR')
    ev['month_ini'] = parser.getint('DEFAULT', 'MONTH_INI')
    ev['year_ini'] = parser.getint('DEFAULT', 'YEAR_INI')
    ev['delta_time'] = parser.getfloat('DEFAULT', 'DELTA_TIME')
    ev['mysql_user'] = parser.get('mysql', 'MYSQL_USER')
    ev['mysql_pass'] = parser.get('mysql', 'MYSQL_PASS')
    ev['mysql_host'] = parser.get('mysql', 'MYSQL_HOST')
    ev['ufile_dir'] = parser.get('osinfo', 'UFILE_DIR')
    dt_ini = datetime.datetime(ev['year_ini'], ev['month_ini'], 1, 0, 0, 0)
    ev['secepoc_ini'] = to_secepoc(dt_ini)

    # graphite section options are Optional
    if parser.has_option('graphite', 'CARBON_SERVER'):
        ev['carbon_server'] = parser.get('graphite', 'CARBON_SERVER')
        ev['carbon_port'] = parser.getint('graphite', 'CARBON_PORT')
        ev['graph_ns'] = parser.get('graphite', 'GRAPH_NS')
        ev['send_inter'] = parser.getint('graphite', 'SEND_INTER')

    # elasticsearch section options are Optional
    if parser.has_option('elasticsearch', 'ESHOST'):
        ev['eshost'] = parser.get('elasticsearch', 'ESHOST')
        ev['esindex'] = parser.get('elasticsearch', 'ESINDEX')
        ev['esapikey'] = parser.get('elasticsearch', 'ESAPIKEY')

    # influxdb section options are Optional
    if parser.has_option('influxdb', 'DBHOST'):
        ev['dbhost'] = parser.get('influxdb', 'DBHOST')
        ev['dbport'] = parser.getint('influxdb', 'DBPORT')
        ev['dbuser'] = parser.get('influxdb', 'DBUSER')
        ev['dbpass'] = parser.get('influxdb', 'DBPASS')
        ev['dbname'] = parser.get('influxdb', 'DBNAME')
        ev['ssl'] = parser.getboolean('influxdb', 'SSL')
        ev['verify_ssl'] = parser.getboolean('influxdb', 'VERIFY_SSL')

    return ev


def get_years(ev):
    """List of years
    :param ev: configuration options
    :return (list) of years
    """
    tf = datetime.datetime.utcnow()
    return list(range(ev['year_ini'], tf.year + 1))


def get_hdf_filename(ev):
    """Get the HDF5 filename
    :param ev: configuration options
    :return (string) filename
    """
    return ev['out_dir'] + os.sep + 'osacc.hdf'

def create_hdf(ev, year):
    """Initial creation of hdf5 file containing the time_series dataset
    The file is for 10 years
    :param ev: configuration options
    :param year: Year
    :return (string) file_name
    """
    di = to_secepoc(datetime.datetime(year, 1, 1, 0, 0, 0))
    df = to_secepoc(datetime.datetime(year+10, 1, 1, 0, 0, 0))
    if year == ev['year_ini']:
        di = ev['secepoc_ini']

    ts = time_series(ev, di, df)
    file_name = ev['out_dir'] + os.sep + 'osacc.hdf'
    with h5py.File(file_name, 'w') as f:
        f.create_dataset('date', data=ts, compression="gzip")
        f.attrs['LastRun'] = di
        f.attrs['LastRunUTC'] = str(to_isodate(di))

    return file_name

def create_proj_datasets(ev, year, proj_id, p_dict):
    """Initial creation of metrics hdf5 dataset containing 1 group per project and datasets
    for each metric.
    Attributes are set for each hdf5 group (project) with project ID and Description
    The projects dictionary is
    proj_dict = { "proj_id": ["project name", "project description"], }
    proj_dict[proj_id][0] - gives the project name
    proj_dict[proj_id][1] - gives the project description
    :param ev: configuration options
    :param year: Year
    :param proj_id: Project ID
    :param p_dict: projects dictionary from keystone
    :return (string) file_name
    """
    di = to_secepoc(datetime.datetime(year, 1, 1, 0, 0, 0))
    df = to_secepoc(datetime.datetime(year+10, 1, 1, 0, 0, 0))
    if year == ev['year_ini']:
        di = ev['secepoc_ini']

    ts = time_series(ev, di, df)
    file_name = get_hdf_filename(ev)
    grp_name = p_dict[proj_id][0]
    with h5py.File(file_name, 'r+') as f:
        grp = f.create_group(grp_name)
        grp.attrs['ProjID'] = proj_id
        grp.attrs['ProjDescription'] = p_dict[proj_id][1]
        ds = numpy.zeros([ts.size, ], dtype=int)
        for m in METRICS:
            grp.create_dataset(m, data=ds, compression="gzip")

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


def time2index(ev, tstamp, time_array):
    """
    Linear step equation: index(t) = floor(1/Dt * (t-t0))
    y = floor(a + bx) -> a = -t0/Dt  b = 1/Dt
    :param ev: configuration options
    :param tstamp: timestamp in seconds to epoc
    :param time_array: array of timestamps
    :return: index
    """
    dt = ev['delta_time']
    t0 = time_array[0]
    index = int(math.floor(1.0 * (tstamp-t0)/dt))
    return index


def now_acc():
    """
    :return: datetime now in seconds from epoch
    """
    nacc = datetime.datetime.utcnow()
    return to_secepoc(nacc)


def time_series(ev, di, df):
    """Create a time array (of ints) in seconds to epoch format with interval
    of delta_time for all years
    :param ev: configuration options
    :param di: Initial Date Time in seconds to epoc
    :param df: Final Date Time in seconds to epoc
    :returns (numpy array) time_array
    """
    return numpy.arange(di, df, ev['delta_time'])


def db_conn(database):
    """Create Mysql database connection
    :param database: database name
    :return: mysql connector
    """
    ev = get_conf()
    return mysql.connector.connect(host=ev['mysql_host'],
                                   user=ev['mysql_user'],
                                   passwd=ev['mysql_pass'],
                                   db=database)


def get_list_db(ti, database, dbtable, state):
    """Get the list of rows of table from database
    Query keystone, nova or cinder to get projects or instances or volumes
    For projects (do not take into account admin and service projects)
    DB = keystone -> Table = project
    DB = cinder   -> Table = volumes
    DB = nova     -> Table = instances and instance_info_caches
    :param ti: Initial Date Time in seconds to epoc
    :param database: Database name
    :param dbtable: Database table name
    :param state: one of the two values - `init` accounting (default), `upd` update
    :return (json dict): List of rows of a table in the database
    """
    local_timezone = tzlocal.get_localzone()
    dtlocal_i = datetime.datetime.fromtimestamp(ti, local_timezone)

    # The cnd_state is "All" if in initialization
    # The cnd_state is deleted_at >= date_time_local if in update
    cnd_state = " "
    cnd_state_nova = " "
    if state == "upd":
        cnd_state = " AND deleted_at >= '%s'" % dtlocal_i
        cnd_state_nova = " AND instances.deleted_at > '%s'" % dtlocal_i

    # Case for projects in keystone
    table_str = "id,name,description,enabled"
    table_coll = table_str.split(",")
    cond = "domain_id='default' AND name!='admin' AND name!='service' AND enabled='1'"
    condition = cond + " ORDER BY name"

    # Case for volumes and snapshots in cinder
    if database == "cinder":
        table_coll = ['created_at', 'deleted_at', 'id', 'project_id',
                      'size', 'status']
        if dbtable == "volumes":
            table_str = "created_at,deleted_at,id,project_id,size,status"
            condition = "(status = 'available' OR status = 'in-use') OR (status = 'deleted'" + cnd_state + ")"
        if dbtable == "snapshots":
            table_str = "created_at,deleted_at,id,project_id,volume_size,status"
            condition = "status = 'available' OR (status = 'deleted'" + cnd_state + ")"

    query = ' '.join((
        "SELECT " + table_str,
        "FROM " + dbtable,
        "WHERE " + condition
    ))

    # Case for instances and fip in nova
    if database == "nova":
        table_coll = ['uuid', 'created_at', 'deleted_at', 'id', 'project_id',
                      'vm_state', 'memory_mb', 'vcpus', 'network_info']
        table_str = "instances.uuid,instances.created_at,instances.deleted_at," \
                    "instances.id,instances.project_id,instances.vm_state," \
                    "instances.memory_mb," \
                    "instances.vcpus,instance_info_caches.network_info"
        ijoin = "instance_info_caches ON uuid=instance_info_caches.instance_uuid"
        condition = "(instances.vm_state = 'active' OR instances.vm_state = 'stopped') OR " \
                    "(instances.vm_state = 'deleted'" + cnd_state_nova + ")"
        query = ' '.join((
            "SELECT " + table_str,
            "FROM " + dbtable,
            "INNER JOIN " + ijoin,
            "WHERE " + condition
        ))

    return get_table_rows(database, query, table_coll)


def get_quotas(database):
    """Get quotas of metrics from database per project
    returns a list of dict() of the form
    [{'quota_name': 'ram', 'project_id': '123', 'quota_value': '1024'},]
    :param database: database name
    :return:
    """
    dbtable = "quotas"
    table_str = "id,project_id,resource,hard_limit"
    if database == "cinder":
        condition = "(resource = 'gigabytes' OR resource = 'volumes' OR resource = 'snapshots')"

    if database == "nova_api":
        condition = "resource = 'cores' OR resource = 'ram' OR resource = 'instances'"

    if database == "neutron":
        table_str = "*"
        condition = "resource = 'floatingip'"

    table_coll = ['id', 'project_id', 'quota_name', 'quota_value']
    query = ' '.join((
        "SELECT " + table_str,
        "FROM " + dbtable,
        "WHERE " + condition
    ))

    return get_table_rows(database, query, table_coll)


def get_table_rows(database, query, table_coll):
    """Creates a list with rows of database table
    :param database: database name
    :param query: database query
    :param table_coll: list with names of table columns
    :return:
    """
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


def get_projects(di, state):
    """
    Get all projects in keystone database
    Returns a dictionary with all projects
    p_dict = {'project_id': ['project_name', 'project_description'], }
    :param di: Initial DateTime in seconds to epoch
    :param state: Either ``init`` or ``upd`` of accounting
    :return: dictionary with keystone projects
    """
    projects = get_list_db(di, "keystone", "project", state)
    p_dict = dict()
    for proj in projects:
        p_dict[proj['id']] = [proj['name'], proj['description']]

    return p_dict


def prep_metrics(time_array, p_dict, proj_id, projects_in, a):
    """Prepare numpy arrays of metrics for a given project
    :param time_array: numpy array with timestamps
    :param p_dict: dictionary with keystone projects
    :param proj_id: project ID
    :param projects_in: list of projects
    :param a: dictionary of metrics per project
    :return: project name
    """
    pname = p_dict[proj_id][0]
    if proj_id not in projects_in:
        projects_in.append(proj_id)
        a[pname] = dict()
        for m in METRICS:
            a[pname][m] = numpy.zeros([time_array.size, ], dtype=int)
    return pname


def get_indexes(ev, created, deleted, di, df, time_array, state):
    """Get indexes corresponding to a timestamp interval for a given
    nova instance
    :param ev: configuration options
    :param created: Nova instance creation time
    :param deleted: Nova instance delete time
    :param di: Initial DateTime in seconds to epoch
    :param df: Final DateTime in seconds to epoch
    :param time_array: numpy array with timestamps
    :param state: one of the two values - `init` accounting (default), `upd` update
    :return: start index of time_array, end index of time_array
    """
    t_create = di
    t_final = df
    crt_sec = to_secepoc(created)
    if deleted:
        t_final = to_secepoc(deleted)

    if state == "init":
        t_create = crt_sec

    if (state == "upd") and (crt_sec > di):
        t_create = crt_sec

    idx_start = time2index(ev, t_create, time_array)
    idx_end = time2index(ev, t_final, time_array) + 1
    return idx_start, idx_end


def process_inst(ev, di, df, time_array, a, p_dict, projects_in, state):
    """Process instances, create/update projects metrics arrays
    array of metrics is filled
    :param ev: configuration options
    :param di: Initial DateTime in seconds to epoch
    :param df: Final DateTime in seconds to epoch
    :param time_array: numpy array with timestamps
    :param a: dictionary with array of metrics for each project
    :param p_dict: projects dictionary from keystone
    :param projects_in: list of projects to process
    :param state: one of the two values - `init` accounting (default), `upd` update
    """
    instances = get_list_db(di, "nova", "instances", state)
    print(80*"=")
    print("Instances selected from DB n = ", len(instances))
    for inst in instances:
        proj_id = inst['project_id']
        if proj_id not in p_dict:
            continue

        crt = inst["created_at"]
        dlt = inst["deleted_at"]
        pname = prep_metrics(time_array, p_dict, proj_id, projects_in, a)
        idx_start, idx_end = get_indexes(ev, crt, dlt, di, df, time_array, state)
        a[pname]['vcpus'][idx_start:idx_end] = a[pname]['vcpus'][idx_start:idx_end] + inst['vcpus']
        a[pname]['mem_mb'][idx_start:idx_end] = a[pname]['mem_mb'][idx_start:idx_end] + inst['memory_mb']
        a[pname]['ninstances'][idx_start:idx_end] = a[pname]['ninstances'][idx_start:idx_end] + 1
        net_info = json.loads(inst['network_info'])
        if net_info:
            for l in range(len(net_info)):
                for n in range(len(net_info[l]['network']['subnets'])):
                    for k in range(len(net_info[l]['network']['subnets'][n]['ips'])):
                        nip = len(net_info[l]['network']['subnets'][n]['ips'][k]['floating_ips'])
                        a[pname]['npublic_ips'][idx_start:idx_end] = a[pname]['npublic_ips'][idx_start:idx_end] + nip


def process_vol(ev, di, df, time_array, a, p_dict, projects_in, state):
    """Process volumes, create/update projects metrics arrays
    array of metrics is filled
    :param ev: configuration options
    :param di: Initial DateTime in seconds to epoch
    :param df: Final DateTime in seconds to epoch
    :param time_array: numpy array with timestamps
    :param a: dictionary with array of metrics for each project
    :param p_dict: projects dictionary from keystone
    :param projects_in: list of projects to process
    :param state: one of the two values - `init` accounting (default), `upd` update
    """
    storages = ["volumes", "snapshots"]
    for stor in storages:
        volumes = get_list_db(di, "cinder", stor, state)
        print(80*"=")
        print(stor, "selected from DB n = ", len(volumes))
        for vol in volumes:
            proj_id = vol['project_id']
            if proj_id not in p_dict:
                continue
            crt = vol["created_at"]
            dlt = vol["deleted_at"]
            pname = prep_metrics(time_array, p_dict, proj_id, projects_in, a)
            idx_start, idx_end = get_indexes(ev, crt, dlt, di, df, time_array, state)
            a[pname]['volume_gb'][idx_start:idx_end] = a[pname]['volume_gb'][idx_start:idx_end] + vol['size']
            a[pname]['nvolumes'][idx_start:idx_end] = a[pname]['nvolumes'][idx_start:idx_end] + 1


def process_quotas(proj_dict):
    """Process quotas per project, returns a list of all quotas
    from get_projects.
    METRIC[i]   <-> quota_name[i]
    vcpus       <-> cores
    mem_mb      <-> ram
    volume_gb   <-> gigabytes
    ninstances  <-> instances
    nvolumes    <-> volumes + snapshots
    npublic_ips <-> floatingip
    :param proj_dict: dict with all projects from 
    """
    dbs = ["nova_api", "cinder", "neutron"]
    all_quotas = list()
    for db in dbs:
        quotas = get_quotas(db)
        for quota in quotas:
            if quota['project_id'] in proj_dict.keys():
                proj_name = proj_dict[quota['project_id']][0]
                quota['grp_name'] = proj_name
                if quota['quota_name'] == "cores":
                    quota['quota_name'] = "q_vcpus"
                if quota['quota_name'] == "ram":
                    quota['quota_name'] = "q_mem_mb"
                if quota['quota_name'] == "gigabytes":
                    quota['quota_name'] = "q_volume_gb"
                if quota['quota_name'] == "instances":
                    quota['quota_name'] = "q_ninstances"
                if quota['quota_name'] == "volumes":
                    quota['quota_name'] = "q_nvolumes"
                if quota['quota_name'] == "snapshots":
                    quota['quota_name'] = "q_snapshots"
                if quota['quota_name'] == "floatingip":
                    quota['quota_name'] = "q_npublic_ips"

                all_quotas.append(quota)

    return all_quotas
