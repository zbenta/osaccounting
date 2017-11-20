# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Create initial hdf5 file to store accounting data for the first year

    Dictionaries (data structures) returned from the query to database tables
    - projects  (DB=keystone, TABLE=project)
    - instances (DB=nova,     TABLE=instances)
    - volumes   (DB=cinder,   TABLE=volumes)
    project = { "description": None,
                "enabled": None,
                "id": None,
                "name": None
               }
    instances = {
                }
    volumes = {
              }
"""

from osacc_functions import *

if __name__ == '__main__':
    year = YEAR_INI
    filename = create_hdf(year)
    evr = get_env()
    projects = get_list_db("keystone", "project")
    instances = get_list_db("nova", "instances")
    instances_info = get_list_db("nova", "instance_info_caches")
    volumes = get_list_db("cinder", "volumes")
    size_a = size_array(year)
    print 80 * "="
    print "Year = %i : Size Array = %i : FileName = %s" % (year, size_a, filename)
    """
    print "--> Projects"
    pprint.pprint(projects)
    print
    print "--> Instances"
    pprint.pprint(instances)
    print
    print "--> Volumes"
    pprint.pprint(volumes)
    """
    print 20 * "-"

    print "Start date <ts>- ", to_isodate(DATEINI)
    # This block is to insert the values from Nova Instances
    for inst in instances:
        t_create = to_secepoc(inst["created_at"])
        t_final = ts[size_a-1]
        if inst["deleted_at"]:
            t_final = to_secepoc(inst["deleted_at"])
        if t_final > to_secepoc(datetime.datetime.utcnow()):
            t_final = to_secepoc(datetime.datetime.utcnow())

        idx_start, idx_end = dt_to_indexes(t_create, t_final, year)
        p = filter(lambda pr: pr['id'] == inst['project_id'], projects)
        if not p:
            continue

        proj = p[0]

        print 80*'-'
        print 10*"x", " Instance ID = ", inst['uuid']
        print "ProjID Inst = ", inst['project_id'], " VCPUs= ", inst['vcpus'], " Mem_MB= ", inst['memory_mb']
        print "ProjID filt = ", proj["id"], proj["name"]
        print 'Inst Start = ', inst["created_at"], ' Inst End = ', inst["deleted_at"]
