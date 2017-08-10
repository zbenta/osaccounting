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

from osacc_functions import *

if __name__ == '__main__':
    create_hdf()
    evr = get_env()
    years = get_years()
    projects = get_projects()

    for year in years:
        size_a = size_array(year)
        with h5py.File(evr['out_dir'] + os.sep + str(year) + '.hdf', 'r+') as f:
            ts = f['date'][:]
            print 80*"O"
            print to_isodate(ts[0])
            # This block is to insert the values from Nova Instances
            instances = get_instances(year)
            pprint.pprint(instances)
            for inst in instances:
                t_create = to_secepoc(inst["created_at"])
                t_final = ts[size_a-1]
                print "Deleted_at = <", inst["deleted_at"], ">"
                if inst["deleted_at"]:
                    t_final = to_secepoc(inst["deleted_at"])
                if t_final > to_secepoc(datetime.datetime.utcnow()):
                    t_final = to_secepoc(datetime.datetime.utcnow())
                idx_start, idx_end = dt_to_indexes(t_create, t_final, year)
                p = filter(lambda pr: pr['ID'] == inst['project_id'], projects)
                proj = p[0]
                grp_name = proj['Name']
                vcpu_array = f[grp_name]['vcpus']
                vcpu_array[idx_start:idx_end] = vcpu_array[idx_start:idx_end] + inst['vcpus']
                mem_array = f[grp_name]['mem_mb']
                mem_array[idx_start:idx_end] = mem_array[idx_start:idx_end] + inst['memory_mb']
                disk_array = f[grp_name]['disk_gb']
                disk_array[idx_start:idx_end] = disk_array[idx_start:idx_end] + inst['root_gb']
                print 80 * '-'
                print 'xxxxxxxxxxx   INSTANCES   xxxxxxxxxxxxxxxxxxxxxxxx'
                print "Instance ID = ", inst['uuid']
                print "ProjID= ", inst['project_id'], " VCPUs= ", inst['vcpus'], " Mem_MB= ", inst['memory_mb']
                print "  --ProjID from filter= ", proj
                print 'IDX_Start= ', idx_start, ' IDX_End= ', idx_end
                print 'Inst Start= ', to_isodate(t_create), ' Inst End= ', to_isodate(t_final)
                print 'VCPU Array Start= ', vcpu_array[idx_start], ' VCPU Array End= ', vcpu_array[idx_end - 1]
                print 'MEM Array Start= ', mem_array[idx_start], ' MEM Array End= ', mem_array[idx_end - 1]
                print 'TS_Start= ', ts[idx_start], ' TS_End= ', ts[idx_end]

            # This block is to insert the values from Cinder volumes
            volumes = get_volumes(year)
            for vol in volumes:
                t_create = to_secepoc(vol["created_at"])
                t_final = ts[size_a-1]
                if vol["deleted"]:
                    t_final = to_secepoc(vol["deleted_at"])
                if t_final > to_secepoc(datetime.datetime.utcnow()):
                    t_final = to_secepoc(datetime.datetime.utcnow())
                idx_start, idx_end = dt_to_indexes(t_create, t_final, year)
                p = filter(lambda pr: pr['ID'] == vol['project_id'], projects)
                proj = p[0]
                grp_name = proj['Name']
                vol_array = f[grp_name]['volume_gb']
                vol_array[idx_start:idx_end] = vol_array[idx_start:idx_end] + vol['size']
                print 80*'-'
                print 'xxxxxxxxxxx   VOLUMES   xxxxxxxxxxxxxxxxxxxxxxxx'
                print "ProjID= ", vol['project_id'], " SizeGB= ", vol['size'], "  --ProjID from filter= ", proj
                print 'IDX_Start= ', idx_start, ' IDX_End= ', idx_end
                print 'Vol_Start= ', to_isodate(t_create), ' Vol_End= ', to_isodate(t_final)
                print 'VolArray_Start= ', vol_array[idx_start], ' VolArray_End= ', vol_array[idx_end-1]
                print 'TS_Start= ', ts[idx_start], ' TS_End= ', ts[idx_end]
