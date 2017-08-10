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
            print to_isodate(ts[0])
            # This block is to insert the values from Nova Instances
            instances = get_instances(year)
            for inst in instances:
                t_create = to_secepoc(inst["created_at"])
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
                print "ProjID= ", vol['project_id'], " SizeGB= ", vol['size'], "  --ProjID from filter= ", proj
                print 'IDX_Start= ', idx_start, ' IDX_End= ', idx_end
                print 'Vol_Start= ', to_isodate(t_create), ' Vol_End= ', to_isodate(t_final)
                print 'VolArray_Start= ', vol_array[idx_start], ' VolArray_End= ', vol_array[idx_end-1]
                print 'TS_Start= ', ts[idx_start], ' TS_End= ', ts[idx_end]



""" This block uses the nova API bindings
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
"""


