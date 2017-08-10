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
    evr = get_env()
    projects = get_projects()
    # Get the list of years
    years = get_years()

    for year in years:
        ts = time_series(year)
        sa = size_array(year)

        # For testing purposes: indexes to check date intervals
        idx_i = sa-15
        idx_f = sa

        with h5py.File(evr['out_dir'] + os.sep + str(year) + '.hdf', 'w') as f:
            res = f.create_dataset('date', data=ts, compression="gzip")
            for proj in projects:
                grp_name = proj['Name']
                grp = f.create_group(grp_name)
                grp.attrs['ProjID'] = proj['ID']
                grp.attrs['ProjDesc'] = proj['Description']

                # Create the arrays for metrics
                a_vcpus = create_metric_array(year)
                a_mem_mb = create_metric_array(year)
                a_disk_gb = create_metric_array(year)
                a_volume_gb = create_metric_array(year)
                print 80*'-'
                print proj['Name']
                print 'Date= ', to_isodate(ts[0]), ' SizeArray= ', sa

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


                res = grp.create_dataset('vcpus', data=a_vcpus, compression="gzip")
                res = grp.create_dataset('mem_mb', data=a_mem_mb, compression="gzip")
                res = grp.create_dataset('disk_gb', data=a_disk_gb, compression="gzip")
                res = grp.create_dataset('volume_gb', data=a_volume_gb, compression="gzip")
                print 'Date: ', to_isodate(ts[sa-1])
                print 'VPUS: ', a_vcpus[idx_i:idx_f]
                print 'MEM: ', a_mem_mb[idx_i:idx_f]
                print 'Disk: ', a_disk_gb[idx_i:idx_f]
