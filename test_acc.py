# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#

from osacc_functions import *

if __name__ == '__main__':
    ev = get_conf()
    dt_ini = ev['secepoc_ini']
    projects = get_list_db(dt_ini, "keystone")
    instances = get_list_db(dt_ini, "nova")
    volumes = get_list_db(dt_ini, "cinder")
    time_array = time_series_ini()
    a = dict()
    for proj in projects:
        pname = proj['name']
        a[pname] = dict()
        for m in METRICS:
            a[pname][m] = numpy.zeros([time_array.size, ], dtype=int)

    for inst in instances:
        t_create = to_secepoc(inst["created_at"])
        t_final = now_acc()
        if inst["deleted_at"]:
            t_final = to_secepoc(inst["deleted_at"])

        idx_start, idx_end = dt_to_index(t_create, t_final, time_array)
        p = filter(lambda pr: pr['id'] == inst['project_id'], projects)
        if not p:
            continue

        proj = p[0]
        pname = proj['name']
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

    for vol in volumes:
        t_create = to_secepoc(inst["created_at"])
        t_final = now_acc()
        if vol["deleted_at"]:
            t_final = to_secepoc(vol["deleted_at"])

        idx_start, idx_end = dt_to_index(t_create, t_final, time_array)
        p = filter(lambda pr: pr['id'] == vol['project_id'], projects)
        if not p:
            continue

        proj = p[0]
        pname = proj['name']
        a[pname]['volume_gb'][idx_start:idx_end] = a[pname]['volume_gb'][idx_start:idx_end] + vol['size']
        a[pname]['nvolumes'][idx_start:idx_end] = a[pname]['nvolumes'][idx_start:idx_end] + 1

    for year in get_years():
        filename = get_hdf_filename(year)
        with h5py.File(filename, 'r+') as f:
            ts = f['date'][:]
            idx_start, idx_end = dt_to_index(ts[0], ts[-1], time_array)
            for proj in projects:
                grp_name = proj['name']
                for metric in METRICS:
                    c = data_array.size
                    d = a[grp_name][metric][idx_start:idx_end]
                    print "Sizes HDF data_array = ", c, " a = ", d.size
                    data_array = f[grp_name][metric]
                    data_array[:] = a[grp_name][metric][idx_start:idx_end]



