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
        print 20 * '-', pname
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
        #a[p]['vcpus'][idx_start:idx_end] = a[p]['vcpus'][idx_start:idx_end] + inst['vcpus']
        #a[p]['mem_mb'][idx_start:idx_end] = a[p]['mem_mb'][idx_start:idx_end] + inst['memory_mb']
        #a[p]['disk_gb'][idx_start:idx_end] = a[p]['disk_gb'][idx_start:idx_end] + inst['root_gb']
        #a[p]['ninstances'][idx_start:idx_end] = a[p]['ninstances'][idx_start:idx_end] + 1

        print type(a[p]['vcpus'])
        if inst['network_info']:
            print 80 * '='
            pprint.pprint(inst['network_info'])

# METRICS = ['vcpus', 'mem_mb', 'disk_gb', 'volume_gb', 'ninstances', 'nvolumes', 'npublic_ips']
