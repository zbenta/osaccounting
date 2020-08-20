#!/usr/bin/env bash
OSCRED=admin.sh
PROJ=/var/log/osacc/proj.txt

date
mkdir -p /var/log/osacc
source ${OSCRED}

echo "Get all projects"
#openstack project list --format value -c Name > ${PROJ}



for proj in `cat ${PROJ}`
do
    eval "$(openstack usage show --project $proj --format shell)"
    echo "---------------"
    echo "$proj,$cpu_hours,$disk_gb_hours,$ram_mb_hours,$servers"
done

DATE=`date +%F_%H-%M`
