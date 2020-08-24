#!/usr/bin/env bash
OSCRED=admin.sh
PROJ=/var/log/osacc/proj.txt

DATE_END=`date +%Y%m%d -d "last Sun"`
DATE_START=`date +%m%d -d "last Sun -7 days"`
BASE_FILE="/var/log/osacc/CLOUD_STRATUS_"
FILE_USAGE="${BASE_FILE}${DATE_END}${DATE_START}_usage.csv"

ACC_END=`date +%Y-%m-%d -d "last Sun"`
ACC_START=`date +%Y-%m-%d -d "last Sun -7 days"`

echo "Start: ${DATE_START}"
echo "End: ${DATE_END}"
echo "Accounting Start: ${ACC_START}"
echo "Accounting End: ${ACC_END}"
echo "Usage file: ${FILE_USAGE}"

mkdir -p /var/log/osacc
source ${OSCRED}

echo "Get all projects"
openstack project list --format value -c Name > ${PROJ}

echo "Account,CPU Hours,Disk GB-Hours,RAM MB-Hours,Servers" > ${FILE_USAGE}

for proj in `cat ${PROJ}`
do
  if [ ${proj} == 'admin' ] || [ ${proj} == 'service' ]
  then
    continue
  else
    echo "---------------"
    eval "$(openstack usage show --project $proj --format shell --start ${ACC_START} --end ${ACC_END})"
    echo "$proj,$cpu_hours,$disk_gb_hours,$ram_mb_hours,$servers" >> ${FILE_USAGE}
  fi
done
