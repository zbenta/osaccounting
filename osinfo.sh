#!/usr/bin/env bash

date
mkdir -p /var/log/osinfo
source /usr/local/py3/bin/activate
echo "$? : py3 venv activated"
/usr/local/bin/osinfo.py /var/log/osinfo/data.json
echo "$? : osinfo.py"
DATE=`date +%F_%H-%M`
cp /var/log/osinfo/data.json /var/log/osinfo/data_${DATE}.json
gzip /var/log/osinfo/data_${DATE}.json

