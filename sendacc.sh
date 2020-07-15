#!/usr/bin/env bash

date
mkdir -p /var/log/osacc
source /usr/local/py3/bin/activate
echo "$? : py3 venv activated"
/usr/local/bin/send_influx.py
echo "$? : send_influx.py"
DATE=`date +%F_%H-%M`
