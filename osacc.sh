#!/usr/bin/env bash

date
mkdir -p /var/log/osacc
source /usr/local/py3/bin/activate
echo "$? : py3 venv activated"
/usr/local/bin/get_acc.py
echo "$? : get_acc.py"
DATE=`date +%F_%H-%M`
