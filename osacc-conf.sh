#!/usr/bin/env bash

# Output directory to store hdf files with accounting
# the filenames are <YEAR>.hdf
OUT_DIR="/var/log/osacc"

# Month and Year to start the accounting
MONTH_INI=1
YEAR_INI=2016

# Interval between timestamps in seconds
DELTA_TIME=60.0

# mysql connect parameters
# uncomment and set password and mysql host
MYSQL_USER=root
#MYSQL_PASS=
#MYSQL_HOST=

# (OPTIONAL) send accounting records to graphite/carbon
#CARBON_SERVER=
#CARBON_PORT=

# Graphite Namespace
#GRAPH_NS="os_accounting"

mkdir -p ${OUT_DIR}
