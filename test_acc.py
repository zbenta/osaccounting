# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#

from influxdb import InfluxDBClient
from osacc_functions import *

if __name__ == '__main__':
    ev = get_conf()
    dbhost = ev['dbhost']
    dbport = ev['dbport']
    dbuser = ev['dbuser']
    dbpass = ev['dbpass']
    dbname = ev['dbname']
    client = InfluxDBClient(dbhost, dbport, dbuser, dbpass, dbname)
    result = client.query('SHOW DATABASES;')
    print("Result: {0}".format(result))
