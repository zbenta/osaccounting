# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#

from osacc_functions import *

if __name__ == '__main__':
    ev = get_conf()
    client = get_influxclient()
    result = client.query('SHOW DATABASES;')
    print("Result: {0}".format(result))
