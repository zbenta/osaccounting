# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#

import pprint
from osacc_functions import *

if __name__ == '__main__':
    ev = get_conf()
    client = get_influxclient()
    pprint.pprint(client.get_list_database())
    pprint.pprint(client.get_list_retention_policies('osacc'))
