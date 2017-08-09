# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Reads and updates volumes datasets
   Old/deleted volumes are not accessible through the cinder API
   Connects to the cinder DB to read volume information and update
   the datasets of hdf5
"""

from osacc_functions import *

if __name__ == '__main__':
    evr = get_env()
    volumes = get_volumes()
    pprint.pprint(volumes)
