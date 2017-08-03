# -*- encoding: utf-8 -*-
#
# Copyright 2017 LIP
#
# Author: Mario David <mariojmdavid@gmail.com>
#


"""Create initial hdf5 files to store accounting data
"""


import pprint
import datetime
import os
import h5py
import time


def get_env():
    envvars = dict()
    envvars['out_dir'] = os.environ['OUT_DIR']

if __name__ == '__main__':
    all_records = {}


