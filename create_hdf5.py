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
import json
import time


def get_env():
    """Get environment variables
    :returns dictionary with environment variables
    """

    ev = dict()
    ev['out_dir'] = os.environ['OUT_DIR']
    return ev

if __name__ == '__main__':
    evr = get_env()
    json_proj = evr['out_dir'] + os.sep + 'projects.json'
    with open(json_proj, 'r') as f:
        projects = json.load(f)
    pprint.pprint(projects)


