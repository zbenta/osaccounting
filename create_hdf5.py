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
import numpy


# Set the initial date to start the accounting
DATEINI = datetime.datetime(2014, 10, 1, 0, 0, 0)
SECEPOC = int(time.mktime(DATEINI.timetuple()))


def get_env():
    """Get environment variables
    :returns dictionary with environment variables
    """

    ev = dict()
    ev['out_dir'] = os.environ['OUT_DIR']
    return ev


def to_secepoc(date=DATEINI):
    """Converts datetime to seconds from epoc
    :param date: Date in datetime format
    :returns (int) seconds from epoc
    """
    return int(time.mktime(date.timetuple()))


def time_series():
    tf = datetime.datetime.now()
    ti = DATEINI
    delta_t = tf - ti
    time_array = numpy.array()
    while (td <= tf):
        time_array


if __name__ == '__main__':
    evr = get_env()
    json_proj = evr['out_dir'] + os.sep + 'projects.json'
    with open(json_proj, 'r') as f:
        projects = json.load(f)
    pprint.pprint(projects)
