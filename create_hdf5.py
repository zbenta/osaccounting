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


# Set the initial date to start the accounting -> 1st April 2016
DATEINI = datetime.datetime(2016, 4, 1, 0, 0, 0)
SECEPOC = time.mktime(DATEINI.timetuple())
# Interval of data points in seconds
DELTA = 3600


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
    :returns (float) seconds from epoch
    """
    return time.mktime(date.timetuple())


def time_series(year=2016):
    """Create a time array (of ints) in epoch format with interval
    of one hour for a given year
    :param year: Year
    :returns (numpy) time_array
    """
    di = to_secepoc(datetime.datetime(year, 1, 1, 0, 0, 0))
    df = to_secepoc(datetime.datetime(year+1, 1, 1, 0, 0, 0))
    n = (df - di)/DELTA
    print 'SIZE ARRAY in function. ', n
    time_array = numpy.arange(int(di), int(df), DELTA)
    print 'time array size. ', time_array.size
    return time_array


def size_array(year=2016):
    """Number of data points is the size of the arrays for 1 year
    :param year: Year
    :return (int) size of arrays"""
    sizea = time_series(year)
    return sizea.size


def set_hdf_fnames():
    """List of HDF5 filenames, are the <YEAR>.hdf
    :returns (list) of filename
    """
    tf = datetime.datetime.now()
    l = range(DATEINI.year, tf.year + 1)
    fname = []
    for i in l:
        fname.append(str(i) + '.hdf')
    return fname


def set_hdf_grp(proj, user, metric):
    """Creates the HDF5 group structure
    :param proj: project name
    :param user: user name
    :param metric: metric name
    :return hdf5 group
    """
    grp = proj + '/' + user + '/' + metric
    return grp

if __name__ == '__main__':
    evr = get_env()
    json_proj = evr['out_dir'] + os.sep + 'projects.json'
    with open(json_proj, 'r') as f:
        projects = json.load(f)

    fn = set_hdf_fnames()
    print 'FILENAME: ', fn
    ts = time_series()
    print 'TIME SERIES: ', ts
    sa = size_array()
    print 'Array size: ', sa

    #pprint.pprint(projects)
