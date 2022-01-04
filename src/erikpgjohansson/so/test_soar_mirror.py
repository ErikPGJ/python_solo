# -*- coding: UTF-8 -*-
'''
Code for implementing a test SOAR data mirror.

This is not generic code, but can be seen as de facto example code.

Initially created 2021-04-23 by Erik P G Johansson, IRF Uppsala, Sweden.
'''
'''
BOGIQ
=====
'''


import erikpgjohansson.so.soar_mirror
import os
import numpy



def _datasets_include_func(instrument, level, beginTime):
    '''
Function that determines whether a specific dataset is included/excluded in
the sync.


Parameters
----------
instrument : String
level      : String


Returns
-------
include: bool
    Whether datasets should be included or not
'''
    # NOTE: Should be configured to match only ONE dataset (file)
    if 1:
        START_TIME = numpy.datetime64('2020-08-13T00:00:00.000')
        STOP_TIME  = numpy.datetime64('2020-08-14T00:00:00.000')

    if (instrument=='EPD') \
        and (level in [      'L1']) \
        and (START_TIME < beginTime) \
        and (beginTime < STOP_TIME):
        return True

    return False



def sync():
    assert os.uname().nodename in ['brain', 'irony'],\
        'This code is not meant to run on this machine.'

    # TEMP
    #print('-'*80)
    #print('PYTHONPATH = '+os.environ['PYTHONPATH'])
    #print('-'*80)

    rootDir = '/home/erjo/temp/soar_mirror'

    erikpgjohansson.so.soar_mirror.sync(
        syncDir                 = os.path.join(rootDir, 'mirror'),
        tempDownloadDir         = os.path.join(rootDir, 'download'),
        datasetsSubsetFunc      = _datasets_include_func,
        downloadLogFormat       = 'long',
        deleteOutsideSubset     = True,
        nMaxNetDatasetsToRemove = 20)


if __name__ == '__main__':
    sync()
