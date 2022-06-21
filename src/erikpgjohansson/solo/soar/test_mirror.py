'''
Code for implementing a test SOAR data mirror.

This module is intended to only contain code for that particular mirror, i.e.
only code that "configures" that particular mirror.

Initially created 2021-04-23 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import erikpgjohansson.solo.soar.mirror
import numpy
import os


'''
BOGIQ
=====
'''


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
    START_TIME = numpy.datetime64('2020-08-13T00:00:00.000')
    STOP_TIME  = numpy.datetime64('2020-08-14T00:00:00.000')

    if (instrument == 'EPD') \
            and (level in ['L1']) \
            and (START_TIME <= beginTime) \
            and (beginTime < STOP_TIME):
        return True

    return False


def sync():
    assert os.uname().nodename in ['brain', 'irony'],\
        'This code is not meant to run on this machine.'

    # TEMP
    # print('-'*80)
    # print('PYTHONPATH = '+os.environ['PYTHONPATH'])
    # print('-'*80)

    ROOT_DIR = '/home/erjo/temp/soar_mirror'

    erikpgjohansson.solo.soar.mirror.sync(
        syncDir                 = os.path.join(ROOT_DIR, 'mirror'),
        tempDownloadDir         = os.path.join(ROOT_DIR, 'download'),
        datasetsSubsetFunc      = _datasets_include_func,
        downloadLogFormat       = 'long',
        deleteOutsideSubset     = True,
        nMaxNetDatasetsToRemove = 20,
    )


if __name__ == '__main__':
    sync()
