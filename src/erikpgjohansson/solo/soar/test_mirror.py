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


def _datasets_include_func(instrument, level, beginTime, datasetId):
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

    LS_LEVELS = ['LL02', 'L1']
    # LS_LEVELS = ['L1']
    if (instrument == 'EPD') \
            and (level in LS_LEVELS) \
            and (START_TIME <= beginTime) \
            and (datasetId == 'solo_LL02_epd-ept-asun-rates'.upper()) \
            and (beginTime < STOP_TIME):
        return True

    return False


def sync():
    assert os.uname().nodename in ['brain', 'spis', 'irony'],\
        'This code is not meant to run on this machine.'

    ROOT_DIR = '/home/erjo/temp/soar_mirror'
    SOAR_TABLE_CACHE_JSON_FILE = "/home/erjo/temp/temp/soar.json"

    erikpgjohansson.solo.soar.mirror.sync(
        syncDir                   = os.path.join(ROOT_DIR, 'mirror'),
        tempDownloadDir           = os.path.join(ROOT_DIR, 'download'),
        datasetsSubsetFunc        = _datasets_include_func,
        downloadLogFormat         = 'long',
        deleteOutsideSubset       = True,
        nMaxNetDatasetsToRemove   = 20,
        SoarTableCacheJsonFilePath=SOAR_TABLE_CACHE_JSON_FILE,
    )


# IMPLEMENTATION NOTE: This code makes it possible to execute the sync from
# the bash command line:
# >> python -m 'erikpgjohansson.solo.soar.test_mirror'
if __name__ == '__main__':
    sync()
