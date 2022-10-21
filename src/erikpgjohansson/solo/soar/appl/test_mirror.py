'''
Code for implementing a test SOAR data mirror.

This module is intended to only contain code for that particular mirror, i.e.
only code that "configures" that particular mirror.

Initially created 2021-04-23 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import datetime
import erikpgjohansson.solo.soar.mirror
import logging
import numpy
import os
# import sys


'''
BOGIQ
=====
'''


def datasets_include_func(instrument, level, beginTime, datasetId):
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
    Whether datasets should be included or not.
'''
    # NOTE: const.LS_SOAR_INSTRUMENTS determines which instruments can be
    # downloaded.
    # solo_L2_mag-rtn-burst_20220311_V01.cdf

    # NOTE: Should be configured to match only ONE dataset (file)
    START_TIME = numpy.datetime64('2020-08-13T00:00:00.000')
    STOP_TIME  = numpy.datetime64('2020-08-14T00:00:00.000')

    # LS_LEVELS = ['LL02', 'L1', 'L2']
    LS_LEVELS = ['L1']
    if START_TIME <= beginTime < STOP_TIME:
        if (instrument == 'EPD') and (level in LS_LEVELS):
            return True
        # if (instrument == 'MAG') and (level in LS_LEVELS):
        #     return True
        if datasetId == 'SOLO_LL02_EPD-EPT-ASUN-RATES':
            return True

    return False


def sync():
    assert os.uname().nodename in ['brain', 'spis', 'irony'], \
        'This code is not meant to run on this machine.'

    ROOT_DIR = '/home/erjo/temp/soar'

    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H.%M.%S")
    removal_dir = os.path.join(ROOT_DIR, f'removal_{timestamp}')

    # Configuring the logger appears necessary to get all the logging output.
    # stream = sys.stdout : Log to stdout (instead of stderr).
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.basicConfig(
        filename=os.path.join(ROOT_DIR, f'test_mirror_sync.{timestamp}.log'),
        level=logging.INFO,
    )

    erikpgjohansson.solo.soar.mirror.sync(
        syncDir                  = os.path.join(ROOT_DIR, 'mirror'),
        tempDownloadDir          = os.path.join(ROOT_DIR, 'download'),
        datasetsSubsetFunc       = datasets_include_func,
        deleteOutsideSubset      = True,
        nMaxNetDatasetsToRemove  = 25,
        tempRemovalDir           = removal_dir,
        removeRemovalDir         = False,
    )


# IMPLEMENTATION NOTE: This code makes it possible to execute the sync from
# the bash command line:
# >> python -m 'erikpgjohansson.solo.soar.test_mirror'
if __name__ == '__main__':
    sync()
