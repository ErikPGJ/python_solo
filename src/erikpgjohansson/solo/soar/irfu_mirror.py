'''
Code for implementing the "official" IRFU-internal SOAR data mirror.

This module is intended to only contain code for that particular mirror, i.e.
only code that "configures" that particular mirror.

Initially created 2021-01-18 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import erikpgjohansson.solo.soar.mirror
import logging
import os
import sys


def _IRFU_datasets_include_func(instrument, level, beginTime, datasetId):
    '''
Function that determines whether a specific dataset is included/excluded in
the sync.

Defines the subset of SOAR datasets that should be "officially" synced for
IRFU staff (nas24:/data/solo/soar/).


Parameters
----------
instrument : String
level      : String

Returns
-------
include: bool
    Whether datasets should be included or not.
'''
    # NOTE: Include all time periods. ==> Do not check beginTime (for now).

    if datasetId in ('SOLO_LL02_SWA-PAS-MOM', 'SOLO_LL02_MAG'):
        return True
    elif instrument == 'MAG' and level in ['L2']:
        return True
    elif instrument == 'EPD' and level in ['L1', 'L2']:
        return True
    elif instrument == 'SWA' and level in ['L1', 'L2']:
        return True
    else:
        return False


def sync():
    # NOTE: Script can be used on irony if SO directories have been
    # mounted.
    assert os.uname().nodename in ['brain', 'spis', 'irony'], (
        'This code is not intended to run on this machine (not'
        ' configured for it.'
    )

    # SOAR_TABLE_CACHE_JSON_FILE = "/home/erjo/temp/soar/soar.json"

    # Configuring the logger appears necessary to get all the logging output.
    # stream = sys.stdout : Log to stdout (instead of stderr).
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    erikpgjohansson.solo.soar.mirror.sync(
        syncDir                   = '/data/solo/soar',
        tempDownloadDir           = '/data/solo/soar/downloads',
        datasetsSubsetFunc        = _IRFU_datasets_include_func,
        downloadLogFormat         = 'long',
        deleteOutsideSubset       = True,
        nMaxNetDatasetsToRemove   = 25,
        # SoarTableCacheJsonFilePath=SOAR_TABLE_CACHE_JSON_FILE,
    )

    # 2021-12-17: "AssertionError: Net number of datasets to remove (25) is
    #              larger than permitted (20). "


# IMPLEMENTATION NOTE: This code makes it possible to execute the sync from
# the bash command line:
# >> python -m 'erikpgjohansson.solo.soar.irfu_mirror'
if __name__ == '__main__':
    sync()
