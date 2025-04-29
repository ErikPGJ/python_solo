'''
Code for implementing a "manual test" SOAR data mirror.

This module is intended to only contain code for that particular mirror, i.e.
only code that "configures" that particular mirror.

Initially created 2021-04-23 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import datetime
import erikpgjohansson.solo.soar.mirror
import logging
import numpy
import os


'''
'''


ROOT_DIR = '/home/erjo/temp/soar'
'''
NOTE: Hard-coded local directory.
'''


def datasets_subset_func(instrument, level, beginTime, dsid):
    '''
Function which determines whether a specific dataset is included/excluded in
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

    # NOTE: Should be configured to match only a small non-zero number of
    # datasets.
    START_TIME_DT64 = numpy.datetime64('2020-08-13T00:00:00.000')
    STOP_TIME_DT64  = numpy.datetime64('2020-08-14T00:00:00.000')

    # LS_LEVELS = ['LL02', 'L1', 'L2']
    LS_LEVELS = ['L1']
    if START_TIME_DT64 <= beginTime < STOP_TIME_DT64:
        if (instrument == 'EPD') and (level in LS_LEVELS):
            return True
        # if (instrument == 'MAG') and (level in LS_LEVELS):
        #     return True
        if dsid == 'SOLO_LL02_EPD-EPT-ASUN-RATES':
            return True

    return False


def sync():
    assert os.uname().nodename in ['brain', 'spis', 'irony'], \
        'This code is not designed to run on this machine.'

    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%dT%H.%M.%S")
    removal_dir = os.path.join(ROOT_DIR, f'removal_{timestamp_str}')

    # Configuring the logger appears necessary to get all the logging output.
    # stream = sys.stdout : Log to stdout (instead of stderr).
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.basicConfig(
        filename=os.path.join(
            ROOT_DIR, f'mtest_mirror_sync.{timestamp_str}.log',
        ),
        level=logging.INFO,
        format='{asctime} {levelname:<8} {message}',
        style='{',
    )

    erikpgjohansson.solo.soar.mirror.sync(
        syncDir                 = os.path.join(ROOT_DIR, 'mirror'),
        tempDownloadDir         = os.path.join(ROOT_DIR, 'download'),
        datasetsSubsetFunc      = datasets_subset_func,
        deleteOutsideSubset     = True,
        nMaxNetDatasetsToRemove = 25,
        tempRemovalDir          = removal_dir,
        removeRemovalDir        = False,
    )


# IMPLEMENTATION NOTE: This code makes it possible to execute the sync from
# the bash command line:
# >> python -m 'erikpgjohansson.solo.soar.appl.mtest_mirror'
if __name__ == '__main__':
    sync()
