'''
Code for implementing the "official" IRFU-internal SOAR data mirror.

This module is intended to only contain code for that particular mirror, i.e.
only code that "configures" that particular mirror.

Initially created 2021-01-18 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import datetime
import erikpgjohansson.solo.soar.mirror
import logging
import os


'''
PROPOSAL: sync() arguments for paths.'''


def datasets_include_func(instrument, level, beginTime, dsid):
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

    if dsid in ('SOLO_LL02_SWA-PAS-MOM', 'SOLO_LL02_MAG'):
        return True
    elif instrument == 'EPD' and level in ['L1', 'L2']:
        return True
    elif instrument == 'MAG' and level in ['L2']:
        return True
    elif instrument == 'SWA' and level in ['L1', 'L2']:
        return True
    elif dsid == 'SOLO_L3_SWA-EAS-NMPAD-PSD':
        # NOTE: One can falsely be led to believe that this is the only L3
        # SWA DSID, but that is wrong. There is at least also
        # "solo_L3_swa-his-comp-10min".
        return True
    else:
        return False


def sync():
    # NOTE: Script can be used on irony if SO directories have been
    # mounted.
    assert os.uname().nodename in ['brain', 'spis', 'anna', 'irony'], (
        'This code is not intended to run on this machine (not'
        ' configured for it).'
    )

    # NOTE: const.LS_SOAR_INSTRUMENTS determines which instruments can at
    # all be downloaded.
    MIRROR_ADMIN = '/data/solo/soar_mirror_admin/'
    TEMP_DOWNLOAD_DIR = os.path.join(MIRROR_ADMIN, 'download')
    TEMP_REMOVAL_PARENT_DIR = os.path.join(MIRROR_ADMIN, 'removal')
    SYNC_DIR = '/data/solo/soar'

    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H.%M.%S")

    log_file = os.path.join(
        f'/home/erjo/logs/'
        f'so_soar_irfu_mirror_sync.{os.uname().nodename}.{timestamp}.log',
    )
    removal_dir = os.path.join(TEMP_REMOVAL_PARENT_DIR, timestamp)

    # Configuring the logger appears necessary to get all the logging output.
    # stream = sys.stdout : Log to stdout (instead of stderr).
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='{asctime} {levelname:<8} {message}',
        style='{',
    )

    erikpgjohansson.solo.soar.mirror.sync(
        syncDir                   = SYNC_DIR,
        tempDownloadDir           = TEMP_DOWNLOAD_DIR,
        tempRemovalDir            = removal_dir,
        removeRemovalDir          = False,   # TEMP?
        datasetsSubsetFunc        = datasets_include_func,
        deleteOutsideSubset       = True,
        nMaxNetDatasetsToRemove   = 25,
    )


# IMPLEMENTATION NOTE: This code makes it possible to execute the sync from
# the bash command line:
# >> python -m 'erikpgjohansson.solo.soar.irfu_mirror'
if __name__ == '__main__':
    sync()
