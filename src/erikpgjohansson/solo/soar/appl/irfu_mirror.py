'''
Code for implementing the "official" IRFU-internal SOAR data mirror.

This module is intended to only contain code for that particular mirror, i.e.
only code that "configures" that particular mirror.

Initially created 2021-01-18 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import datetime
import erikpgjohansson.solo.soar.mirror
import logging
import numpy as np
import os


'''
'''


class DatasetsSubset(erikpgjohansson.solo.soar.mirror.DatasetsSubset):

    def dataset_in_subset(self, instrument, level, begin_dt64, dsid):
        # IMPLEMENTATION NOTE: Assertions to ensure that a bad interface
        # (e.g. interface changes, bad calls) does not accidentally lead to
        # returning False, leading to deleting many datasets.
        assert type(instrument) is str
        assert type(level) is str
        assert isinstance(begin_dt64, np.datetime64)
        assert type(dsid) is str

        # NOTE: Include all time periods. ==> Do not check begin_dt64.

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
    MIRROR_ADMIN_DIR = '/data/solo/soar_mirror_admin/'
    TEMP_DOWNLOAD_DIR = os.path.join(MIRROR_ADMIN_DIR, 'download')
    TEMP_REMOVAL_PARENT_DIR = os.path.join(MIRROR_ADMIN_DIR, 'removal')
    SYNC_DIR = '/data/solo/soar'

    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%dT%H.%M.%S")

    log_file = os.path.join(
        f'/home/erjo/logs/'
        f'so_soar_irfu_mirror_sync.{os.uname().nodename}.{timestamp_str}.log',
    )
    removal_dir = os.path.join(TEMP_REMOVAL_PARENT_DIR, timestamp_str)

    # Configuring the logger appears necessary to get all the logging output.
    # stream = sys.stdout : Log to stdout (instead of stderr).
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='{asctime} {levelname:<8} {message}',
        style='{',
    )

    erikpgjohansson.solo.soar.mirror.sync(
        sync_dir                  = SYNC_DIR,
        temp_download_dir         = TEMP_DOWNLOAD_DIR,
        removal_dir               = removal_dir,
        remove_removal_dir        = False,   # TEMP?
        dsss                      = DatasetsSubset(),
        delete_outside_subset     = True,
        n_max_datasets_net_remove = 25,
    )


# IMPLEMENTATION NOTE: This code makes it possible to execute the sync from
# the bash command line:
# >> python -m 'erikpgjohansson.solo.soar.irfu_mirror'
if __name__ == '__main__':
    sync()
