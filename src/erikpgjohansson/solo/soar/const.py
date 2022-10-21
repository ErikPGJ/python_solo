'''
Module for constants.

In practice constants used by erikpgjohansson.solo.soar.mirror.sync().

In practice also a module for configuration w.r.t. some debugging.
'''
'''
BOGIQ
-----
PROPOSAL: Move constants to "settings" dictionary.
    PROPOSAL: Custom dictionary with hardcoded set of permitted keys.
PROPOSAL: Move constants to erikpgjohansson.solo.soar.mirror.sync() keyword
          arguments with default values.
'''


LS_SOAR_INSTRUMENTS = ('EPD', 'MAG', 'SWA')
'''List of instruments for which lists of datasets should be retrieved from
SOAR before syncing datasets. Must include every instrument for which
datasets is retrieved.'''

FILE_REMOVAL_COMMAND_LIST = ['remove_to_trash', 'SOAR_sync']
# FILE_REMOVAL_COMMAND_LIST = ['rm', '-v']
'''Command and arguments to use for removing old local datasets.
Paths to actual files or directory (!) to remove are added as additional
arguments at the end.

NOTE: "remove_to_trash" is one of Erik P G Johansson's private bash scripts
that moves files/directories to an automatically selected "trash" directory
("recycle bin").'''


CREATE_DIR_PERMISSIONS = 0o755


N_EXCESS_DATASETS_PRINT = 25
'''Number of local datasets to log, and that would be removed if it were not
for the triggering of the nMaxNetDatasetsToRemove failsafe.'''


SOAR_TAP_URL = 'http://soar.esac.esa.int/soar-sl-tap'
'''URL to use for automatic downloads from SOAR. Must be extended and
arguments added to actually be used.'''


FILE_SUFFIX_IGNORE_LIST = ['.zip', '.jp2', '.h5', '.bin', '.fits']
'''File suffixes of SOAR files whose dataset filenames are permitted to not be
recognized (parsable) by the code. These files will effectively be ignored.

NOTE: It is assumed that all filenames with any other file suffix than listed
here can be handled. This means effectively that all file types are explicitly
"listed" one way or another. In case a new file type which this code can not
handle is added to SOAR, then that file type must be added to the list.
'''


USE_PARALLEL_DOWNLOADS = False
# USE_PARALLEL_DOWNLOADS = True
'''Whether to use an experimental parallelized version of batch downloads.
Speeds up downloads.'''
