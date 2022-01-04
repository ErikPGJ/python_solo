# -*- coding: UTF-8 -*-
'''
Utilities for synching SOAR datasets with a local directory.


Initially created 2020-12-21 by Erik P G Johansson, IRF Uppsala, Sweden.
'''
'''
BOGIQ
=====
NOTE: SOAR may remove datasets, i.e. state that a previously latest dataset
      version is no longer the latest dataset with supplying an even later
      version.

PROBLEM: How handle that a download may take more time than the time between
    cron jobs?
    PROPOSAL: Set an (approximate) max download time. Abort downloads after
              that and just move the downloaded files after that.
        NOTE: Even if the actual SOAR sync slightly overlaps with the next SOAR
            sync (using the same download directory), it should still protect
            against avalanching cron SOAR syncs.
        PROPOSITION: Can only specify a maximum download time.
            CON: Can use time prediction to abort before a download expected to
                 exceed the time limit.
                CON: Works badly for very few downloaded files within time
                     limit, e.g. the first one.

PROPOSAL: Verify that downloaded file is the intended one (filename incl.
    version, size).

PROPOSAL: Do not sync all data items together. Split up syncing into multiple
          syncs for various subsets of data items.
    PRO: Good to split up long downloads (~days).
    NOTE: Must ensure that algorithm only REMOVES datasets from chosen subset
        (and not from entire set, which could lead to removing all datasets
        outside the subset)

PROPOSAL: Basic syncing algorithm:
    Derive DST for datasets on disk.
    Download DST for online SOAR data (only latest versions).
    --
    Derive list of datasets present in SOAR but not locally.
    Derive list of datasets present locally but not in SOAR.
        This covers
            (1) SOAR "data items" that have been removed.
            (2) SOAR datasets where the latest version(s) has been removed.
            (3) Local datasets that have been superseeded by later SOAR
                datasets (and should be removed so that there will not
                simultaneously be two versions for the same data item).
            (4) Local datasets of which there are multiple versions
                (by mistake).
    Download datasets to temporary directory.
        NOTE: Actually downloaded datasets may technically not be exactly
              the ones on the list since
              only the latest version (in that instant) can be downloaded.
    Move downloaded datasets to final destination.
    Remove datasets.
        Check: Number of net removals is "not too large".
    ...

PROPOSAL: erikpgjohansson.so.soar_utils.log_DST() returns string that can be
          indented by caller.

PROPOSAL: Separate function for removing files instead of command+arguments
          (FILE_REMOVAL_COMMAND_LIST).
'''


import codetiming
import erikpgjohansson.so.soar
import erikpgjohansson.so.iddt
import numpy as np
import subprocess







@codetiming.Timer('sync')
def sync(syncDir, tempDownloadDir, datasetsSubsetFunc,
         deleteOutsideSubset=False,
         downloadLogFormat='short',
         nMaxNetDatasetsToRemove=10):
    '''
Sync local directory with subset of online SOAR datasets.

NOTE/BUG: Does not correct the locations of misplaced datasets.


Parameters
----------
syncDir : String. Path.
tempDownloadDir : String. Path.
    Must be empty on datasets. Otherwise those will be moved too.
datasetsSubsetFunc : Function (instrument=str, level=str) --> bool
    Function which determines whether a specific dataset should be included
    in the sync.
deleteOutsideSubset : Boolean
    Whether local datasets not specified by datasetsSubsetFunc should be
    deleted or not.
    False: Local datasets which are excluded can not be removed by the syncing.
    True:  Local datasets which are excluded will be removed by the syncing.
    NOTE: This distinction is important when one wants to
        update only some of the local datasets but not other (e.g. for speed).
nMaxNetDatasetsToRemove : int
    Maximum permitted net number of deleted datasets ("nDeletedDatasets minus
    nNewDatasets"). This is a failsafe (assertion), to protect against deleting
    too many slow-to-redownload datasets due to bugs or misconfiguration (e.g.
    datasetsSubsetFunc).


Returns
-------
None.
'''
    '''
PROPOSAL: Limit number of datasets that are allowed to be removed (assertion).
    PRO: Limits consequences of misconfiguration (datasetsSubsetFunc)
         or bugs leading to removing many slow-to-download datasets.
    PROPOSAL: Argument.
    PROPOSAL: By number of datasets.
    PROPOSAL: By number of bytes.

BUG: Does not correct the locations of misplaced datasets.
    PROBLEM: How handle symlinks?
        Ex: Might be used for duplicating locations for backward compatibility.
    PROPOSAL: Make relative dataset path part of the information that should be
              synced.
        CON: Will re-download files that have moved.

~BUG: If download directory is not empty on datasets, then those datasets will
      be moved too.
BUG: Can not handle multiple identical datasets.

~BUG: Not sure about behaviour for datasets (mistakenly) not recognized by
    erikpgjohansson.so.utils.parse_dataset_filename among the SOAR datasets.
        NOTE: Should only happen if datasetsSubsetFunc() uses time.

BUG: Gets frequent error messages when calling from bash/python wrapper script
so_irfu_soar_sync.py on brain. Works better on spis(?). /2021-01-19
"""""""
Traceback (most recent call last):
  File "/amd/hem/export/home/erjo/bin/global/so_irfu_soar_sync.py", line 60, in <module>
    main(sys.argv[1:])   # # NOTE: sys.argv[0] är inget CLI-argument. ==> Ignorera
  File "/amd/hem/export/home/erjo/bin/global/so_irfu_soar_sync.py", line 56, in main
    erikpgjohansson.so.irfu_soar_mirror.sync()
  File "/home/erjo/python_copy/erikpgjohansson/so/irfu_soar_mirror.py", line 33, in sync
    nMaxNetDatasetsToRemove = 20)
  File "/home/erjo/python_copy/erikpgjohansson/so/soar_mirror.py", line 172, in sync
    localDst = erikpgjohansson.so.soar_utils.derive_DST_from_dir(syncDir)
  File "/home/erjo/python_copy/erikpgjohansson/so/soar_utils.py", line 383, in derive_DST_from_dir
    fileSizeList    += [os.stat(filePath).st_size]
PermissionError: [Errno 13] Permission denied: '/data/solo/soar/swa/L2/swa-eas1-nm3d-psd/2020/10/solo_L2_swa-eas1-nm3d-psd_20201011T000035-20201011T235715_V01.cdf'
"""""""
    PROPOSAL: Inspect local datasets before download SOAR datasets list.
        PRO: Faster to fail when fails due to not being able to access files
             (see bug).
    PROPOSAL: Try to trigger automount.
        NOTE: create_pull_push_sync_cmd: ls "$local_path" >> /dev/null
        PROPOSAL: Change current directory.
        PROPOSAL: List files in directory.
    PROPOSAL: Assertion for datasetsSubsetFunc never return True
        (for any of the calls made).

    PROPOSAL: Do not download all missing datasets at once.
        PROPOSAL: Download one at a time, and send it to SOAR directory
                  immediately after.
            PRO: Better when interrupted.
                CON: Not when debugging.
            CON: Can not use batch download function, with e.g. time prediction.
                CON-PROPOSAL: New feature: Function argument for what to do with
                              just downloaded dataset.
            CON-PROBLEM: When should one remove datasets? Before? After? During
                         somehow?
    '''
    # ASSERTIONS
    erikpgjohansson.so.asserts.is_dir(syncDir)
    erikpgjohansson.so.asserts.is_dir(tempDownloadDir)
    assert callable(datasetsSubsetFunc)
    assert type(nMaxNetDatasetsToRemove) in [int, float]



    # DEBUG: Settings for debugging.
    # --
    # NOTE: Does not by itself change SOAR datasets to be listed as 0 bytes.
    DEBUG_DOWNLOAD_EMPTY_DATASETS  = False    # NOTE: Default=False.
    DEBUG_DOWNLOAD_DATASETS        = True
    DEBUG_DELETE_LOCAL_DATASETS    = True
    DEBUG_MOVE_DOWNLOADED_DATASETS = True

    # "remove_to_trash" is one of Erik P G Johansson's private bash scripts
    # that moves files/directories to an automatically selected "trash"
    # directory ("recycle bin").
    #FILE_REMOVAL_COMMAND_LIST = ['remove_to_trash', 'SOAR_sync']
    FILE_REMOVAL_COMMAND_LIST = ['rm', '-v']
    CREATE_DIR_PERMISSIONS = 0o755

    #================================
    # Create table of local datasets
    #================================
    # NOTE: Explicitly includes ALL versions, i.e. also NON-LATEST versions.
    # There should theoretically only be one version of each dataset locally,
    # but if there are more, then they should be included so that they can be
    # removed (or kept in unlikely case of SOAR downversioning datasets).
    localDst = erikpgjohansson.so.soar_utils.derive_DST_from_dir(syncDir)
    # NOTE: Not logging this to reduce amount of logging.
    # NOTE: Identical to later logging when deleteOutsideSubset=False.
    #print('Pre-existing local datasets that should be synced:')
    #erikpgjohansson.so.soar_utils.log_DST(localDst)



    #========================================
    # Download table of online SOAR datasets
    #========================================
    (soarDst, _JsonDict) = erikpgjohansson.so.soar.download_SOAR_DST()
    print(
        'All online SOAR datasets'
        ' (synced and non-synced; all dataset versions):'
    )
    erikpgjohansson.so.soar_utils.log_DST(soarDst)
    erikpgjohansson.so.soar_utils.log_codetiming()   # DEBUG

    # ASSERTION: SOAR DST not empty.
    # IMPLEMENTATION NOTE: Just a hunch that SOAR might one day return a DST
    # with zero datasets by mistake. This could in turn lead to deleting all
    # local datasets.
    nRows = erikpgjohansson.so.soar_utils.nRows_DST(soarDst)
    assert nRows > 0, 'SOAR returns a zero-size datasets table.'\
        +' There seems to be something wrong with SOAR.'



    #================================================
    # Select specified subset of datasets (item IDs)
    #================================================
    bSoarSubset = _find_DST_subset(datasetsSubsetFunc,
                    instrumentArray=soarDst['instrument'],
                    levelArray     =soarDst['processing_level'],
                    beginTimeArray =soarDst['begin_time_FN'])
    soarDst = erikpgjohansson.so.soar_utils.index_DST(soarDst, bSoarSubset)
    print(
        'Subset of online SOAR datasets that should be synced with local'
        ' datasets:')
    erikpgjohansson.so.soar_utils.log_DST(soarDst)
    erikpgjohansson.so.soar_utils.log_codetiming()   # DEBUG



    #==========================================================================
    # Only keep latest version of each online SOAR dataset in table
    # -------------------------------------------------------------
    # IMPLEMENTATION NOTE: This can be slow. Therefore doing this first AFTER
    # selecting subset of datasets. Particularly useful for small test subsets.
    #==========================================================================
    bLv     = erikpgjohansson.so.soar_utils.find_latest_versions(
        soarDst['item_id'],
        soarDst['item_version'])
    soarDst = erikpgjohansson.so.soar_utils.index_DST(soarDst, bLv)

    print(
        'Latest versions of all online SOAR datasets (synced and non-synced):'
    )
    erikpgjohansson.so.soar_utils.log_DST(soarDst)
    erikpgjohansson.so.soar_utils.log_codetiming()   # DEBUG



    # ASSERT: The subset of SOAR is non-empty.
    # IMPLEMENTATION NOTE: This is to prevent mistakenly deleting all local
    # files due to faulty datasetsSubsetFunc.
    assert erikpgjohansson.so.soar_utils.nRows_DST(soarDst) > 0, (
        'Trying to sync with empty subset of SOAR datasets.'
        ' datasetsSubsetFunc could be faulty.'
    )

    # Local datasets
    if not deleteOutsideSubset:
        bLocalSubset = _find_DST_subset(datasetsSubsetFunc,
                        instrumentArray=localDst['instrument'],
                        levelArray     =localDst['processing_level'],
                        beginTimeArray =localDst['begin_time_FN'])
        localDst = erikpgjohansson.so.soar_utils.index_DST(
            localDst, bLocalSubset)
        print(
            'NOTE: Only syncing against subset of local datasets.'
            ' Will NOT delete outside datasets outside the specified subset.'
        )
    else:
        print(
            'NOTE: Syncing against all local datasets (in specified directory).'
            ' Will DELETE datasets outside the specified subset.'
        )
    # NOTE: localDst has no begin_time. Can therefore not log.
    print('Pre-existent set of local datasets that should be synced/updated:')
    erikpgjohansson.so.soar_utils.log_DST(localDst)
    erikpgjohansson.so.soar_utils.log_codetiming()   # DEBUG

    #================================================================
    # Find (1) datasets to download, and (2) local datases to delete
    #================================================================
    (bSoarMissing, bLocalExcess) = find_DST_difference(
        soarDst['file_name'],
        localDst['file_name'],
        soarDst['file_size'],
        localDst['file_size'])

    soarMissingDst = erikpgjohansson.so.soar_utils.index_DST(soarDst,  bSoarMissing)
    localExcessDst = erikpgjohansson.so.soar_utils.index_DST(localDst, bLocalExcess)

    print('Online SOAR datasets that need to be downloaded:')
    erikpgjohansson.so.soar_utils.log_DST(soarMissingDst)
    print('Local datasets that need to be removed:')
    erikpgjohansson.so.soar_utils.log_DST(localExcessDst)

    # ASSERTION
    # NOTE: Deliberately doing this after logging datasets to download
    # and delete.
    nNetDatasetsToRemove = \
        erikpgjohansson.so.soar_utils.nRows_DST(localExcessDst) \
      - erikpgjohansson.so.soar_utils.nRows_DST(soarMissingDst)
    assert nNetDatasetsToRemove <= nMaxNetDatasetsToRemove, \
        'Net number of datasets to remove ({0}) is larger than permitted ({1}). This might indicate a bug or configuration error. This assertion is a failsafe.'.format(
            nNetDatasetsToRemove, nMaxNetDatasetsToRemove)



    #===========================
    # Download missing datasets
    #===========================
    print('Downloading {0} datasets'.format(soarMissingDst['item_id'].size))
    if DEBUG_DOWNLOAD_DATASETS:
        erikpgjohansson.so.soar_utils.download_latest_datasets_batch(
            soarMissingDst['item_id'],
            soarMissingDst['file_size'],
            tempDownloadDir,
            logFormat=downloadLogFormat,
            debugDownloadingEnabled=True,
            debugCreateEmptyFiles=DEBUG_DOWNLOAD_EMPTY_DATASETS)
    else:
        print('DEBUG: Disabled downloading datasets.')
        for fileName in soarMissingDst['file_name']:
            print('Virtually downloading "{0}"'.format(fileName))

    #==============================
    # Remove (some) local datasets
    #==============================
    # NOTE: Deliberately removing datasets AFTER having successfully downloaded
    # new ones (including potential replacements for removed files).
    # This is to avoid that bugs lead to unnecessarily deleting datasets that
    # are hard/slow to replace.
    nRows = erikpgjohansson.so.soar_utils.nRows_DST(localExcessDst)
    print('Removing {0} local datasets'.format(nRows))
    if DEBUG_DELETE_LOCAL_DATASETS:
        if nRows > 0:
            pathsToRemoveList = localExcessDst['file_path'].tolist()
            stdoutBytes = subprocess.check_output(
                FILE_REMOVAL_COMMAND_LIST + pathsToRemoveList)
            stdoutStr = str(stdoutBytes, 'utf-8')
            print(stdoutStr)    # NOTE: Always prints line break.
    else:
        print('DEBUG: Disabled removing local datasets.')
        for iRow in range(nRows):
            print('Virtually removing "{0}" ({1} bytes)'.format(
                localExcessDst['file_path'][iRow],
                localExcessDst['file_size'][iRow]))



    #====================================================
    # Move downloaded datasets into local directory tree
    #====================================================
    if DEBUG_MOVE_DOWNLOADED_DATASETS:
        print('Moving downloaded datasets to selected directory structure (if there are any).')
        erikpgjohansson.so.iddt.copy_move_datasets_to_irfu_dir_tree(
            'move', tempDownloadDir, syncDir,
            dirCreationPermissions=CREATE_DIR_PERMISSIONS)
    else:
        print('DEBUG: Disabled moving downloaded datasets to local datasets (IDDT).')







#def find_DST_difference(itemIdArray1, versionArray1, itemIdArray2, versionArray2):
def find_DST_difference(fileNameArray1, fileNameArray2,
                        fileSizeArray1, fileSizeArray2):
    '''
Find both set differences between lists of strings as boolean index arrays.
Intended for dataset filenames, to determine which datasets need to be synched
(removed, downloaded).

NOTE: Does not check file size or content.


Parameters
----------
fileNameArray1 : 1D numpy.ndarray of strings.
fileNameArray2 : 1D numpy.ndarray of strings.
NOTE: Arrays (separately) do not need to contain unique strings, though
that is the intended use.


Returns
-------
(bDiff1, bDiff2)
    '''
    '''
PROPOSAL: Use filenames, not itemId+version.
PROPOSAL: Include file sizes.
    '''
    # assert type(fileNameArray1) == np.ndarray
    # assert type(fileNameArray2) == np.ndarray
    # assert fileNameArray1.dtype == np.dtype('object')
    # assert fileNameArray2.dtype == np.dtype('object')
    erikpgjohansson.so.soar_utils.assert_col_array(fileNameArray1, np.dtype('O'))
    erikpgjohansson.so.soar_utils.assert_col_array(fileNameArray2, np.dtype('O'))

    # assert type(fileSizeArray1) == np.ndarray
    # assert type(fileSizeArray2) == np.ndarray
    # assert fileSizeArray1.dtype == np.dtype('int64')
    # assert fileSizeArray2.dtype == np.dtype('int64')
    erikpgjohansson.so.soar_utils.assert_col_array(fileSizeArray1, np.dtype('int64'))
    erikpgjohansson.so.soar_utils.assert_col_array(fileSizeArray2, np.dtype('int64'))



    # NOTE: Suspect there is some way of doing this using numpy functionality
    # but has not found it.
    # FNS = File Name & Size
    fnsArray1 = np.array(
        list(zip(fileNameArray1, fileSizeArray1)),
        dtype=[('fileName', fileNameArray1.dtype),
               ('fileSize', fileSizeArray1.dtype)])
    fnsArray2 = np.array(
        list(zip(fileNameArray2, fileSizeArray2)),
        dtype=[('fileName', fileNameArray2.dtype),
               ('fileSize', fileSizeArray2.dtype)])

    bDiff1 = ~np.isin(fnsArray1, fnsArray2)
    bDiff2 = ~np.isin(fnsArray2, fnsArray1)



    # ASSERTIONS
    # NOTE: Many re-implementations have failed this assertion.
    assert type(bDiff1) == np.ndarray
    assert type(bDiff2) == np.ndarray

    return (bDiff1, bDiff2)






@codetiming.Timer('_find_DST_subset')
def _find_DST_subset(datasetIncludeFunc,
                     instrumentArray, levelArray, beginTimeArray):
    '''
Returns indices to datasets that are permitted by datasetIncludeFunc.
Basically just iterates over datasetIncludeFunc.


Parameters
----------
datasetIncludeFunc : Function handle.
    Checks whether a given dataset should be included.
instrumentArray : numpy array
levelArray : numpy array


Returns
-------
bSubset : numpy array
'''
    erikpgjohansson.so.soar_utils.assert_col_array(
        instrumentArray, np.dtype('O'))
    erikpgjohansson.so.soar_utils.assert_col_array(
        levelArray,      np.dtype('O'))
    erikpgjohansson.so.soar_utils.assert_col_array(
        beginTimeArray,  np.dtype('<M8[ms]'))   # Omit type?

    bSubset = np.zeros(instrumentArray.shape, dtype=bool)
    #for i, _instrument in enumerate(instrumentArray):
    for i in range(instrumentArray.size):

        # DEBUG
        # isMag = (soarDst['instrument'][i]       == 'MAG')
        # isL2  = (soarDst['processing_level'][i] =='L2')
        # if isMag & isL2:
        #     print('MAG L2')

        bSubset[i] = datasetIncludeFunc(
                        instrument=instrumentArray[i],
                        level     =levelArray[i],
                        beginTime =beginTimeArray[i])

    return bSubset







def test_datasets_include_func(instrument, level, beginTime):
    '''
Function that determines whether dataset is included/excluded in syncing.

NOTE: For testing purposes.


Parameters
----------
beginTime : numpy array, shape=(), numpy.datetime64
'''
    MIN_DT64 = np.datetime64('2020-06-30T23:00:00.000')
    MAX_DT64 = np.datetime64('2020-07-01T00:00:00.000')

    assert type(beginTime) == np.datetime64

    return (
        instrument=='EPD'
        and level in ['L1']
        and MIN_DT64 <= beginTime and beginTime <= MAX_DT64)
