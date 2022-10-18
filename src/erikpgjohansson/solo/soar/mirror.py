'''
Utilities for synching SOAR datasets with a local directory.


Initially created 2020-12-21 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import codetiming
import erikpgjohansson.solo.iddt
import erikpgjohansson.solo.soar.const as const
import erikpgjohansson.solo.soar.dwld
import erikpgjohansson.solo.soar.utils
import erikpgjohansson.solo.utils
import logging
import numpy as np
import os
import subprocess
import typing
import sys


'''
BOGIQ
=====
NOTE: SOAR may remove datasets, i.e. state that a previously latest dataset
      version is no longer the latest dataset without supplying a later
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

PROPOSAL: Use timestamped directory under download directory.
    PRO: Easier to just disable moving datasets from it.
        CON: On brain/spis, downloads/ is under the directory being synced.
             If there are remaining datasets there, then they will be
             identified as being local datasets that are part of the mirror.

PROPOSAL: erikpgjohansson.solo.soar.utils.log_DST() returns string that can be
          indented by caller.

PROPOSAL: Abbreviations for specific subsets.
    PROPOSAL: Something for online SOAR datasets
        PROPOSAL: Include "online" since "SOAR dataset" could imply a
                  local dataset from SOAR.
            OLS = OnLine SOAR
    PROPOSAL: "Local" is opposite of "online SOAR datasets". Needs no
              abbreviation.
    PROPOSAL: Something for the subset of datasets that should be
        SSS = Synced SOAR Subset
        SSS = ~Synced SubSet
        TODO-DEC: Only latest version datasets? Only subset of item IDs?
    AV = All Versions
    LV = Latest Version(s) only
    LVD = Latest Version Datasets
'''


@codetiming.Timer('sync', logger=None)
def sync(
    syncDir, tempDownloadDir, datasetsSubsetFunc: typing.Callable,
    deleteOutsideSubset=False,
    downloadLogFormat='short',
    nMaxNetDatasetsToRemove=10,
    SoarTableCacheJsonFilePath=None,
    tempRemovalDir=None,
    removeRemovalDir=False,
    downloader: erikpgjohansson.solo.soar.dwld.Downloader
    = erikpgjohansson.solo.soar.dwld.SoarDownloader(),
):
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
        Whether local datasets for which datasetsSubsetFunc returns false
        should be deleted or not, even if there is no newer version.
        NOTE: This distinction is important when one wants to
        update only some of the local datasets but not other (e.g. for speed)
        by temporarily using another function datasetsSubsetFunc.
    nMaxNetDatasetsToRemove : int
        Maximum permitted net number of deleted datasets ("nDeletedDatasets
        minus nNewDatasets"). This is a failsafe (assertion), to protect
        against deleting too many slow-to-redownload datasets due to bugs or
        misconfiguration (e.g. "datasetsSubsetFunc").
    SoarTableCacheJsonFilePath
        Path to JSON file which is used for caching the SOAR table. May or may
        not pre-exist.
        None: Do not cache.
        This is useful for debugging (speeds up execution; can manually inspect
        SOAR table).
    tempRemovalDir
        None or path to "removal directory" to which datasets are moved before
        the directory itself is itself optionally removed (depends on argument
        "removeRemovalDir").
        Removal directory may preexist. Is created if not.
    removeRemovalDir
        Bool. If using a removal directory, then whether to actually remove the
        removal directory or keep it.


    Return values
    -------------
    None.
    '''
    '''
    BUG: Does not correct the locations of misplaced datasets.
        PROBLEM: How handle symlinks?
            Ex: Might be used for duplicating locations for backward
                compatibility.
        PROPOSAL: Make relative dataset path part of the information that
                  should be synced.
            CON: Will re-download files that have moved.

    ~BUG: If download directory is not empty on datasets, then those datasets
          will be moved too.
    BUG: Can not handle multiple identical datasets.

    ~BUG: Not sure about behaviour for datasets (mistakenly) not recognized by
        erikpgjohansson.solo.utils.parse_dataset_filename among the SOAR
        datasets.
            NOTE: Should only happen if datasetsSubsetFunc() uses time.

    BUG: Gets frequent error messages when calling from bash/python wrapper
    script so_irfu_soar_sync.py on brain. Works better on spis(?). /2021-01-19
    """""""
    Traceback (most recent call last):
      File "/amd/hem/export/home/erjo/bin/global/so_irfu_soar_sync.py",
      line 60, in <module>
        main(sys.argv[1:])   # # NOTE: sys.argv[0] är inget CLI-argument.
        ==> Ignorera
      File "/amd/hem/export/home/erjo/bin/global/so_irfu_soar_sync.py",
      line 56, in main
        erikpgjohansson.so.irfu_soar_mirror.sync()
      File "/home/erjo/python_copy/erikpgjohansson/so/irfu_soar_mirror.py",
      line 33, in sync
        nMaxNetDatasetsToRemove = 20)
      File "/home/erjo/python_copy/erikpgjohansson/so/soar_mirror.py",
      line 172, in sync
        localDst = erikpgjohansson.so.soar_utils.derive_DST_from_dir(syncDir)
      File "/home/erjo/python_copy/erikpgjohansson/so/soar_utils.py",
      line 383, in derive_DST_from_dir
        fileSizeList    += [os.stat(filePath).st_size]
    PermissionError: [Errno 13] Permission denied:
    '/data/solo/soar/swa/L2/swa-eas1-nm3d-psd/2020/10/
    solo_L2_swa-eas1-nm3d-psd_20201011T000035-20201011T235715_V01.cdf'
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
            CON: Can not use batch download function, with e.g. time
                 prediction.
                CON-PROPOSAL: New feature: Function argument for what to do
                              with just downloaded dataset.
            CON-PROBLEM: When should one remove datasets? Before? After? During
                         somehow?
    '''
    L = logging.getLogger(__name__)

    # ASSERTIONS
    assert isinstance(downloader, erikpgjohansson.solo.soar.dwld.Downloader)
    erikpgjohansson.solo.asserts.is_dir(syncDir)
    erikpgjohansson.solo.asserts.is_dir(tempDownloadDir)
    assert callable(datasetsSubsetFunc)
    assert type(nMaxNetDatasetsToRemove) in [int, float]

    # IMPLEMENTATION NOTE: Useful to print Python executable to verify that
    # the correct Python environment is used.
    L.info(f'sys.executable = "{sys.executable}"')

    # ==============================
    # Create table of local datasets
    # ==============================
    # NOTE: Explicitly includes ALL versions, i.e. also NON-LATEST versions.
    # There should theoretically only be one version of each dataset locally,
    # but if there are more, then they should be included so that they can be
    # removed (or kept in the rare but possible case of SOAR
    # down-versioning datasets).
    L.info('Producing table of pre-existing local datasets.')
    localDst = erikpgjohansson.solo.soar.utils.derive_DST_from_dir(syncDir)
    erikpgjohansson.solo.soar.utils.log_DST(
        localDst, 'Pre-existing local datasets that should be synced',
    )

    # ======================================
    # Download table of online SOAR datasets
    # ======================================
    L.info('Downloading SOAR table of datasets.')
    soarDst, _JsonDict = erikpgjohansson.solo.soar.dwld.download_SOAR_DST(
        downloader, CacheJsonFilePath=SoarTableCacheJsonFilePath,
    )
    erikpgjohansson.solo.soar.utils.log_DST(
        soarDst,
        'All online SOAR datasets'
        ' (synced and non-synced; all dataset versions)',
    )

    # ASSERTION: SOAR DST is not empty
    # --------------------------------
    # IMPLEMENTATION NOTE: SOAR might one day return a DST with zero datasets
    # by mistake or due to bug. This could in turn lead to deleting all local
    # datasets.
    assert soarDst.n() > 0, (
        'SOAR returned an empty datasets table.'
        ' This should imply that there is something wrong with either (1) '
        'SOAR, or (2) this software.'
    )

    soarMissingDst, localExcessDst = _calculate_sync_dir_update(
        soarDst=soarDst,
        localDst=localDst,
        datasetsSubsetFunc=datasetsSubsetFunc,
        deleteOutsideSubset=deleteOutsideSubset,
        nMaxNetDatasetsToRemove=nMaxNetDatasetsToRemove,
    )

    _execute_sync_dir_update(
        downloader=downloader,
        soarMissingDst=soarMissingDst,
        localExcessDst=localExcessDst,
        syncDir=syncDir,
        tempDownloadDir=tempDownloadDir,
        tempRemovalDir=tempRemovalDir,
        removeRemovalDir=removeRemovalDir,
        downloadLogFormat=downloadLogFormat,
    )

    erikpgjohansson.solo.soar.utils.log_codetiming()   # DEBUG


@codetiming.Timer('_calculate_sync_dir_update', logger=None)
def _calculate_sync_dir_update(
    soarDst, localDst, datasetsSubsetFunc,
    deleteOutsideSubset, nMaxNetDatasetsToRemove,
):
    '''Given DSTs for SOAR and local files, calculate which files should be
    removed or downloaded locally.

    (1) Converts complete SOAR datasets list
        -->SOAR subset dataset list (all versions)
        -->SOAR subset dataset list (latest versions)
    (2) Assert SOAR subset dataset list (latest versions) not empty.
    (3) Optionally trims local datasets list to subset.
    (4) Assert that not too many local datasets should be deletec.
    '''
    '''
    NOTE: Not a very "pure" function since it logs.
    PROPOSAL: Make purer logic function, without logging.
    '''
    L = logging.getLogger(__name__)

    L.info('Identifying missing datasets and datasets to remove.')

    # =====================================================================
    # Select subset of SOAR datasets (item IDs) to be synced (all versions)
    # =====================================================================
    bSoarSubset = _find_DST_subset(datasetsSubsetFunc, soarDst)
    soarSubsetDst = soarDst.index(bSoarSubset)
    erikpgjohansson.solo.soar.utils.log_DST(
        soarSubsetDst,
        'Subset of online SOAR datasets (all versions) that should be '
        'synced with local datasets',
    )

    # =========================================================================
    # Only keep latest version of each online SOAR dataset in table
    # -------------------------------------------------------------
    # IMPLEMENTATION NOTE: This can be slow. Therefore doing this first AFTER
    # selecting subset of item IDs. Particularly useful for small test subsets.
    # =========================================================================
    bLv = erikpgjohansson.solo.soar.utils.find_latest_versions(
        soarSubsetDst['item_id'],
        soarSubsetDst['item_version'],
    )
    soarSubsetLvDst = soarSubsetDst.index(bLv)

    erikpgjohansson.solo.soar.utils.log_DST(
        soarSubsetDst,
        'Subset of online SOAR datasets (latest versions) that should be '
        'synced with local datasets',
    )

    # ASSERT: The subset of SOAR is non-empty.
    # IMPLEMENTATION NOTE: This is to prevent mistakenly deleting all local
    # files due to faulty datasetsSubsetFunc.
    assert soarSubsetLvDst.n() > 0, (
        'Trying to sync with empty subset of SOAR datasets.'
        ' Argument "datasetsSubsetFunc" could be faulty.'
    )

    # Whether to hide local datasets outside of subset
    # = whether to prevent local datasets outside of subset from being
    #   potentially deleted later.
    if deleteOutsideSubset:
        L.info(
            'NOTE: Syncing against all local datasets'
            ' (in specified directory).'
            ' Will DELETE datasets outside the specified subset.',
        )
    else:
        bLocalSubset = _find_DST_subset(datasetsSubsetFunc, localDst)
        localDst = localDst.index(bLocalSubset)
        L.info(
            'NOTE: Only syncing against subset of local datasets.'
            ' Will NOT delete datasets outside the specified subset.',
        )

    # NOTE: localDst has no begin_time. Can therefore not log.
    erikpgjohansson.solo.soar.utils.log_DST(
        localDst,
        'Pre-existent set of local datasets that should be synced/updated',
    )

    # ==============================================================#
    # Find (1) datasets to download, and (2) local datasets to delete
    # ==============================================================#
    bSoarMissing, bLocalExcess = _find_DST_difference(
        soarSubsetLvDst['file_name'], localDst['file_name'],
        soarSubsetLvDst['file_size'], localDst['file_size'],
    )

    soarMissingDst = soarSubsetLvDst.index(bSoarMissing)
    localExcessDst = localDst.index(bLocalExcess)

    erikpgjohansson.solo.soar.utils.log_DST(
        soarMissingDst, 'Online SOAR datasets that need to be downloaded',
    )
    erikpgjohansson.solo.soar.utils.log_DST(
        localExcessDst, 'Local datasets that need to be removed',
    )

    # ASSERTION
    # NOTE: Deliberately doing this first after logging which datasets to
    # download and delete.
    nNetDatasetsToRemove = localExcessDst.n() - soarMissingDst.n()
    if nNetDatasetsToRemove > nMaxNetDatasetsToRemove:
        msg = (
            f'Net number of datasets to remove ({nNetDatasetsToRemove})'
            f' is larger than the permitted ({nMaxNetDatasetsToRemove}).'
            ' This might indicate a bug or configuration error.'
            ' This assertion is a failsafe to prevent deleting too many'
            ' datasets by mistake.'
        )
        L.error(msg)
        L.error(
            f'First {const.N_EXCESS_DATASETS_PRINT} dataset that would have '
            f' been deleted:',
        )
        for fileName in localExcessDst['file_name'][
                0:const.N_EXCESS_DATASETS_PRINT
        ]:
            L.error(f'    {fileName}')

        raise AssertionError(msg)

    return soarMissingDst, localExcessDst


def _execute_sync_dir_update(
    downloader: erikpgjohansson.solo.soar.dwld.Downloader,
    soarMissingDst, localExcessDst, syncDir, tempDownloadDir,
    tempRemovalDir, removeRemovalDir, downloadLogFormat,
):
    '''Execute a pre-calculated syncing of local directory by downloading
    specified datasets and removing specified local datasets.

    Deliberately combines downloads and removals in one function so that
    errors and interruptions do not unnecessarily lead to leaving the synced
    directory in "corrupted state", i.e. either having
    (1) duplicate dataset versions, or
    (2) file removal without downloaded replacements.

    Downloads are made via a temporary directory and are only transferred
    to their final locations after
    (1) the entire download is complete, and
    (2) all file removals have been completed successfully.
    '''
    '''
    PROPOSAL: Better name.
        ~Sync, ~update, ~refresh, ~execute, ~download, ~remove
        update_sync_dir
        execute_sync
    '''
    assert isinstance(downloader, erikpgjohansson.solo.soar.dwld.Downloader)

    L = logging.getLogger(__name__)

    # =================
    # Download datasets
    # =================
    n_datasets = soarMissingDst['item_id'].size
    L.info(f'Downloading {n_datasets} datasets')
    erikpgjohansson.solo.soar.utils.download_latest_datasets_batch(
        downloader,
        soarMissingDst['item_id'],
        soarMissingDst['file_size'],
        tempDownloadDir,
        logFormat=downloadLogFormat,
        debugDownloadingEnabled=True,
    )

    # =====================
    # Remove local datasets
    # =====================
    # NOTE: Deliberately removing datasets AFTER having successfully downloaded
    # new ones (including potential replacements for removed files).
    # This is to avoid that bugs lead to unnecessarily deleting datasets that
    # are hard/slow to replace.
    n_datasets = localExcessDst.n()
    L.info(f'Removing {n_datasets} local datasets')
    if n_datasets > 0:
        # NOTE: Not calling if zero files to remove. ==> No removal
        #       directory created, no logging (if added).
        pathsToRemoveList = localExcessDst['file_path'].tolist()
        stdoutStr = _remove_files(
            pathsToRemoveList, tempRemovalDir, removeRemovalDir,
        )
        L.info(stdoutStr)

    # =================================================
    # Move downloaded datasets into sync directory tree
    # =================================================
    L.info(
        'Moving downloaded datasets to'
        ' selected directory structure (if there are any).',
    )
    erikpgjohansson.solo.iddt.copy_move_datasets_to_irfu_dir_tree(
        'move', tempDownloadDir, syncDir,
        dirCreationPermissions=const.CREATE_DIR_PERMISSIONS,
    )


def _remove_files(ls_paths_remove, temp_removal_dir, remove_removal_dir):
    assert type(ls_paths_remove) in (list, tuple)
    assert type(remove_removal_dir) == bool
    L = logging.getLogger(__name__)

    if temp_removal_dir is not None:
        L.info(f'Using removal directory "{temp_removal_dir}"')
        # NOTE: Permit pre-existing removal directory.
        # NOTE: os.makedirs() can create directory recursively.
        os.makedirs(
            temp_removal_dir, mode=const.CREATE_DIR_PERMISSIONS, exist_ok=True,
        )
        for path_remove in ls_paths_remove:
            file_name = os.path.basename(path_remove)
            final_path = os.path.join(temp_removal_dir, file_name)
            os.replace(path_remove, final_path)

        # NOTE: Replaces value of "ls_paths_remove".
        ls_paths_remove = [temp_removal_dir]

        if not remove_removal_dir:
            return ''

    stdoutBytes = subprocess.check_output(
        const.FILE_REMOVAL_COMMAND_LIST + ls_paths_remove,
    )
    return str(stdoutBytes, 'utf-8')


def _find_DST_difference(
    fileNameArray1, fileNameArray2,
    fileSizeArray1, fileSizeArray2,
):
    '''
    Find both set differences between lists of strings as boolean index
    arrays. Intended for dataset filenames, to determine which datasets need
    to be synched (removed, downloaded).

    NOTE: Does not check file size or content.
    NOTE: Does not directly use dataset versions.


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
    PROPOSAL: Use DSTs.
    '''
    erikpgjohansson.solo.soar.utils.assert_col_array(
        fileNameArray1, np.dtype('O'),
    )
    erikpgjohansson.solo.soar.utils.assert_col_array(
        fileNameArray2, np.dtype('O'),
    )

    erikpgjohansson.solo.soar.utils.assert_col_array(
        fileSizeArray1, np.dtype('int64'),
    )
    erikpgjohansson.solo.soar.utils.assert_col_array(
        fileSizeArray2, np.dtype('int64'),
    )

    # NOTE: It is suspect that there is not some way of doing this using numpy
    # functionality I have not yet managed to find it.
    # FNS = File Name & Size
    fnsArray1 = np.array(
        list(zip(fileNameArray1, fileSizeArray1)),
        dtype=[
            ('fileName', fileNameArray1.dtype),
            ('fileSize', fileSizeArray1.dtype),
        ],
    )
    fnsArray2 = np.array(
        list(zip(fileNameArray2, fileSizeArray2)),
        dtype=[
            ('fileName', fileNameArray2.dtype),
            ('fileSize', fileSizeArray2.dtype),
        ],
    )

    bDiff12 = ~np.isin(fnsArray1, fnsArray2)
    bDiff21 = ~np.isin(fnsArray2, fnsArray1)

    # ASSERTIONS
    # NOTE: Many re-implementations have failed this assertion.
    assert type(bDiff12) == np.ndarray
    assert type(bDiff21) == np.ndarray

    return bDiff12, bDiff21


@codetiming.Timer('_find_DST_subset', logger=None)
def _find_DST_subset(
    datasetIncludeFunc: typing.Callable,
    dst: erikpgjohansson.solo.soar.dst.DatasetsTable,
):
    '''
    Returns indices to datasets that are permitted by datasetIncludeFunc.
    Basically just iterates over datasetIncludeFunc.


    Parameters
    ----------
    datasetIncludeFunc : Function handle.
        Checks whether a given dataset should be included.


    Returns
    -------
    bSubset : numpy array
    '''
    instrumentArray = dst['instrument']
    levelArray      = dst['processing_level']
    beginTimeArray  = dst['begin_time_FN']

    datasetIdArray = np.array(
        tuple(
            erikpgjohansson.solo.utils.parse_item_ID(itemId)['DATASET_ID']
            for itemId in dst['item_id']
        ),
    )

    bSubset = np.zeros(instrumentArray.shape, dtype=bool)
    for i in range(instrumentArray.size):
        bSubset[i] = datasetIncludeFunc(
            instrument=instrumentArray[i],
            level     =levelArray[i],
            beginTime =beginTimeArray[i],
            datasetId =datasetIdArray[i],
        )

    return bSubset
