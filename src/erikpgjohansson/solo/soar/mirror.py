'''
Utilities for synching SOAR datasets with a local directory.

Initially created 2020-12-21 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import codetiming
import erikpgjohansson.solo.asserts
import erikpgjohansson.solo.iddt
import erikpgjohansson.solo.metadata
import erikpgjohansson.solo.soar.const as const
import erikpgjohansson.solo.soar.dst
import erikpgjohansson.solo.soar.dwld as dwld
import erikpgjohansson.solo.soar.utils as utils
import logging
import numpy as np
import os
import subprocess
import typing
import sys


'''
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

PROPOSAL: erikpgjohansson.solo.soar.dst.log_DST() returns string that can be
          indented by caller.

PROPOSAL: Better naming for specifid dataset subsets.
    Reference DST : Appears to be subset+LV of SOAR datasets.
    SOAR missing  : Reference datasets not found locally.
        PROPOSAL: Rename ~local_missing.
PROPOSAL: Abbreviations/names for specific dataset subsets.
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

PROPOSAL: Always use removal directory.
PROPOSAL: Download exact version of dataset, not the latest version.
    PRO: More robust in case SOAR updates the datasets (increments version)
         between
         (1) sync code calculates datasets to download, and
         (2) the actual download of that particular dataset.
         If not, then mirror code might try to move a file that has another
         name, or dwld.download_latest_dataset() may fail assertion on
         filename. (Implementation-dependent.)
        CON: Current implementation (2022-10-20) uses
             erikpgjohansson.solo.iddt.copy_move_datasets_to_IRFU_dir_tree()
             and is thus not dependent on exact filenames.

PROPOSAL: Make code robust w.r.t. network error, file not present at SOAR.

PROPOSAL: Use configuration file (JSON) to set system/setup-dependent values.
          Abolish wrappers irfu_mirror (probably), mtest_mirror.
    NOTE: Can not completely abolish irfu_mirror_shell. (Can abolish
          mtest_mirror_shell though).
    PRO: No hardcoded values in git repo.
    CON: Can not specify generic datasetsSubsetFunc in
         configuration file! "Must" always have code for datasets_subset_func
         for the official IRFU SOAR mirror!
        CON-PROPOSAL: Invent syntax/storable data structure which covers the
            official mirror's datasets_subset_func.
            CON: Less general.
            PRO: Might be ~easy.
            PROPOSAL: List of entries, where each entry contains fields for
                instrument, level, etc. Field=null <=> wildcard (accept any
                value).
            PROPOSAL: List of explicit DSIDs.

PROPOSAL: Replace datasetsSubsetFunc with class.
    CON: Only one function replaced by one method.
    PRO: Can potentially configure (set values) in constructor.
    PRO: Better documentation of method/function signature.
    PRO: Can verify that class has expected superclass.
    PRO: Can implement class which takes behaviour from configuration file.
    PROPOSAL: Use class for returning other configuration values.
'''


@codetiming.Timer('sync', logger=None)
def sync(
    syncDir, tempDownloadDir, datasetsSubsetFunc: typing.Callable,
    deleteOutsideSubset=False,
    nMaxNetDatasetsToRemove=10,
    tempRemovalDir=None,
    removeRemovalDir=False,
    sodl: dwld.SoarDownloader = dwld.SoarDownloaderImpl(),
):
    '''
    Sync local directory with subset of online SOAR datasets.

    NOTE/BUG: Does not correct the locations of misplaced datasets.


    Parameters
    ----------
    syncDir : String. Path.
    tempDownloadDir : String. Path.
        Must be empty on datasets. Otherwise those will be moved too.
    datasetsSubsetFunc : Function (instrument=str, level=str, ...) --> bool
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
    tempRemovalDir
        None or path to "removal directory" to which datasets are moved before
        the directory itself is itself optionally removed (depends on argument
        "removeRemovalDir").
        Removal directory may preexist. Is created if not.
    removeRemovalDir
        Bool. If using a removal directory, then whether to actually remove the
        removal directory or keep it.
    sodl
        erikpgjohansson.solo.soar.dwld.SoarDownloader object. The default value
        should be used except for automated tests.


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
        erikpgjohansson.solo.metadata.DatasetFilename.parse_filename() among
        the SOAR datasets.
            NOTE: Should only happen if datasetsSubsetFunc() uses time.

    BUG: Gets frequent error messages when calling from bash/python wrapper
         script so_irfu_soar_sync.py on brain. Works better on spis(?).
         /2021-01-19
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

    PROPOSAL: Use offline_cleanup()
        NOTE: Would need to convert _execute_sync_dir_SOAR_update() into
              only downloading datasets.
        PRO: Less overlaping code.
        PRO: Safer.
            PRO: Can (more explicitly) handle that the downloaded
                 dataset version is later than the dataset version in the SDT
                  (if SOAR updated datasets while syncing).
            PRO: Can (more explicitly) handle that some downloads may have
                 failed.
        PRO: Can potentially loop over (1) downloading a subset of
             datasets, and (2) offline_cleanup().
             PRO: Will only remove replaced datasets when replacement has
                  already been downloaded.
        CON: Slower since has to scan local files twice.

    PROBLEM: How handle exceptions?
        TODO-DEC: How handle exceptions in log?
        PROPOSAL: Raise exception
            PROPOSAL: Print exception in log and propagate exception still.
                      -- IMPLEMENTED
        PROPOSAL: Catch exceptions
    '''
    L = logging.getLogger(__name__)

    try:
        # ==========
        # ASSERTIONS
        # ==========
        assert isinstance(sodl, dwld.SoarDownloader)
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
        # NOTE: Explicitly includes ALL versions, i.e. also NON-LATEST
        # versions. There should theoretically only be one version of each
        # dataset locally, but if there are more, then they should be
        # included so that they can be removed (or kept in the rare but
        # possible case of SOAR down-versioning datasets).
        L.info('Producing table of pre-existing local datasets.')
        dst_local = erikpgjohansson.solo.soar.dst.derive_DST_from_dir(syncDir)
        erikpgjohansson.solo.soar.dst.log_DST(
            dst_local, 'Pre-existing local datasets that should be synced',
        )

        # ============
        # Download SDT
        # ============
        L.info('Downloading SDT (SOAR Datasets Table).')
        dst_sdt = dwld.download_SDT_DST(sodl)
        erikpgjohansson.solo.soar.dst.log_DST(
            dst_sdt,
            'SDT (SOAR Datasets Table):'
            ' Synced and non-synced, all dataset versions,'
            ' not necessarily all types of datasets.',
        )

        # ASSERTION: SDT is not empty
        # ---------------------------
        # IMPLEMENTATION NOTE: SOAR might one day return a DST with zero
        # datasets by mistake or due to bug. This could in turn lead to
        # deleting all local datasets.
        assert dst_sdt.n_rows > 0, (
            'SOAR returned an empty SDT (SOAR Datasets Table),'
            ' making it seem as if SOAR has no datasets.'
            ' This should imply that there is something wrong with either'
            ' (1) SOAR, or (2) this software.'
        )

        dst_ref = _calculate_reference_DST(dst_sdt, datasetsSubsetFunc)
        erikpgjohansson.solo.soar.dst.log_DST(
            dst_ref,
            'Reference datasets that should be synced with local datasets',
        )

        dst_soar_missing, dst_local_excess = _calculate_sync_dir_update(
            dst_ref=dst_ref,
            dst_local=dst_local,
            datasetsSubsetFunc=datasetsSubsetFunc,
            b_delete_outside_subset=deleteOutsideSubset,
            nMaxNetDatasetsToRemove=nMaxNetDatasetsToRemove,
        )

        _execute_sync_dir_SOAR_update(
            sodl=sodl,
            dst_soar_missing=dst_soar_missing,
            dst_local_excess=dst_local_excess,
            syncDir=syncDir,
            tempDownloadDir=tempDownloadDir,
            tempRemovalDir=tempRemovalDir,
            removeRemovalDir=removeRemovalDir,
        )

        utils.log_codetiming()   # DEBUG

    except Exception as e:
        L.exception(e)
        raise e


def offline_cleanup(
    syncDir, tempDownloadDir, datasetsSubsetFunc,
    b_delete_outside_subset=False,
    tempRemovalDir=None,
    removeRemovalDir=False,
):
    '''
    Given a temporary download directory and a local sync directory, both of
    them containing datasets,
    (1) move all datasets into the local sync directory,
    (2) make sure that all datasets are in the correct location under the
        sync directory,
    (3) only keep the latest versions.

    Should be useful for
    (1) cleaning up a non-nominal state (e.g. after a crash,
        after having killed the process, or after a bug), and
    (2) inserting manually downloaded datasets.
    '''

    L = logging.getLogger(__name__)

    L.info(
        'Ensuring that datasets under sync directory are in'
        ' the correct locations.',
    )
    erikpgjohansson.solo.iddt.copy_move_datasets_to_IRFU_dir_tree(
        'move', syncDir, syncDir,
        dirCreationPermissions=const.CREATE_DIR_PERMISSIONS,
    )

    L.info('Moving datasets from download directory to sync directory.')
    erikpgjohansson.solo.iddt.copy_move_datasets_to_IRFU_dir_tree(
        'move', tempDownloadDir, syncDir,
        dirCreationPermissions=const.CREATE_DIR_PERMISSIONS,
    )

    L.info('Producing table of pre-existing local datasets.')
    dst_local = erikpgjohansson.solo.soar.dst.derive_DST_from_dir(syncDir)

    dst_ref = _calculate_reference_DST(dst_local, datasetsSubsetFunc)
    dst_ref_missing, dst_local_excess = _calculate_sync_dir_update(
        dst_ref=dst_ref,
        dst_local=dst_local,
        datasetsSubsetFunc=datasetsSubsetFunc,
        b_delete_outside_subset=b_delete_outside_subset,
        nMaxNetDatasetsToRemove=float("Inf"),
    )
    assert dst_ref_missing.n_rows == 0

    # =====================
    # Remove local datasets
    # =====================
    # NOTE: Deliberately removing datasets AFTER having successfully downloaded
    # new ones (including potential replacements for removed files).
    # This is to avoid that bugs lead to unnecessarily deleting datasets that
    # are hard/slow to replace.
    n_datasets = dst_local_excess.n_rows
    L.info(f'Removing {n_datasets} local datasets')
    ls_files_remove = dst_local_excess['file_path'].tolist()
    stdout_str = _remove_files(
        ls_files_remove, tempRemovalDir, removeRemovalDir,
    )
    L.info(stdout_str)


@codetiming.Timer('_calculate_reference_DST', logger=None)
def _calculate_reference_DST(dst, datasetsSubsetFunc: typing.Callable):
    ''''''
    '''
    PROPOSAL: Return logical indices, not DST.
    '''
    # =====================================================================
    # Select subset of SOAR datasets (item IDs) to be synced (all versions)
    # =====================================================================
    na_b_subset = _find_DST_subset(datasetsSubsetFunc, dst)
    dst_subset = dst.index(na_b_subset)

    # =========================================================================
    # Only keep latest version of each dataset in table
    # -------------------------------------------------
    # IMPLEMENTATION NOTE: This can be slow. Therefore doing this first AFTER
    # selecting subset of item IDs. Particularly useful for small test subsets.
    # =========================================================================
    na_b_latest_version = utils.find_latest_versions(
        dst_subset['item_id'],
        dst_subset['item_version'],
    )
    dst_subset_latest_version = dst_subset.index(na_b_latest_version)
    return dst_subset_latest_version


@codetiming.Timer('_calculate_sync_dir_update', logger=None)
def _calculate_sync_dir_update(
    dst_ref, dst_local, datasetsSubsetFunc: typing.Callable,
    b_delete_outside_subset: bool,
    nMaxNetDatasetsToRemove: typing.Union[int, float],
):
    '''Given reference datasets and local datasets, calculate which files
    should be removed or downloaded.

    (1) Assert reference subset dataset list (latest versions) is not empty.
    (2) Optionally trims local datasets list to subset.
    (3) Assert that not too many local datasets should be deleted.

    Parameters
    ----------
    dst_ref
        Reference datasets. The set of datasets for which missing and excess
        datasets should be obtained.
    '''
    '''
    NOTE: Not a very "pure" function since it logs.
    PROPOSAL: Make purer logic function, without logging.
    '''
    assert isinstance(dst_ref, erikpgjohansson.solo.soar.dst.DatasetsTable)
    assert isinstance(dst_local, erikpgjohansson.solo.soar.dst.DatasetsTable)
    assert type(b_delete_outside_subset) is bool
    assert (type(nMaxNetDatasetsToRemove) is int) \
           or (nMaxNetDatasetsToRemove == float('inf'))

    # ASSERT: The list of reference datasets is non-empty.
    # IMPLEMENTATION NOTE: This is to prevent mistakenly deleting all local
    # files due to e.g. a faulty datasetsSubsetFunc.
    assert dst_ref.n_rows > 0, (
        'Trying to sync with empty subset of reference datasets.'
        ' Argument "datasetsSubsetFunc" could be faulty.'
    )

    L = logging.getLogger(__name__)

    L.info('Identifying (1) missing datasets, and (2) datasets to remove.')

    # Whether to hide local datasets outside of subset
    # = whether to prevent local datasets outside of subset from being
    #   potentially deleted later.
    if b_delete_outside_subset:
        L.info(
            'NOTE: Syncing against all local datasets'
            ' (in specified directory).'
            ' Will potentially DELETE datasets outside the specified subset.',
        )
    else:
        # "Hide"/ignore local datasets which are not recognized by
        # "datasetsSubsetFunc".
        na_b_local_subset = _find_DST_subset(datasetsSubsetFunc, dst_local)
        dst_local = dst_local.index(na_b_local_subset)
        L.info(
            'NOTE: Only syncing against subset of local datasets.'
            ' Will NOT delete datasets outside the specified subset.',
        )

    # ==============================================================#
    # Find (1) datasets to download, and (2) local datasets to delete
    # ==============================================================#
    na_b_soar_missing, na_b_local_excess = _find_file_name_size_difference(
        dst_ref['file_name'], dst_local['file_name'],
        dst_ref['file_size'], dst_local['file_size'],
    )

    dst_soar_missing = dst_ref.index(na_b_soar_missing)
    dst_local_excess = dst_local.index(na_b_local_excess)

    erikpgjohansson.solo.soar.dst.log_DST(
        dst_soar_missing, 'Online SOAR datasets that need to be downloaded',
    )
    erikpgjohansson.solo.soar.dst.log_DST(
        dst_local_excess, 'Local datasets that need to be removed',
    )

    # ASSERTION
    # NOTE: Deliberately doing this first after logging which datasets to
    # download and delete.
    n_files_net_remove = dst_local_excess.n_rows - dst_soar_missing.n_rows
    if n_files_net_remove > nMaxNetDatasetsToRemove:
        msg = (
            f'Net number of datasets to remove ({n_files_net_remove})'
            f' is larger than the permitted ({nMaxNetDatasetsToRemove}).'
            ' This might indicate a bug or configuration error.'
            ' This assertion is a failsafe to prevent deleting too many'
            ' datasets by mistake.'
        )
        L.error(msg)
        L.error(
            f'First {const.N_EXCESS_DATASETS_PRINT} dataset that would have'
            f' been deleted:',
        )
        for file_name in dst_local_excess['file_name'][
                0:const.N_EXCESS_DATASETS_PRINT
        ]:
            L.error(f'    {file_name}')

        raise AssertionError(msg)

    return dst_soar_missing, dst_local_excess


def _execute_sync_dir_SOAR_update(
    sodl: dwld.SoarDownloader,
    dst_soar_missing, dst_local_excess, syncDir, tempDownloadDir,
    tempRemovalDir, removeRemovalDir,
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
    assert isinstance(sodl, dwld.SoarDownloader)

    L = logging.getLogger(__name__)

    # =================
    # Download datasets
    # =================
    n_datasets = dst_soar_missing['item_id'].size
    L.info(f'Downloading {n_datasets} datasets')
    if const.USE_PARALLEL_DOWNLOADS:
        download_latest_datasets_batch = \
            utils.download_latest_datasets_batch_parallel
    else:
        download_latest_datasets_batch = \
            utils.download_latest_datasets_batch_nonparallel

    download_latest_datasets_batch(
        sodl,
        dst_soar_missing['item_id'],
        dst_soar_missing['file_size'],
        tempDownloadDir,
    )

    # =====================
    # Remove local datasets
    # =====================
    # NOTE: Deliberately removing datasets AFTER having successfully downloaded
    # new ones (including potential replacements for removed files).
    # This is to avoid that bugs lead to unnecessarily deleting datasets that
    # are hard/slow to replace.
    n_datasets = dst_local_excess.n_rows
    L.info(f'Removing {n_datasets} local datasets')
    ls_files_remove = dst_local_excess['file_path'].tolist()
    stdout_str = _remove_files(
        ls_files_remove, tempRemovalDir, removeRemovalDir,
    )
    L.info(stdout_str)

    # =================================================
    # Move downloaded datasets into sync directory tree
    # =================================================
    L.info(
        'Moving downloaded datasets to'
        ' selected directory structure (if there are any).',
    )
    erikpgjohansson.solo.iddt.copy_move_datasets_to_IRFU_dir_tree(
        'move', tempDownloadDir, syncDir,
        dirCreationPermissions=const.CREATE_DIR_PERMISSIONS,
    )


def _remove_files(ls_paths_remove, temp_removal_dir, remove_removal_dir):
    assert type(ls_paths_remove) in (list, tuple)
    assert type(remove_removal_dir) is bool
    L = logging.getLogger(__name__)

    for path_remove in ls_paths_remove:
        # NOTE: Could be excessive logging/printing if
        # const.FILE_REMOVAL_COMMAND_LIST also logs/prints.
        L.info(f'Dataset selected for removal: {path_remove}')

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


def _find_file_name_size_difference(
    na_file_name1: np.ndarray, na_file_name2: np.ndarray,
    na_file_size1: np.ndarray, na_file_size2: np.ndarray,
):
    '''
    Find both set differences between lists of strings as boolean index
    arrays. Intended for dataset filenames, to determine which datasets need
    to be synched (removed, downloaded).

    NOTE: Does not check file size or content.
    NOTE: Does not directly use dataset versions.


    Parameters
    ----------
    na_file_name1 : 1D numpy.ndarray of strings.
    na_file_name2 : 1D numpy.ndarray of strings.
    na_file_size1 : 1D numpy.ndarray of int64.
    na_file_size2 : 1D numpy.ndarray of int64.
    --
    NOTE: Parameter arrays (separately) do not need to contain unique strings,
    though that is the intended use.


    Returns
    -------
    (na_b_diff1, na_b_diff2)
    '''
    '''
    PROPOSAL: Move to utils.
        CON: Not generic enough.
    PROPOSAL: Test code.
    '''
    # ==========
    # ASSERTIONS
    # ==========
    utils.assert_1D_NA(na_file_name1, np.dtype('O'))
    utils.assert_1D_NA(na_file_name2, np.dtype('O'))

    utils.assert_1D_NA(na_file_size1, np.dtype('int64'))
    utils.assert_1D_NA(na_file_size2, np.dtype('int64'))

    # =========
    # ALGORITHM
    # =========
    # NOTE: It is suspect that there is not some way of doing this using
    # numpy functionality, but I have not yet found any numpy functionality
    # for doing this in a better way.

    na_file_name_size1 = np.array(
        list(zip(na_file_name1, na_file_size1)),
        dtype=[
            ('file_name', na_file_name1.dtype),
            ('file_size', na_file_size1.dtype),
        ],
    )
    na_file_name_size2 = np.array(
        list(zip(na_file_name2, na_file_size2)),
        dtype=[
            ('file_name', na_file_name2.dtype),
            ('file_size', na_file_size2.dtype),
        ],
    )

    na_b_diff12 = ~np.isin(na_file_name_size1, na_file_name_size2)
    na_b_diff21 = ~np.isin(na_file_name_size2, na_file_name_size1)

    # ASSERTIONS
    # NOTE: Many re-implementations have failed this assertion.
    assert type(na_b_diff12) is np.ndarray
    assert type(na_b_diff21) is np.ndarray

    return na_b_diff12, na_b_diff21


@codetiming.Timer('_find_DST_subset', logger=None)
def _find_DST_subset(
    datasetsSubsetFunc: typing.Callable,
    dst: erikpgjohansson.solo.soar.dst.DatasetsTable,
):
    '''
    Returns indices to datasets that are permitted by datasetsSubsetFunc.
    Basically just iterates over datasetsSubsetFunc.


    Parameters
    ----------
    datasetsSubsetFunc : Function handle.
        Checks whether a given dataset should be included.


    Returns
    -------
    na_b_subset : numpy array
    '''
    '''
    PROPOSAL: Move to erikpgjohansson.solo.soar.dst.
        CON: Does not work for generic DST since assumes the existence of
             specifiec fields.
            CON: erikpgjohansson.solo.soar.dst already contains code for
                 non-generic DSTs.
        CON: Only used in this module.
    '''
    na_instrument = dst['instrument']
    na_level      = dst['processing_level']
    na_dt64_begin = dst['begin_time_FN']

    na_dsid = np.array(
        tuple(
            erikpgjohansson.solo.metadata.parse_item_ID(item_id)['DSID']
            for item_id in dst['item_id']
        ),
    )

    na_b_subset = np.zeros(na_instrument.shape, dtype=bool)
    for i in range(na_instrument.size):
        na_b_subset[i] = datasetsSubsetFunc(
            instrument=na_instrument[i],
            level     =na_level[i],
            begin_dt64=na_dt64_begin[i],
            dsid      =na_dsid[i],
        )

    return na_b_subset
