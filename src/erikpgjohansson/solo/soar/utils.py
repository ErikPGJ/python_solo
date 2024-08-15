'''
Utilities related to using SOAR, but not for connecting to SOAR directly.
Includes functionality for syncing SOAR data (for now).

Initially created 2020-12-17 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import codetiming
import collections
import concurrent.futures
import datetime
import erikpgjohansson.solo.metadata
import erikpgjohansson.solo.asserts
import erikpgjohansson.solo.soar.dst
import erikpgjohansson.solo.soar.dwld as dwld
import erikpgjohansson.solo.soar.utils as utils
import logging
import numpy as np
import os
import threading


'''
BOGIQ
=====
NOTE: download_latest_dataset() downloads latest dataset
          version, not specified dataset version?
    PROPOSAL: Compare downloaded datasets with online list.
        PROPOSAL: Assertion.
        PROPOSAL: If disagreeing, re-download online list and compare again
                  (once).

PROPOSAL: Function: Difference between DSTs.
    Find differences between datasets in in two DSTs.
    Find all files/datasets (defined by item ID+version+FILE SIZE)
    * only in dst1,
    * only in dst2,
    * in both dst1 and dst2.
    CON: Does not distinguish datasets where item ID is part of difference.
        NOTE: Will not separately catch datasets that are removed because SOAR
        has removed the item ID.
    PROPOSAL: Function for doing this for exactly one column.
        Construct same operation for multiple columns through multiple calls.
        CON: Strange to compare anything without involving item ID.
            Ex: Set operation between sets of file sizes (mixing item IDs
                together).
            Ex: Set operation between sets of versions (mixing item IDs
                together).
        CON: DST is list, not set. May have duplicates (e.g. versions, file
             sizes, item IDs)
    PROPOSAL: Difference between DSTs for selected columns.
        In practice: item ID, item ID+version, item ID+version+file size (?)



PROPOSAL: Split up ~SOAR-related functionality into multiple modules.
    TODO-DEC: Should functionality for managing DSTs be considered ~SOAR?
    TODO-DEC: Should separate SOAR and mirroring code?
        PROPOSAL: so.soar + so.soar_mirror
            PRO: so.soar_mirror USES so.soar, but is not PART of
                 SOAR functionality.
    PROPOSAL: so.filter, so.filterdst
    PROPOSAL: so.soar.download/connect/network/access or so.soar (only)
    PROPOSAL: so.soar.mirror, so.soar_mirror, so.soar_utils
    PROPOSAL: so.soar.misc, so.soar.other, so.soar.utils

PROPOSAL: DST filtering functions should accept arrays (not DST) and return
          logical index array (not DST).
    PRO: Can handle different column names.
PROPOSAL: DST filtering functions should accept DST+relevant column names.
    PRO: Can handle different column names.
'''


def assert_1D_NA(v, dtype=None):
    '''
    PROPOSAL: Use numpy.issubdtype()
      PRO: Useful for categories of types.
          Ex: Variants of datetime64, strings?
          Ex: np.issubdtype(beginTimeArray.dtype, np.dtype('<M8[ms]'))
    '''

    # IMPLEMENTATION NOTE: Some automatic tests have historically mistakenly
    # used 0-dim arrays which causes hard-to-understand errors.
    # ==> Want assertion against this.
    assert type(v) is np.ndarray
    assert v.ndim == 1

    if dtype:
        # ASSERTION: Correct ARGUMENT type: dtype (not the array)
        # assert type(dtype) == np.dtype, \
        assert isinstance(dtype, np.dtype), \
            'Argument dtype has the wrong type.'

        # ASSERTION: Array type
        assert v.dtype == dtype

    # return v.size   # Exclude?


@codetiming.Timer('download_latest_datasets_batch', logger=None)
def download_latest_datasets_batch(
    downloader: dwld.Downloader,
    itemIdArray, fileSizeArray, outputDirPath,
    downloadByIncrFileSize=False,
):
    '''
    Download latest versions of datasets (multiple ones), for selected item
    ID's. Will likely overwrite pre-existing files (not checked). Will log
    progress, speed, predicted remainder & completion to stdout.


    Parameters
    ----------
    fileSizeArray : 1D numpy.ndarray of integers.
        NOTE: Needed for logging the predicted remaining wall time needed for
        the download.
    downloadByIncrFileSize : bool
        True: Sort datasets by increasing file size.
        Useful for testing/debugging. Bad for predicted log values.


    Returns
    -------
    None.
    '''
    '''
    TODO-DEC: How handle pre-existing files? What does
              download_latest_dataset() do?
        NOTE: This function does not know the filenames.
        PROPOSAL: policy which is sent to download_latest_dataset().
    PROPOSAL: Argument for filenames.

    PROPOSAL: Multiple simultaneous downloads.
        PRO: Faster.
        CON: More complicated error handling?

    PROPOSAL: Abolish downloadByIncrFileSize.
    '''

    # ASSERTIONS
    assert isinstance(downloader, dwld.Downloader)
    utils.assert_1D_NA(itemIdArray, np.dtype('O'))
    assert np.unique(itemIdArray).size == itemIdArray.size, \
        'itemIdArray contains duplicates.'
    utils.assert_1D_NA(fileSizeArray, np.dtype('int64'))
    assert itemIdArray.size == fileSizeArray.size
    erikpgjohansson.solo.asserts.is_dir(outputDirPath)

    L = logging.getLogger(__name__)

    if downloadByIncrFileSize:
        iSort         = np.argsort(fileSizeArray)
        itemIdArray   = itemIdArray[iSort]
        fileSizeArray = fileSizeArray[iSort]

    complBytes = 0
    totalBytes = fileSizeArray.sum()
    startDt    = datetime.datetime.now()
    n_datasets = itemIdArray.size

    for i_dataset in range(n_datasets):
        itemId   = itemIdArray[i_dataset]
        fileSize = fileSizeArray[i_dataset]

        fileSizeMb = fileSize / 2**20

        # ================
        # Download dataset
        # ================
        L.info(f'Download starting:  {fileSizeMb:.2f} [MiB], {itemId}')
        downloader.download_latest_dataset(itemId, outputDirPath)
        L.info(f'Download completed: {fileSizeMb:.2f} [MiB], {itemId}')

        # ===
        # Log
        # ===
        # NOTE: Doing statistics AFTER downloading file. Could also be done
        # BEFORE downloading. Note that this uses another timestamp
        # representing "now".
        complBytes += fileSize

        _download_latest_datasets_batch_log_progress(
            n_datasets, i_dataset+1, totalBytes, complBytes, startDt,
        )


@codetiming.Timer('download_latest_datasets_batch2', logger=None)
def download_latest_datasets_batch2(
    downloader: dwld.Downloader,
    itemIdArray, fileSizeArray, outputDirPath,
    downloadByIncrFileSize=False,
):
    '''
    Experimental parallized version of download_latest_datasets_batch().
    '''
    '''
    PROPOSAL: Somehow return results (nbr of exceptions).
    PROPOSAL: Raise exception if any task raised exception, but after all
              tasks have completed.
    PROPOSAL: Raise exception immediately if any task raises exception.
    PROPOSAL: Move code from callback to end of task.
        NOTE: Does not seem to need callbacks unless tasks are inhomogeneous.
    '''
    class CollectiveTaskState:

        LOCK = threading.Lock()

        def __init__(self):
            self._compl_download_attempts = 0
            self._compl_bytes = 0

        def task_callback(self, file_size):
            with CollectiveTaskState.LOCK:
                try:
                    self._compl_download_attempts += 1
                    self._compl_bytes += file_size
                    # L.info(
                    #     'task_callback(): '
                    #     f'file_size={file_size}, '
                    #     f'total_bytes={total_bytes}, '
                    #     f'self._compl_bytes={self._compl_bytes}'
                    # )   # DEBUG
                    _download_latest_datasets_batch_log_progress(
                        n_datasets, self._compl_download_attempts,
                        total_bytes, self._compl_bytes,
                        start_dt,
                    )
                except Exception as e:
                    L.error(e)
                    # NOTE: Exception will be caught by the "concurrent"
                    # library code. Can be detected by the future.
                    raise e

    class Task:
        def __init__(self, item_id, file_size):
            self._item_id = item_id
            self._file_size = file_size

        def run(self):
            try:
                file_size_mb = self._file_size / 2**20
                L.info(
                    f'Download starting:  '
                    f'{file_size_mb:.2f} [MiB], {self._item_id}',
                )
                downloader.download_latest_dataset(
                    self._item_id, outputDirPath,
                )
                L.info(
                    f'Download completed: '
                    f'{file_size_mb:.2f} [MiB], {self._item_id}',
                )
            except Exception as e:
                L.error(e)
                # Exception caught by concurrent library code. Can be detected
                # by future (not implemented).
                raise e

    # ==========
    # ASSERTIONS
    # ==========
    assert isinstance(downloader, dwld.Downloader)
    utils.assert_1D_NA(itemIdArray, np.dtype('O'))
    assert np.unique(itemIdArray).size == itemIdArray.size, \
        'itemIdArray contains duplicates.'
    utils.assert_1D_NA(fileSizeArray, np.dtype('int64'))
    assert itemIdArray.size == fileSizeArray.size
    erikpgjohansson.solo.asserts.is_dir(outputDirPath)

    # =============
    # Miscellaneous
    # =============
    L = logging.getLogger(__name__)
    if downloadByIncrFileSize:
        i_sort        = np.argsort(fileSizeArray)
        itemIdArray   = itemIdArray[i_sort]
        fileSizeArray = fileSizeArray[i_sort]

    # =====================
    # Run tasks / downloads
    # =====================
    ls_future = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        cts = CollectiveTaskState()
        total_bytes = fileSizeArray.sum()
        start_dt = datetime.datetime.now()
        n_datasets = itemIdArray.size

        _download_latest_datasets_batch_log_progress(
            n_datasets, 0, total_bytes, 0, start_dt,
        )

        for i_task in range(n_datasets):
            item_id = itemIdArray[i_task]
            file_size = fileSizeArray[i_task]

            task = Task(item_id, file_size)
            # =============================================================
            # IMPLEMENTATION NOTE: Must use lambda function default values
            # to "bind" the VALUES cts and file_size (not the variables) to
            # the function call.
            # =============================================================
            # Start running task in parallel with other tasks (unless too many)
            future = executor.submit(lambda task2=task: task2.run())
            # Set function to be called after task has finished
            future.add_done_callback(
                lambda future2, cts2=cts, file_size2=file_size:
                cts2.task_callback(file_size2),
            )

            ls_future.append(future)

    # ====================
    # Log summary of tasks
    # ====================
    n_exc = 0
    n_done = 0
    for future in ls_future:
        if isinstance(future.exception(), Exception):
            n_exc += 1
        if future.done():
            # Includes cancelled tasks, but code does not cancel tasks.
            n_done += 1
    L.info('All download tasks completed.')
    L.info(f'#Download tasks that raised exceptions: {n_exc}')
    L.info(f'#Download tasks that completed:         {n_done}')


def _download_latest_datasets_batch_log_progress(
    n_datasets_total, n_datasets_compl, bytes_tot, bytes_compl, dt_begin,
):
    '''
    Log batch download progress.

    Parameters
    ----------
    n_datasets_total
    n_datasets_compl
    bytes_tot
    bytes_compl
    dt_begin
        When downloads began.
    '''
    '''
    PROPOSAL: Return string instead of logging
    PROPOSAL: Call logger once with a multi-row string.
        PRO: Can probably prevent printing log from being interrupted by
             logging from other threads.
            NOTE: (Almost) all calls to this function are made with the same
                  mutex lock: CollectiveTaskState.LOCK
        CON: Logger prefix not designed for this by default.
    '''
    # Variable naming conventions
    # ===========================
    # remain = remaining
    # compl  = completed (so far; not timestamp of completion)
    # tot    = total for all downloads combined
    # sec    = seconds
    # mb     = MiB
    # mbps   = MiB/s

    def TD_to_str(td):
        '''Hack to make timedelta better for printing.'''
        '''Ex: "3 days, 17:53:42.966222" --> "3 days, 17:53:42"
        '''
        if isinstance(td, datetime.timedelta):
            return str(datetime.timedelta(seconds=round(td.total_seconds())))
        elif td is None:
            return 'n/a'
        else:
            raise AssertionError()

    assert n_datasets_total >= n_datasets_compl
    assert bytes_tot >= bytes_compl
    # NOTE: Can not assert that sec_compl == 0, since it should not have.
    assert (bytes_compl == 0) == (n_datasets_compl == 0)
    assert isinstance(dt_begin, datetime.datetime)

    L = logging.getLogger(__name__)

    # n_datasets_compl
    # bytes_compl
    td_compl = datetime.datetime.now() - dt_begin   # Elapsed wall time.
    sec_compl = td_compl.total_seconds()

    # Derive the remainder
    # (Use linear extrapolation for estimating time.)
    n_datasets_remain = n_datasets_total - n_datasets_compl
    bytes_remain = bytes_tot - bytes_compl
    if bytes_compl == 0:
        # CASE: Beginning of downloads
        # sec_remain = None
        sec_remain = float('NaN')
        td_remain = None
        # n_datasets_total
        # bytes_tot
        sec_tot = float('NaN')
        td_tot = None
        # Timestamp of completion (end of downloads)
        dt_end = None   # datetime=dt (not td=deltatime)
    else:
        # CASE: Nominal case
        # Download speed: bytes_remain / sec_remain == bytes_compl / sec_compl
        # <==>
        sec_remain = bytes_remain / (bytes_compl / sec_compl)  # Estimate
        td_remain = datetime.timedelta(seconds=sec_remain)
        # n_datasets_total
        # bytes_tot
        sec_tot = sec_compl + sec_remain
        td_tot = datetime.timedelta(seconds=sec_tot)
        # Timestamp of completion (end of downloads)
        dt_end = dt_begin + td_tot   # dt=datetime (not td=deltatime)

    mb_compl = bytes_compl / 2 ** 20
    mb_remain = bytes_remain / 2 ** 20
    mb_tot = bytes_tot / 2 ** 20

    speed_mbps = (bytes_compl / sec_compl) / 2 ** 20

    s_td_compl = TD_to_str(td_compl)
    s_td_remain = TD_to_str(td_remain)
    s_td_tot = TD_to_str(td_tot)
    if dt_end is None:
        s_dt_end = 'n/a'
    else:
        s_dt_end = dt_end.strftime("%Y-%m-%dT%H.%M.%S")

    # Ex: "3 days, 17:53:42" ==> 16 characters
    STR_DIVIDER = '-' * 73
    ls_s = (
        STR_DIVIDER,
        '    Completed        Remaining      Total (est.)     Unit',
        STR_DIVIDER,
        f'{n_datasets_compl:16} {n_datasets_remain:16} {n_datasets_total:16}'
        f' [datasets]',
        f'{mb_compl:16.2f} {mb_remain:16.2f} {mb_tot:16.2f} [MiB]',
        f'{sec_compl:16.1f} {sec_remain:16.1f} {sec_tot:16.1f} [s]',
        f'{s_td_compl:>16} {s_td_remain:>16} {s_td_tot:>16}'
        f' [(days,) hour:min:sec]',
        STR_DIVIDER,
        f'Effective average download speed so far: {speed_mbps:.2f} [MiB/s]',
        f'Estimated time of completion:            {s_dt_end}',
        STR_DIVIDER,
    )
    for s in ls_s:
        L.info(s)


@codetiming.Timer('find_latest_versions', logger=None)
def find_latest_versions(itemIdArray, itemVerNbrArray):
    '''
    Find boolean indices for datasets which have the latest version (for that
    particular item ID.

    NOTE: Works, but appears to be slower than necessary.
    2022-01-04: 548 s when applied to entire SDT.


    Parameters
    ----------
    itemIdArray     : 1D numpy array of strings.
    itemVerNbrArray : 1D numpy array of integers.


    Returns
    -------
    bLvArray : 1D numpy bool array.
    '''
    '''
    PROPOSAL: Temporary variable for "itemIdArray == uii".
    '''
    # LV = Latest Version
    # UII = Unique Item ID

    # ASSERTIONS
    # ----------
    # IMPLEMENTATION NOTE: Automatic tests have historically mistakenly used
    # 0-dim arrays which causes hard-to-understand errors.
    # ==> Want to assert for this.
    # import pdb; pdb.set_trace()
    utils.assert_1D_NA(itemIdArray,     np.dtype('O'))
    utils.assert_1D_NA(itemVerNbrArray, np.dtype('int64'))
    assert itemIdArray.shape == itemVerNbrArray.shape

    # NOTE: itemIdArray[iUniques] == uniqItemIdArray
    uniqItemIdArray, iUniques, jInverse, uniqueCounts = np.unique(
        itemIdArray, return_index=1, return_inverse=1, return_counts=1,
    )

    # Datasets that should ultimately be kept.
    bLvArray = np.full(itemIdArray.size, False)   # Create same-sized array.

    # Iterate over unique item IDs.
    for iUii in range(iUniques.size):
        uii  = uniqItemIdArray[iUii]
        nDuplicates = uniqueCounts[iUii]
        if nDuplicates == 1:
            # CASE: There is only one version.
            bLvArray[iUniques[iUii]] = True
        else:
            # CASE: There are multiple versions.
            uiiLv = itemVerNbrArray[itemIdArray == uii].max()

            b = (itemIdArray == uii) & (itemVerNbrArray == uiiLv)
            (i,) = np.nonzero(b)   # NOTE: Returns tuple or arrays.

            assert i.size == 1, (
                'Found multiple datasets with itemId = {}'
                ' with the same highest version number V{:d}.'
            ).format(
                uii, uiiLv,
            )

            bLvArray[i] = True

    # ASSERTION: All item ID's are unique.
    itemIdArray2 = itemIdArray[bLvArray]
    assert np.unique(itemIdArray2).size == itemIdArray2.size

    return bLvArray


@codetiming.Timer('derive_DST_from_dir', logger=None)
def derive_DST_from_dir(rootDir):
    '''
    Derive a DST from a directory tree datasets. Searches directory
    recursively.

    NOTE: Ignores filenames that can not be parsed as datasets.


    Parameters
    ----------
    rootDir : String

    Returns
    -------
    dst
    '''
    erikpgjohansson.solo.asserts.is_dir(rootDir)

    fileNameList    = []
    filePathList    = []
    fileVerList     = []
    itemIdList      = []
    fileSizeList    = []
    instrumentList  = []
    levelList       = []
    beginTimeFnList = []    # FN = File Name. Time derived from filename.
    for (dirPath, subDirNames, fileNames) in os.walk(rootDir):
        for fileName in fileNames:

            dsfn = \
                erikpgjohansson.solo.metadata.DatasetFilename.parse_filename(
                    fileName,
                )
            # IMPLEMENTATION NOTE:
            # erikpgjohansson.solo.metadata.DatasetFilename.parse_filename()
            # returns None for non-parsable filenames.
            if dsfn:
                _dataSrc, level, instrument, _descriptor = \
                    erikpgjohansson.solo.metadata.parse_DSID(
                        dsfn.dsid,
                    )

                filePath = os.path.join(dirPath, fileName)
                filePathList   += [filePath]
                fileNameList   += [fileName]
                fileVerList    += [int(dsfn.versionStr)]
                itemIdList     += [dsfn.itemId]
                fileSizeList   += [os.stat(filePath).st_size]
                instrumentList += [instrument]
                levelList      += [level]

                # NOTE: datetime.datetime requires integer seconds+microseconds
                # in separate arguments (as integers). Filenames should only
                # contain time with microseconds=0 so we ignore them.
                tv1    = list(dsfn.timeVector1)
                tv1[5] = int(tv1[5])
                beginTimeFnList += [datetime.datetime(*tv1)]

    dst = erikpgjohansson.solo.soar.dst.DatasetsTable({
        'file_name':        np.array(fileNameList,    dtype=object),
        'file_path':        np.array(filePathList,    dtype=object),
        'item_version':     np.array(fileVerList,     dtype='int64'),
        'item_id':          np.array(itemIdList,      dtype=object),
        'file_size':        np.array(fileSizeList,    dtype='int64'),
        'begin_time_FN':    np.array(beginTimeFnList, dtype='datetime64[ms]'),
        'instrument':       np.array(instrumentList,  dtype=object),
        'processing_level': np.array(levelList,       dtype=object),
    })
    # NOTE: Key name "processing_level" chosen to be in agreement with
    # erikpgjohansson.solo.soar.dwld.Downloader.download_SDT_DST().
    return dst


# def filter_DST(
#     dst, levelsSet=None, instrumentsSet=None,
#     intervalTimeStrs=None,
# ):
#     '''
# General-purpose customizable DST filter. In particular for manually selecting
# which subset of datasets that should be downloaded or synced. Datasets in DST
# that match all arguments will be kept. All other datasets will be removed.
#
# Parameters
# ----------
# dst :
# levelsSet : Set of strings
# instrumentsSet : Set of instruments
# intervalTimeStrs : Two UTC strings, start & stop.
#     Approximate time interval to keep. Applies to column begin_time.
#
# Returns
# -------
# dict
#     '''
#     assert type(dst) == erikpgjohansson.solo.soar.dst.DatasetsTable
#
#     b = np.full(dst.n(), True)
#
#     if levelsSet:
#         b = b & np.isin(dst['processing_level'], np.array(list(levelsSet)))
#     if instrumentsSet:
#         b = b & np.isin(dst['instrument'], np.array(list(instrumentsSet)))
#
#     if intervalTimeStrs:
#         assert len(intervalTimeStrs) == 2
#         DtMin = np.datetime64(intervalTimeStrs[0])
#         DtMax = np.datetime64(intervalTimeStrs[1])
#         bta = dst['begin_time']   # BTA = Begin Time Array
#         b = b & (DtMin <= bta) & (bta <= DtMax)
#
#     return dst.index(b)


def log_DST(dst: erikpgjohansson.solo.soar.dst.DatasetsTable, title: str):
    '''
    Log the content of a table of datasets. Assumes that it contains certain
    fields.
    '''
    '''
    PROPOSAL: Log amount of data per combination of level and instrument.
    PROPOSAL: Log amount of data per DSID.
    PROPOSAL: Log number of datasets per DSID.
    '''
    assert isinstance(dst, erikpgjohansson.solo.soar.dst.DatasetsTable)

    SEPARATOR_LENGTH = 80

    def sep():
        L.info('=' * SEPARATOR_LENGTH)

    def ssl(set_ls):
        '''Convert set or list/tuple to string for logging.'''
        # IMPLEMENTATION NOTE: Should not sort since that might not always
        # be desirable (e.g. logging combinations of level and instrument).
        # Caller should sort the argument first if needed.
        if set_ls:
            return ', '.join(set_ls)
        else:
            return '(none)'

    L = logging.getLogger(__name__)

    bytes_tot = dst['file_size'].sum()
    n_datasets = dst['file_size'].size
    gb_tot = bytes_tot / 2**30

    set_instr = set(dst['instrument'])
    set_level = set(dst['processing_level'])
    cnt_level_instr = collections.Counter(
        zip(dst['processing_level'], dst['instrument']),
    )

    # BTF = begin_time_FN
    na_btf = dst['begin_time_FN']
    assert na_btf.dtype == np.dtype('datetime64[ms]')
    na_btf_nonnull = na_btf[~np.isnat(na_btf)]
    n_btf_null = na_btf[np.isnat(na_btf)].size

    sep()
    L.info(title)
    L.info('-'*len(title))
    L.info(f'Number of datasets:       {n_datasets:d} datasets')
    L.info(f'Total amount of data:     {gb_tot:.2f} [GiB]')
    L.info(f'Unique instruments:       {ssl(sorted(set_instr))}')
    L.info(f'Unique processing levels: {ssl(sorted(set_level))}')
    L.info('begin_time_FN:')
    L.info(f'    #NaT: {n_btf_null} datasets')
    if na_btf_nonnull.size > 0:
        # NOTE: na_btf_nonnull.min() crashes if empty array.
        L.info(f'    Min: {na_btf_nonnull.min()}')
        L.info(f'    Max: {na_btf_nonnull.max()}')
    else:
        L.info('    (No datasets without NaT)')
    L.info('Unique combinations of instrument and processing level:')
    for (level, instr) in sorted(cnt_level_instr):
        n = cnt_level_instr[(level, instr)]
        L.info(f'    {level:4} {instr:3}: {n:5} datasets')
    sep()


def log_codetiming():
    '''Quick-and-dirty function for logging codetiming results.
    '''
    TITLE = 'Time used for various labelled parts of code (codetiming)'

    L = logging.getLogger(__name__)
    L.info('')
    L.info(TITLE)
    L.info('-' * len(TITLE))
    for key, value in codetiming.Timer.timers.items():
        L.info(f'{key:40s} {value:10.2f} [s]')
    L.info('')
