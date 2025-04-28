'''
Utilities related to using SOAR, but not for connecting to SOAR directly.
Includes functionality for syncing SOAR data (for now).

Initially created 2020-12-17 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import codetiming
import collections
import concurrent.futures
import datetime
import erikpgjohansson.solo.asserts
import erikpgjohansson.solo.metadata
import erikpgjohansson.solo.soar.dst
import erikpgjohansson.solo.soar.dwld as dwld
import logging
import numpy as np
import os
import threading


'''
PROPOSAL: Split up ~SOAR-related functionality into multiple modules.
    TODO-DEC: Should functionality for managing DSTs be considered ~SOAR?
    TODO-DEC: Should separate SOAR and mirroring code?
        PROPOSAL: so.soar + so.soar_mirror
            PRO: so.soar_mirror USES so.soar, but is not PART of
                 SOAR functionality.
    PROPOSAL: so.filter, so.filterdst
    PROPOSAL: so.soar.download/connect/network/access or so.soar (only)
    PROPOSAL: so.soar.mirror
    PROPOSAL: so.soar.misc, so.soar.other, so.soar.utils
    PROPOSAL: Move dependence on erikpgjohansson.solo.soar.dst to
              erikpgjohansson.solo.soar.dst.
        PRO:: erikpgjohansson.solo.soar.dst is used by this module but can not
              use erikpgjohansson.solo.soar.utils.assert_1D_NA() due to
              circular imports.

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


@codetiming.Timer('download_latest_datasets_batch_nonparallel', logger=None)
def download_latest_datasets_batch_nonparallel(
    sodl: dwld.SoarDownloader,
    na_item_id: np.ndarray, na_file_size: np.ndarray, outputDirPath,
    downloadByIncrFileSize=False,
):
    '''
    Download the latest version of datasets (multiple ones), for selected item
    ID's. Will likely overwrite pre-existing files (not checked). Will log
    progress, speed, predicted remainder & completion to stdout.


    Parameters
    ----------
    na_file_size : 1D numpy.ndarray of integers.
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

    PROPOSAL: Delete function.
        PRO: download_latest_datasets_batch_parallel() is well tested in
             production.
        PRO: There is test code.
    PROPOSAL: Abolish downloadByIncrFileSize.
    '''

    # ASSERTIONS
    assert isinstance(sodl, dwld.SoarDownloader)
    assert_1D_NA(na_item_id, np.dtype('O'))
    assert np.unique(na_item_id).size == na_item_id.size, \
        'na_item_id contains duplicates.'
    assert_1D_NA(na_file_size, np.dtype('int64'))
    assert na_item_id.size == na_file_size.size
    erikpgjohansson.solo.asserts.is_dir(outputDirPath)
    assert type(downloadByIncrFileSize) is bool

    L = logging.getLogger(__name__)

    if downloadByIncrFileSize:
        iSort        = np.argsort(na_file_size)
        na_item_id   = na_item_id[iSort]
        na_file_size = na_file_size[iSort]

    complBytes = 0
    totalBytes = na_file_size.sum()
    startDt    = datetime.datetime.now()
    n_datasets = na_item_id.size

    for i_dataset in range(n_datasets):
        item_id  = na_item_id[i_dataset]
        fileSize = na_file_size[i_dataset]

        fileSizeMb = fileSize / 2**20

        # ================
        # Download dataset
        # ================
        L.info(f'Download starting:  {fileSizeMb:.2f} [MiB], {item_id}')
        sodl.download_latest_dataset(item_id, outputDirPath)
        L.info(f'Download completed: {fileSizeMb:.2f} [MiB], {item_id}')

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


@codetiming.Timer('download_latest_datasets_batch_parallel', logger=None)
def download_latest_datasets_batch_parallel(
    sodl: dwld.SoarDownloader,
    na_item_id: np.ndarray, na_file_size, outputDirPath,
    downloadByIncrFileSize=False,
):
    '''
    Parallelized version of download_latest_datasets_batch_nonparallel().
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

    class DownloadDatasetTask:
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
                sodl.download_latest_dataset(
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
    assert isinstance(sodl, dwld.SoarDownloader)
    assert_1D_NA(na_item_id, np.dtype('O'))
    assert np.unique(na_item_id).size == na_item_id.size, \
        'na_item_id contains duplicates.'
    assert_1D_NA(na_file_size, np.dtype('int64'))
    assert na_item_id.size == na_file_size.size
    erikpgjohansson.solo.asserts.is_dir(outputDirPath)
    assert type(downloadByIncrFileSize) is bool

    # =============
    # Miscellaneous
    # =============
    L = logging.getLogger(__name__)
    if downloadByIncrFileSize:
        i_sort       = np.argsort(na_file_size)
        na_item_id   = na_item_id[i_sort]
        na_file_size = na_file_size[i_sort]

    # =====================
    # Run tasks / downloads
    # =====================
    ls_future = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        cts         = CollectiveTaskState()
        total_bytes = na_file_size.sum()
        start_dt    = datetime.datetime.now()
        n_datasets  = na_item_id.size

        _download_latest_datasets_batch_log_progress(
            n_datasets, 0, total_bytes, 0, start_dt,
        )

        for i_task in range(n_datasets):
            item_id   = na_item_id[i_task]
            file_size = na_file_size[i_task]

            task = DownloadDatasetTask(item_id, file_size)
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

    def _TD_to_str(td):
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

    s_td_compl = _TD_to_str(td_compl)
    s_td_remain = _TD_to_str(td_remain)
    s_td_tot = _TD_to_str(td_tot)
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
def find_latest_versions(
    na_item_id: np.ndarray, na_item_id_version_nbr: np.ndarray,
):
    '''
    Find boolean indices for datasets which have the latest version (for that
    particular item ID.

    NOTE: Works, but appears to be slower than necessary.
    2022-01-04: 548 s when applied to entire SDT.


    Parameters
    ----------
    na_item_id             : 1D numpy array of strings.
    na_item_id_version_nbr : 1D numpy array of integers.


    Returns
    -------
    na_b_latest_version : 1D numpy bool array.
    '''
    # LV = Latest Version
    # UII = Unique Item ID

    # ASSERTIONS
    # ----------
    # IMPLEMENTATION NOTE: Automatic tests have historically mistakenly used
    # 0-dim arrays which causes hard-to-understand errors.
    # ==> Want to assert for this.
    # import pdb; pdb.set_trace()
    assert_1D_NA(na_item_id,             np.dtype('O'))
    assert_1D_NA(na_item_id_version_nbr, np.dtype('int64'))
    assert na_item_id.shape == na_item_id_version_nbr.shape

    # Find unique item IDs (to iterate over).
    # NOTE: na_item_id[iUniques] == na_item_id_unique
    na_item_id_unique, na_i_unique, _, na_unique_counts = np.unique(
        na_item_id, return_index=1, return_inverse=1, return_counts=1,
    )

    # ==================================
    # Iterate over unique item IDs (UII)
    # ==================================
    # Pre-allocated. Datasets that should ultimately be kept.
    na_b_latest_version = np.full(na_item_id.size, False)
    for i_uii in range(na_i_unique.size):
        uii  = na_item_id_unique[i_uii]
        n_duplicates = na_unique_counts[i_uii]
        if n_duplicates == 1:
            # CASE: There is only one version.
            na_b_latest_version[na_i_unique[i_uii]] = True
        else:
            # CASE: There are multiple versions.
            uii_latest_version = \
                na_item_id_version_nbr[na_item_id == uii].max()

            b = (na_item_id == uii) \
                & (na_item_id_version_nbr == uii_latest_version)
            (i,) = np.nonzero(b)   # NOTE: Returns tuple or arrays.

            assert i.size == 1, (
                f'Found multiple datasets with item ID={uii}'
                f' and with the same'
                f' highest version number V{uii_latest_version}.'
            )

            na_b_latest_version[i] = True

    # ASSERTION: All item ID's are unique.
    na_item_id2 = na_item_id[na_b_latest_version]
    assert np.unique(na_item_id2).size == na_item_id2.size

    return na_b_latest_version


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
    for (dirPath, _subDirNameList, dirFileNamesList) in os.walk(rootDir):
        for fileName in dirFileNamesList:

            dsfn = \
                erikpgjohansson.solo.metadata.DatasetFilename.parse_filename(
                    fileName,
                )
            # IMPLEMENTATION NOTE:
            # erikpgjohansson.solo.metadata.DatasetFilename.parse_filename()
            # returns None for non-parsable filenames.
            if dsfn:
                _dataSrc, level, instrument, _descriptor = \
                    erikpgjohansson.solo.metadata.parse_DSID(dsfn.dsid)

                filePath = os.path.join(dirPath, fileName)
                filePathList   += [filePath]
                fileNameList   += [fileName]
                fileVerList    += [int(dsfn.version_str)]
                itemIdList     += [dsfn.item_id]
                fileSizeList   += [os.stat(filePath).st_size]
                instrumentList += [instrument]
                levelList      += [level]

                # NOTE: datetime.datetime requires integer seconds+microseconds
                # in separate arguments (as integers). Filenames should only
                # contain time with microseconds=0 so we ignore them.
                tv1    = list(dsfn.tv1)
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
    # erikpgjohansson.solo.soar.dwld.SoarDownloader.download_SDT_DST().
    return dst


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
