'''
Utilities related to using SOAR, but not for connecting to SOAR directly.
Includes functionality for syncing SOAR data (for now).

Initially created 2020-12-17 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import codetiming
import datetime
import erikpgjohansson.solo.asserts
import erikpgjohansson.solo.soar.dst
import erikpgjohansson.solo.soar.dwld
import logging
import numpy as np
import os


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


def assert_col_array(v, dtype=None):

    # PROPOSAL: Use numpy.issubdtype()
    #   PRO: Useful for categories of types.
    #       Ex: Variants of datetime64, strings?
    #       Ex: np.issubdtype(beginTimeArray.dtype, np.dtype('<M8[ms]'))

    # IMPLEMENTATION NOTE: Some automatic tests have historically mistakenly
    # used 0-dim arrays which causes hard-to-understand errors.
    # ==> Want assertion against this.
    assert type(v) == np.ndarray
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
    downloader: erikpgjohansson.solo.soar.dwld.Downloader,
    itemIdArray,
    fileSizeArray,
    outputDirPath,
    logFormat='long',
    downloadByIncrFileSize=False,
    debugDownloadingEnabled=True,
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
    logFormat : str
        String constant. 'long' or 'short'.
    downloadByIncrFileSize : bool
        True: Sort datasets by increasing file size.
        Useful for testing/debugging. Bad for predicted log values.
    debugDownloadingEnabled : bool
        False: Do everything except actual download.
        Useful for testing/debugging.


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

    PROPOSAL: Keyword argument for file-size sorted download.
    '''

    # ASSERTIONS
    assert isinstance(downloader, erikpgjohansson.solo.soar.dwld.Downloader)
    erikpgjohansson.solo.soar.utils.assert_col_array(
        itemIdArray, np.dtype('O'),
    )
    erikpgjohansson.solo.soar.utils.assert_col_array(
        fileSizeArray, np.dtype('int64'),
    )
    erikpgjohansson.solo.asserts.is_dir(outputDirPath)

    L = logging.getLogger(__name__)

    if downloadByIncrFileSize:
        iSort         = np.argsort(fileSizeArray)
        itemIdArray   = itemIdArray[iSort]
        fileSizeArray = fileSizeArray[iSort]

    soFarBytes = 0
    totalBytes = fileSizeArray.sum()
    StartDt    = datetime.datetime.now()

    for i in range(itemIdArray.size):
        itemId   = itemIdArray[i]
        fileSize = fileSizeArray[i]

        nowStr = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        L.info(
            '{}: Downloading: {:.2f} [MiB], {}'.format(
                nowStr, fileSize / 2**20, itemId,
            ),
        )
        if debugDownloadingEnabled:
            downloader.download_latest_dataset(
                itemId,
                outputDirPath,
            )

        # NOTE: Doing statistics AFTER downloading file. Could also be done
        # BEFORE downloading. Note that this uses another timestamp
        # representing "now".
        soFarBytes += fileSize
        SoFarTd     = datetime.datetime.now() - StartDt   # Elapsed wall time.
        soFarSec    = SoFarTd.total_seconds()

        remainingBytes   = totalBytes - soFarBytes
        # Predictions (using plain linear extrapolation).
        remainingTimeSec = (totalBytes - soFarBytes) / soFarBytes * soFarSec
        RemainingTimeTd  = datetime.timedelta(seconds=remainingTimeSec)
        completionDt     = StartDt + SoFarTd + RemainingTimeTd

        if logFormat == 'long':
            L.info(
                'So far:        {:.2f} [MiB] of {:.2f} [MiB]'.format(
                    soFarBytes / 2**20,
                    totalBytes / 2**20,
                ),
            )
            L.info(
                '               {:.2f} [s] = {}'.format(
                    soFarSec,
                    SoFarTd,
                ),
            )
            L.info(
                '               {:.2f} [MiB/s], on average'.format(
                    (soFarBytes / soFarSec) / 2**20,
                ),
            )

            L.info(
                'Remainder:     {:.2f} [MiB] of {:.2f} [MiB]'.format(
                    remainingBytes / 2**20,
                    totalBytes     / 2**20,
                ),
            )
            L.info(
                '               {:.0f} [s] = {} (prediction)'.format(
                    remainingTimeSec,
                    RemainingTimeTd,
                ),
            )
            L.info(f'Expected completion at: {completionDt} (prediction)')
        else:
            assert logFormat == 'short'


@codetiming.Timer('find_latest_versions', logger=None)
def find_latest_versions(itemIdArray, itemVerNbrArray):
    '''
    Find boolean indices for datasets which have the latest version (for that
    particular item ID.

    NOTE: Works, but appears to be slower than necessary.
    2022-01-04: 548 s when applied to entire SOAR table.


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
    erikpgjohansson.solo.soar.utils.assert_col_array(
        itemIdArray,     np.dtype('O'),
    )
    erikpgjohansson.solo.soar.utils.assert_col_array(
        itemVerNbrArray, np.dtype('int64'),
    )
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

            di = erikpgjohansson.solo.utils.parse_dataset_filename(fileName)
            # IMPLEMENTATION NOTE: parse_dataset_filename() returns None for
            # non-parsable filenames.
            if di:
                _dataSrc, level, instrument, _descriptor = \
                    erikpgjohansson.solo.utils.parse_DATASET_ID(
                        di['DATASET_ID'],
                    )

                filePath = os.path.join(dirPath, fileName)
                filePathList    += [filePath]
                fileNameList    += [fileName]
                fileVerList     += [int(di['version string'])]
                itemIdList      += [di['item ID']]
                fileSizeList    += [os.stat(filePath).st_size]
                instrumentList  += [instrument]
                levelList       += [level]

                # NOTE: datetime.datetime requires integer seconds+microseconds
                # in separate arguments (as integers). Filenames should only
                # contain time with microseconds=0 so we ignore them.
                tv1    = list(di['time vector 1'])
                tv1[5] = int(tv1[5])
                beginTimeFnList += [datetime.datetime(*tv1)]

    dst = erikpgjohansson.solo.soar.dst.DatasetsTable({
        'file_name':        np.array(fileNameList,    dtype=object),
        'file_path':        np.array(filePathList,    dtype=object),
        'item_version':     np.array(fileVerList,     dtype='int32'),
        'item_id':          np.array(itemIdList,      dtype=object),
        'file_size':        np.array(fileSizeList,    dtype='int64'),
        'begin_time_FN':    np.array(beginTimeFnList, dtype='datetime64[ms]'),
        'instrument':       np.array(instrumentList,  dtype=object),
        'processing_level': np.array(levelList,       dtype=object),
    })
    # NOTE: Key name "processing_level" chosen to be in agreement with
    # erikpgjohansson.solo.soar.dwld.Downloader.download_SOAR_DST().
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
    assert type(dst) == erikpgjohansson.solo.soar.dst.DatasetsTable

    SEPARATOR_LENGTH = 80

    def sep():
        L.info('=' * SEPARATOR_LENGTH)

    L = logging.getLogger(__name__)

    totalBytes = dst['file_size'].sum()
    bta        = dst['begin_time_FN']
    assert bta.dtype == np.dtype('datetime64[ms]')
    nonNullBta = bta[~np.isnat(bta)]

    sep()
    L.info(title)
    L.info('-'*len(title))
    L.info('Totals: Unique instruments:       ' + str(set(dst['instrument'])))
    L.info(
        '        Unique processing levels: ' + str(
            set(dst['processing_level']),
        ),
    )
    L.info(
        '                                  {:d} datasets'.format(
            dst['file_size'].size,
        ),
    )
    L.info(
        '                                  {:.2f} [GiB]'.format(
            totalBytes / 2**30,
        ),
    )
    if nonNullBta.size > 0:
        L.info('   begin_time_FN (non-null): Min: ' + str(nonNullBta.min()))
        L.info('                             Max: ' + str(nonNullBta.max()))
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
