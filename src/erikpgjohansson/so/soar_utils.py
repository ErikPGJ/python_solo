# -*- coding: UTF-8 -*-
'''
Utilities related to using SOAR, but not for connecting to SOAR directly.
Includes functionality for syncing SOAR data (for now).

Initially created 2020-12-17 by Erik P G Johansson, IRF Uppsala, Sweden.
'''
'''
BOGIQ
=====
PROPOSAL: Assertion function: table on format dictionary, with same-length arrays as values.
PROPOSAL: Class for dataset tables/DSTs. Constant set of columns.
        Use "None"/NaN for unknown values.

NOTE: download_latest_dataset() downloads latest dataset
          version, not specified dataset version?
    PROPOSAL: Compare downloaded datasets with online list.
        PROPOSAL: Assertion.
        PROPOSAL: If disagreeing, re-download online list and compare again (once).

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
            Ex: Set operation between sets of file sizes (mixing item IDs together).
            Ex: Set operation between sets of versions (mixing item IDs together).
        CON: DST is list, not set. May have duplicates (e.g. versions, file sizes, item IDs)
    PROPOSAL: Difference between DSTs for selected columns.
        In practice: item ID, item ID+version, item ID+version+file size (?)



PROPOSAL: Split up ~SOAR-related functionality into multiple modules.
    NOTE: Should exclude so.iddt since it is unrelated to SOAR. It is used also
          for pure IRFU processing!
    TODO-DEC: Should functionality for managing DSTs be considered ~SOAR?
    TODO-DEC: Should separate SOAR and mirroring code?
        PROPOSAL: so.soar + so.soar_mirror
            PRO: so.soar_mirror USES so.soar, but is not PART of SOAR-functionality.
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



import codetiming
import datetime
import erikpgjohansson.asserts
import erikpgjohansson.so.soar
import numpy as np
import os







def assert_DST(dst):
    '''
Assert that an argument is a legal DST (DataSets Table).

Parameters
----------
dst : A DST

Returns
-------
None.
'''
    assert type(dst == dict)

    nRows = None
    for (k,v) in dst.items():
        assert type(k) == str
        erikpgjohansson.so.soar_utils.assert_col_array(v)
        #assert type(v) == np.ndarray
        #assert v.ndim == 1

        if nRows is None:
            nRows = v.size
        else:
            assert nRows == v.size

    # ASSERTION: Implicitly that there are >=1 COLUMNS (not >= rows).
    assert nRows is not None

    return nRows







def nRows_DST(dst):
    return assert_DST(dst)







def assert_col_array(v, dtype=None):

    # PROPOSAL: Use numpy.issubdtype()
    #   PRO: Useful for categories of types.
    #       Ex: Variants of datetime64, strings?
    #       Ex: np.issubdtype(beginTimeArray.dtype, np.dtype('<M8[ms]'))

    # IMPLEMENTATION NOTE: Some automatic tests have historically mistakenly
    # used 0-dim arrays which causes hard-to-understand errors.
    # ==> Want assertion against this.
    assert type(v) == np.ndarray
    assert v.ndim  == 1

    if dtype:
        # ASSERTION: Correct ARGUMENT type: dtype (not the array)
        #assert type(dtype) == np.dtype, \
        assert isinstance(dtype, np.dtype), \
            'Argument dtype has the wrong type.'

        # ASSERTION: Array type
        assert v.dtype == dtype

    #return v.size   # Exclude?







def index_DST(dst, bi):
    '''
"Index" all arrays in dictionary, i.e. return dictionary where each dictionary
value contains only that subset defined by the indices specified in "bi".

Simple utility function to avoid having to remember exact syntax.


Parameters
----------
bi : numpy array
    boolean index or integer index.

    '''
    assert_DST(dst)
    assert type(bi) == np.ndarray

    return {k:v[bi] for k,v in dst.items()}







def download_latest_datasets_batch(itemIdArray,
                                   fileSizeArray,
                                   outputDirPath,
                                   logFormat='long',
                                   downloadByIncrFileSize=False,
                                   debugDownloadingEnabled=True,
                                   debugCreateEmptyFiles=False):
    '''
Download latest versions of datasets (multiple ones), for selected item ID's.
Will likely overwrite pre-existing files (not checked).
Will log progress, speed, predicted remainder & completion to stdout.


Parameters
----------
dst
fileSizeArray : 1D numpy.ndarray of integers.
    NOTE: The code needs it at the very least for logging predicted remainder.
downloadByIncrFileSize : bool
    True: Sort datasets by increasing file size.
    Useful for testing/debugging. Bad for predicted log values.
debugDownloadingEnabled : bool
    False: Do everything except actual download.
    Useful for testing/debugging.
debugCreateEmptyFiles : bool
    True: Create empty files instead of downloading them.
        NOTE: The filename is still fetched from a HTTP request which still
        takes some time, but which is still much faster than actually downloading
        the file.
    Useful for testing/debugging.


Returns
-------
None.
    '''
    '''
TODO-DEC: How handle pre-existing files? How does download_latest_dataset()?
    NOTE: This function does not know the filenames.
    PROPOSAL: policy which is sent to download_latest_dataset().
PROPOSAL: Argument for filenames.

PROPOSAL: Keyword argument for file-size sorted download.
    '''

    # ASSERTIONS
    erikpgjohansson.so.soar_utils.assert_col_array(itemIdArray,   np.dtype('O'))
    erikpgjohansson.so.soar_utils.assert_col_array(fileSizeArray, np.dtype('int64'))
    erikpgjohansson.asserts.is_dir(outputDirPath)



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
        print('{0}: Downloading: {1:.2f} [MiB], {2}'.format(
            nowStr, fileSize/2**20, itemId))
        if debugDownloadingEnabled:
            erikpgjohansson.so.soar.download_latest_dataset(
                itemId,
                outputDirPath,
                debugCreateEmptyFile=debugCreateEmptyFiles)

        # NOTE: Doing statistics AFTER downloading file. Could also be done
        # BEFORE downloading. Note that this uses another timestamp
        # representing "now".
        soFarBytes += fileSize
        SoFarTd     = datetime.datetime.now() - StartDt   # Elapsed wall time.
        soFarSec    = SoFarTd.total_seconds()

        remainingBytes   = totalBytes - soFarBytes
        # Predictions (using plain linear extrapolation).
        remainingTimeSec = (totalBytes-soFarBytes) / soFarBytes * soFarSec
        RemainingTimeTd  = datetime.timedelta(seconds=remainingTimeSec)
        completionDt     = StartDt + SoFarTd + RemainingTimeTd

        if logFormat == 'long':
            print('So far:        {0:.2f} [MiB] of {1:.2f} [MiB]'.format(
                soFarBytes / 2**20,
                totalBytes / 2**20))
            print('               {0:.2f} [s] = {1}'.format(
                soFarSec,
                SoFarTd))
            print('               {0:.2f} [MiB/s], on average'.format(
                soFarBytes/soFarSec / 2**20))

            print('Remainder:     {0:.2f} [MiB] of {1:.2f} [MiB]'.format(
                remainingBytes / 2**20,
                totalBytes     / 2**20))
            print('               {0:.0f} [s] = {1} (prediction)'.format(
                remainingTimeSec,
                RemainingTimeTd))
            print('Expected completion at: {0} (prediction)'.format(completionDt))
        else:
            assert logFormat == 'short'







@codetiming.Timer('find_latest_versions')
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
    #import pdb; pdb.set_trace()
    erikpgjohansson.so.soar_utils.assert_col_array(
        itemIdArray,     np.dtype('O'))
    erikpgjohansson.so.soar_utils.assert_col_array(
        itemVerNbrArray, np.dtype('int64'))
    assert itemIdArray.shape == itemVerNbrArray.shape



    # NOTE: itemIdArray[iUniques] == uniqItemIdArray
    (uniqItemIdArray, iUniques, jInverse, uniqueCounts) = np.unique(
        itemIdArray, return_index=1, return_inverse=1, return_counts=1)

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
                'Found multiple datasets with itemId = {0}'
                +' with the same highest version number V{2:d}.').format(
                uii, uiiLv)

            bLvArray[i] = True

    # ASSERTION: All item ID's are unique.
    itemIdArray2 = itemIdArray[bLvArray]
    assert np.unique(itemIdArray2).size == itemIdArray2.size

    return bLvArray







def derive_DST_from_dir(rootDir):
    '''
Derive a DST from a directory tree datasets. Searches directory recursively.

NOTE: Ignores filenames that can not be parsed as datasets.


Parameters
----------
rootDir : String

Returns
-------
dst
'''
    erikpgjohansson.asserts.is_dir(rootDir)

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

            di = erikpgjohansson.so.utils.parse_dataset_filename(fileName)
            # IMPLEMENTATION NOTE: parse_dataset_filename() returns None for
            # non-parsable filenames.
            if di:
                (_dataSrc, level, instrument, _descriptor
                 ) = erikpgjohansson.so.utils.parse_DATASET_ID(di['DATASET_ID'])

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

    return {
        'file_name'       :np.array(fileNameList,    dtype=object),
        'file_path'       :np.array(filePathList,    dtype=object),
        'item_version'    :np.array(fileVerList,     dtype=np.int),
        'item_id'         :np.array(itemIdList,      dtype=object),
        'file_size'       :np.array(fileSizeList,    dtype=np.int),
        'begin_time_FN'   :np.array(beginTimeFnList, dtype='datetime64[ms]'),
        'instrument'      :np.array(instrumentList,  dtype=object),
        'processing_level':np.array(levelList,       dtype=object)
        }
    # NOTE: Column name "processing_level" chose to be in agreement with
    # erikpgjohansson.so.soar.download_SOAR_DST().







def filter_DST(dst, levelsSet=None, instrumentsSet=None,
               intervalTimeStrs=None):
    '''
General-purpose customizable DST filter. In particular for manualy selecting
which subset of datasets that should be downloaded or synced. Datasets in DST
that match all arguments will be kept. All other datasets will be removed.

Parameters
----------
dst :
levelsSet : Set of strings
instrumentsSet : Set of instruments
intervalTimeStrs : Two UTC strings, start & stop.
    Approximate time interval to keep. Applies to column begin_time.

Returns
-------
dict
    '''
    b = np.full(len(dst['processing_level']), True)

    if levelsSet:
        b = b & np.isin(dst['processing_level'], np.array(list(levelsSet)))
    if instrumentsSet:
        b = b & np.isin(dst['instrument'],       np.array(list(instrumentsSet)))

    if intervalTimeStrs:
        assert len(intervalTimeStrs) == 2
        DtMin = np.datetime64(intervalTimeStrs[0])
        DtMax = np.datetime64(intervalTimeStrs[1])
        bta = dst['begin_time']   # BTA = Begin Time Array
        b = b & (DtMin <= bta) & (bta <= DtMax)

    return index_DST(dst, b)







def log_DST(dst):
    assert_DST(dst)

    totalBytes = dst['file_size'].sum()
    bta        = dst['begin_time_FN']
    assert bta.dtype == np.dtype('datetime64[ms]')
    nonNullBta = bta[~np.isnat(bta)]

    print('='*80)
    print(    'Totals: Unique instruments:       ' + str(set(dst['instrument'])))
    print(    '        Unique processing levels: ' + str(set(dst['processing_level'])))
    print(    '                                  {0:d} datasets'.format(
        dst['file_size'].size))
    print(    '                                  {0:.2f} [GiB]'.format(
        totalBytes / 2**30))
    if nonNullBta.size > 0:
        print('   begin_time_FN (non-null): Min: ' + str(nonNullBta.min()))
        print('                             Max: ' + str(nonNullBta.max()))
    print('='*80)







def log_codetiming():
    '''Quick-and-dirty function for logging codetiming results.
    '''
    for key, value in codetiming.Timer.timers.items():
        print(f'{key:40s} {value:f} [s]')







def download_batch___MTEST(fileParentPath):
    '''
    Batch downloading with mostly hardcoded arguments.
    '''
    # dirDst = derive_DST_from_dir(fileParentPath)
    # print('dirDst:')
    # log_DST(dirDst)

    (dst, _JsonDict) = erikpgjohansson.so.soar.download_SOAR_DST()
    bLv = find_latest_versions(dst['item_ID'], dst['item_version'])
    dst = index_DST(dst, bLv)
    dst = filter_DST(dst,
                     levelsSet=['L2'],
                     instrumentsSet=['MAG'],
                     intervalTimeStrs=('2020-07-01', '2020-07-01 12:00:00'))

    download_latest_datasets_batch(dst['item_id'], dst['file_size'], fileParentPath)



def download_batch___MANUAL():
    '''
    Batch downloading with mostly hardcoded arguments.
    '''
    # def print_bytes():
    #     totalBytes = dst['file_size'].sum()
    #     print('Number of datasets:     {0:d}'.format(dst['file_size'].size))
    #     print('Total size of datasets: {0:.2f} [GiB]'.format(totalBytes / 2**30))

    #OUTPUT_DIR = '/home/erjo/temp/soar/download'
    OUTPUT_DIR = '/data/solo/soar/download'
    #OUTPUT_DIR = '/homelocal/erjo/SOLAR_ORBITER/SOAR_download'
    #intervalTimeStrs=('2020-07-01', '2020-07-02')
    #intervalTimeStrs=('2020-01-01', '2020-12-31')

    (dst, _JsonDict)  = erikpgjohansson.so.soar.download_SOAR_DST()
    bLv = find_latest_versions(dst['item_id'], dst['item_version'])
    dst = index_DST(dst, bLv)

    #dst  = filter_DST_dir(dst, OUTPUT_DIR)   # Remove already downloaded files.

    ######################
    intervalTimeStrs = ('2020-09-01', '2021-07-01 12:00:00')
    dst2 = filter_DST(dst, levelsSet=['L1', 'L2'],
                           instrumentsSet=['SWA', 'EPD'],
                           intervalTimeStrs=intervalTimeStrs)
    log_DST(dst2)
    download_latest_datasets_batch(dst2['item_id'],
                                   dst2['file_size'],
                                   OUTPUT_DIR,
                                   downloadByIncrFileSize=False,
                                   debugDownloadingEnabled=True)
    ######################
