'''
Code directly related to SOAR (Solar Orbiter ARchive).

NOTE: Does/should not include code related to how datasets downloaded from SOAR
are stored/organized locally at e.g. IRFU (e.g. IDDT functionality, mirroring).

NOTE: Uses numpy.datetime64.
""The datetime API is experimental in 1.7.0, and may undergo changes in future
versions of NumPy.""
https://docs.scipy.org/doc//numpy-1.17.0/reference/arrays.datetime.html
Empirical: numpy.datetime64(..., dtype='datetime64[ms]') works with numpy
v1.18.5, but not numpy v1.19.5.



DEFINITIONS
===========
item_id
    Type of value (column) returned from SOAR when requesting table.
    Unique dataset except version.
    Ex:
        'solo_L2_rpw-tds-surv-tswf-b_20200707'
        'solo_L2_epd-ept-south-rates_20200720'
        'solo_LL02_epd-het-sun-rates_20201012T000034-20201013T000034'
data_item_id
    Appears to be same as "item_id". Term used in call to SOAR to download
    datasets.
DST = DataSets Tables
    [iColumnName][iRecord]. Dictionary of equal length 1D numpy.array
    describing set of datasets. The exact set of columns is not fixed.


Created by Erik P G Johansson 2020-10-12, IRF Uppsala, Sweden.
'''


import codetiming
import datetime
import erikpgjohansson.solo.utils
import erikpgjohansson.solo.soar.utils
import json
import numpy as np
import os.path
import pathlib
import shutil
import urllib.request


'''
BOGIQ
=====
PROPOSAL: Remove dependence on erikpgjohansson.solo, dataset filenaming
          conventions, and FILE_SUFFIX_IGNORE_LIST.
    PRO: Makes module handle ONLY communication with SOAR. More pure.
    PROPOSAL: Move out _convert_raw_SOAR_datasets_table()
'''


FILE_SUFFIX_IGNORE_LIST = ['.zip', '.jp2', '.h5', '.bin', '.fits']
'''File suffixes of SOAR files whose dataset filenames are permitted to not be
recognized (parsable) by the code. These files will effectively be ignored.

NOTE: It is assumed that all filenames with any other file suffix than listed
here can be handled. This means effectively that all file types are explicitly
"listed" one way or another. In case a new file type which this code can not
handle is added to SOAR, then that file type must be added to the list.
'''


@codetiming.Timer('download_SOAR_DST')
def download_SOAR_DST(CacheJsonFilePath=None):
    '''
Download table of datasets (+metadata) available for download at SOAR.

Parameters
----------
CacheJsonFilePath
    Path to file (may or may not pre-exist) to be used for caching.
    None: Do not cache.

Returns
-------
dst : dict[colName][iRow] = numpy.ndarray
    colName : String as in JSON table metadata.
        ['archived_on', 'begin_time', 'data_type', 'file_name',
        'file_size', 'instrument', 'item_id', 'item_version',
        'processing_level']  /2020-10-12
        item_version : Ex: 'V01'
        begin_time   : Ex: '2020-09-28 00:00:33.0'. NOTE: String
JsonDict :

NOTE: See notes at top of file.
NOTE: Same dataset may have multiple versions in list.
'''
    '''
PROPOSAL: Do not return JsonDict.
    PRO: Wrong data structure for debugging.
PROPOSAL: Cache.
'''
    if CacheJsonFilePath and os.path.isfile(CacheJsonFilePath):
        # ===========================================
        # Retrieve SOAR datasets table from JSON file
        # ===========================================
        with open(CacheJsonFilePath) as f:
            with codetiming.Timer(name='json.load()', logger=None):
                # NOTE: Is fast (as opposed to json.load()).
                JsonDict = json.load(f)
    else:
        # ============================
        # Download SOAR datasets table
        # ============================
        JsonDict = _download_raw_SOAR_datasets_table()
    if CacheJsonFilePath and not os.path.isfile(CacheJsonFilePath):
        # =====================================
        # Save SOAR datasets table to JSON file
        # =====================================
        with open(CacheJsonFilePath, mode='w', encoding='utf-8') as f:
            with codetiming.Timer(name='json.dump()', logger=None):
                # NOTE: Takes time.
                json.dump(JsonDict, f, indent=2)

    dst = _convert_raw_SOAR_datasets_table(JsonDict)

    return dst, JsonDict


@codetiming.Timer('_download_raw_SOAR_datasets_table')
def _download_raw_SOAR_datasets_table():
    '''
Download list of datasets from SOAR, without modifying the list to more
convenient format.

NOTE: Uses SOAR list "v_public_files" which presumably contains all datasets.
It does include LL02 and ANC.

NOTE: begin_time may contain string "null".
NOTE: item_version == string, e.g. "V02".
NOTE: This call is SLOW.

Returns
-------
JsonDict : Representation of SOAR data list.
'''
    URL = (
        'http://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery'
        '&LANG=ADQL&FORMAT=json&QUERY=SELECT+*+FROM+v_public_files'
    )

    HttpResponse = urllib.request.urlopen(URL)
    JsonDict = json.loads(HttpResponse.read().decode())

    return JsonDict


@codetiming.Timer('_convert_raw_SOAR_datasets_table')
def _convert_raw_SOAR_datasets_table(JsonDict):
    '''
Convert downloaded SOAR datasets table to better format.
Useful to have this as a separate function for testing purposes.

NOTE: Works, but seems too slow. 2022-01-04: 75 s for entire SOAR table.

Parameters
----------
JsonDict
    JSON-like representation of SOAR datasets table.

Returns
-------
dst : Dictionary of numpy arrays.
'''
    '''
    PROPOSAL: Do not modify/convert columns, but add new columns instead.
        PRO: Useful if format is ambiguous.
            Ex: Time.
        CON: Kind of unnecessary.
    PROPOSAL: Use entirely independent column names.
        PRO: Can use own naming convention.
            PRO: Can indicate format.
                Ex: Numeric (instead of string), time format.

    PROPOSAL: Assert that files not recognized by
        erikpgjohansson.solo.parse_dataset_filename() are really not datasets.
        PROPOSAL: Assert that there is a non-null begin_time.
    '''
    # Columns that should be converted string-->int
    INT_COLUMN_NAMES    = {'file_size'}
    # Columns that should be converted string-->string
    # (numpy array of objects).
    STRING2INT_COLUMN_NAMES = {'item_id'}

    STR2DATETIME_COLUMN_NAMES = {'begin_time', 'archived_on'}

    TIME_STR_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

    metadataList   = JsonDict['metadata']
    columnNameList = [
        columnMetadataDict['name'] for columnMetadataDict in metadataList
    ]
    dataTuples     = JsonDict['data']

    dst = dict()
    for iCol in range(len(columnNameList)):
        colName    = columnNameList[iCol]
        columnList = [value[iCol] for value in dataTuples]
        nRows = len(columnList)

        # Convert data to numpy array, depending on column.

        if colName in INT_COLUMN_NAMES:
            columnArray = np.array(columnList, dtype=np.int)

        elif colName in STRING2INT_COLUMN_NAMES:
            columnArray = np.array(columnList, dtype=object)

        elif colName in STR2DATETIME_COLUMN_NAMES:
            if 0:
                # IMPLEMENTATION 0: Convert to datetime.datetime
                # ----------------------------------------------
                # CON: Less standard time format.
                # PRO: Can use vectorized numpy functionality, e.g.
                #   numpyArray < dt64 .
                # NOTE: A plain
                #   columnArray = np.datetime64(columnList)
                # does not work. Due to "null" strings?
                columnArray = np.full(len(columnList), object())
                for iRow in range(nRows):
                    if columnList[iRow] == 'null':
                        value = None
                    else:
                        value = datetime.datetime.strptime(
                            columnList[iRow],
                            TIME_STR_FORMAT,
                        )
                    columnArray[iRow] = value
            else:
                # IMPLEMENTATION 1: Use numpy's datetime64.
                columnArray = np.full(
                    nRows,
                    np.datetime64('nat'),
                    dtype='datetime64[ms]',
                )
                # NOTE: Must iterate over rows to handle special string "null".
                #   PROPOSAL: Replace string "null"-->"NaT" first, and then
                #             use vectorization?
                for iRow in range(len(columnList)):
                    if columnList[iRow] != 'null':
                        columnArray[iRow] = np.datetime64(
                            columnList[iRow],
                            'ms',
                        )

        elif colName == 'item_version':
            for value in columnList:
                assert value[0] == 'V'
            columnArray = np.array([int(s[1:]) for s in columnList])

        else:
            columnArray = np.array(columnList, dtype=object)

        dst[columnNameList[iCol]] = columnArray

    # =================================
    # Add extra column "begin_time_FN"
    # =================================
    filenameArray   = dst['file_name']
    beginTimeFnArray = np.full(
        nRows,
        np.datetime64('nat'),
        dtype='datetime64[ms]',
    )
    for iRow in range(nRows):
        fileName = filenameArray[iRow]
        di = erikpgjohansson.solo.utils.parse_dataset_filename(fileName)
        # IMPORTANT NOTE: parse_dataset_filename() might fail for datasets
        # which have a valid non-null begin_time. Is therefore dependent on how
        # well-implemented that function is.

        if di and len(di['time vector 1']) == 6:
            # CASE: Can parse filename as dataset filename.

            # NOTE: datetime.datetime requires integer seconds+microseconds
            # in separate arguments (as integers). Filenames should only
            # contain time with microseconds=0 so we ignore them.
            tv1    = list(di['time vector 1'])
            tv1[5] = int(tv1[5])
            value  = datetime.datetime(*tv1)
            beginTimeFnArray[iRow] = np.datetime64(value, 'ms')

            # NOTE: Ex: solo_LL02_eui-fsi174-image_20201021T100259_V01C.fits
            # has begin_time = null, despite having a begin_time_FN.
            # ==> Can therefore not assert that there should be a
            #     begin_time_FN.
            #     assert not np.isnat(dst['begin_time'][iRow])
            # except Exception as E:
            #     pass   # For setting breakpoints
            #     raise E
        else:
            # CASE: Can NOT parse filename.
            # ASSERTION: Assert that file is any of the known cases that
            # erikpgjohansson.solo.parse_dataset_filename() can not handle.
            fileNameSuffix = pathlib.Path(fileName).suffix
            assert (fileNameSuffix in FILE_SUFFIX_IGNORE_LIST), (
                f'Can neither parse SOAR file name "{fileName}", nor recognize'
                f' the file suffix "{fileNameSuffix}" as a file type'
                ' that should be ignored.'
            )

    dst['begin_time_FN'] = beginTimeFnArray

    erikpgjohansson.solo.soar.utils.assert_DST(dst)
    return dst


def _extract_HTTP_response_filename(HttpResponse):
    '''
Extract filename of a to-be-downloaded file.
Asserts that response contains a filename.
Likely not a perfect parsing given a limited understanding of HTTP, but
likely good enough.
'''
    # Example: response
    # -----------------
    # response.getheaders()
    # [('Cache-Control', 'no-cache, no-store, max-age=0, must-revalidate'),
    #  ('Pragma', 'no-cache'),
    #  ('Expires', '0'),
    #  ('X-XSS-Protection', '1; mode=block'),
    #  ('X-Frame-Options', 'SAMEORIGIN'),
    #  ('X-Content-Type-Options', 'nosniff'),
    #  ('Set-Cookie',
    #   'JSESSIONID=6156F42C0A6DFF76D0E80EAB71146709;'
    #   ' Path=/soar-sl-tap; HttpOnly'),
    #  ('Content-Disposition',
    #   'attachment;filename="solo_L2_mag-rtn-normal-1-minute_20200603_V02.cdf"'),
    #  ('Content-Type', 'application/octet-stream'),
    #  ('Content-Length', '28981'),
    #  ('Date', 'Mon, 12 Oct 2020 14:33:03 GMT'),
    #  ('Connection', 'close')]

    FILENAME_PREFIX = 'filename='
    header_value = HttpResponse.getheader('Content-Disposition')
    assert header_value is not None

    for sv in header_value.split(';'):
        if str.startswith(sv, FILENAME_PREFIX):
            fileName = sv[len(FILENAME_PREFIX):]
            # Remove surrounding quotes.
            fileName = fileName.strip('"')
            return fileName

    raise Exception('Failed to derive filename from HTTP response.')


def download_latest_dataset(
    dataItemId, fileParentPath,
    expectedFileName=None, expectedFileSize=None,
    debugCreateEmptyFile=False,
):
    '''
Download the latest version of a particular dataset.

NOTE: I have not been able to figure out how to download a dataset of a
specified version. /Erik P G Johansson 2021-01-11

Parameters
----------
dataItemId : String specifying the exact dataset (not version).
    Unsure if this is an officially defined concept. "data_item_id"
    appears as column name in SOAR's tables.
    Empirically roughly DATASET_ID + date: Filename minus _Vxx.cdf (x=digit).
    Ex: solo_L2_epd-ept-south-rates_20200730
fileParentPath :
expectedFileName : String, None
expectedFileSize : Number, None
debugCreateEmptyFile : Boolean
    True : Creates empty files with filename from SOAR.
           NOTE: Disables any file size checking.

Returns
-------
filePath

EXCEPTIONS
==========
HTTPError : If invalid dataItemId.
Exception : If expectedFileName or expectedFileSize do not match.

NOTE: Does not appear to be able to download LL02 datasets.
NOTE: Function selects filename based on HTTP request result.
'''
    '''
TODO-DEC: How handle pre-existing files?
    PROPOSAL: Policy argument.
PROPOSAL: Somehow log download speed bytes/s.
PROPOSAL: Somehow predict time left.
PROPOSAL: No exception for downloading unexpected file. Return boolean(s).
'''

    # NOTE: Not a real constant since a string is inserted into it.
    def get_URL(dataItemId, level):
        if level in ('LL02', 'LL01'):
            product_type = 'LOW_LATENCY'
        else:
            product_type = 'SCIENCE'
        return (
            f'http://soar.esac.esa.int/soar-sl-tap/data?'
            f'data_item_id={dataItemId}'
            f'&retrieval_type=LAST_PRODUCT&product_type={product_type}'
        )

    assert type(dataItemId) == str

    # Extract level from item ID.
    d1 = erikpgjohansson.solo.utils.parse_item_ID(dataItemId)
    _, level, _, _ = erikpgjohansson.solo.utils.parse_DATASET_ID(
        d1['DATASET_ID'],
    )

    url = get_URL(dataItemId, level)

    # DEBUG
    print(f'url              = {url}')
    print(f'dataItemId       = {dataItemId}')
    print(f'expectedFileName = {expectedFileName}')
    print(f'level            = {level}')

    HttpResponse = urllib.request.urlopen(url)
    fileName = _extract_HTTP_response_filename(HttpResponse)

    # ~ASSERTION
    if expectedFileName:
        if expectedFileName != fileName:
            raise Exception(
                f'Filename returned from HTTP response "{fileName}" is '
                f'not equal to expected filenames "{expectedFileName}".',
            )

    filePath = os.path.join(fileParentPath, fileName)

    erikpgjohansson.solo.asserts.path_is_available(filePath)

    if debugCreateEmptyFile:
        pathlib.Path(filePath).touch()
    else:
        # Download file from `HttpResponse` and save it locally as `filePath`:
        # open() options:
        # 'w' open for writing, truncating the file first
        # 'b' binary mode
        FileObj = open(filePath, 'wb')
        shutil.copyfileobj(HttpResponse, FileObj)
        FileObj.close()

        # ~ASSERTION
        if expectedFileSize:
            fileSize = os.stat(filePath).st_size
            if fileSize != expectedFileSize:
                raise Exception(
                    f'Size of downloaded file '
                    f'("{fileName}"; {fileSize} bytes) is not equal to'
                    f' expected file size ({expectedFileSize} bytes.',
                )

    return filePath
