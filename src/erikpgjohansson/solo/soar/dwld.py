'''
Code directly related to downloading from SOAR (Solar Orbiter ARchive).

dwld=download

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
data_item_id
    Appears to be same as "item_id". Term used in call to SOAR to download
    datasets.
Note: See MISC_CONVENTIONS.md.

Created by Erik P G Johansson 2020-10-12, IRF Uppsala, Sweden.
'''


import codetiming
import datetime
import erikpgjohansson.solo.asserts
import erikpgjohansson.solo.soar.const as const
import erikpgjohansson.solo.soar.dst
import erikpgjohansson.solo.utils
import json
import logging
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
        PRO: Fits description of module.
        TODO-DEC: Whereto?
            PROPOSAL: utils
            PROPOSAL: New module ~retrieve, ~access
                CON: Just one function.

PROPOSAL: download_raw_SOAR_datasets_table() should return JSON string.
'''


class Downloader:
    '''Abstract class for class that handles all communication with SOAR.
    This is to permit the use of "mock object" for automated testing.
    '''
    def download_raw_SOAR_datasets_table(self):
        raise NotImplementedError()

    def download_latest_dataset(
        self, dataItemId, fileParentPath,
        expectedFileName=None, expectedFileSize=None,
    ):
        raise NotImplementedError()


class SoarDownloader(Downloader):
    '''Class that handles all actual (i.e. not simulated) communication with
    SOAR.'''

    @codetiming.Timer('download_raw_SOAR_datasets_table', logger=None)
    def download_raw_SOAR_datasets_table(self):
        '''
        Download complete list of datasets from SOAR, without modifying the
        list to a more convenient format.

        NOTE: Uses SOAR list "v_public_files" which presumably contains all
              datasets. It does include LL02 and ANC.

        NOTE: begin_time may contain string "null".
        NOTE: item_version == string, e.g. "V02".
        NOTE: This call is slow.

        SOAR(?) BUG: 2022-10-13: It appears that the JSON file downloaded
        from SOAR is not a complete list of datasets at SOAR. It appears to
        be limited by the amount that SOAR can transmit (and amount the user
        can download) in ~62 s. The returned JSON file is valid despite the
        list being truncated though. This also means that a better internet
        connection implies receiving a longer list (tested), or possibly
        receiving a complete one (not tested).

        Returns
        -------
        JsonDict : Representation of SOAR data list.
        '''
        L = logging.getLogger(__name__)
        # 2022-10-18: "Works" except that SOAR has a bug that makes it
        # return a valid JSON table that is ~truncated if the HTTP request
        # exceeds a timeout of ~62 s.
        # url = (
        #     f'{const.SOAR_TAP_URL}/tap/sync?REQUEST=doQuery'
        #     '&LANG=ADQL&FORMAT=json&QUERY=SELECT+*+FROM+v_public_files'
        # )
        # Temporary URL to a smaller table, covering only specified
        # instruments, in order to avoid SOAR bug that truncates the
        # datasets list. These instruments must thus be consistent with (a
        # superset of) the instruments for which "datasetsSubsetFunc" can
        # ever return true.
        url = (
            f'{const.SOAR_TAP_URL}/tap/sync?REQUEST=doQuery'
            '&LANG=ADQL&FORMAT=json&QUERY=SELECT+*+FROM+v_public_files+WHERE+'
            '((instrument=\'EPD\') OR'
            ' (instrument=\'MAG\') OR'
            ' (instrument=\'SWA\'))'
        ).replace(' ', '%20').replace('\'', '%27')

        L.info(f'Calling URL: {url}')
        HttpResponse = urllib.request.urlopen(url)

        s = HttpResponse.read().decode()
        L.info(f'List of datasets downloaded from SOAR: Size: {len(s)} bytes')

        JsonDict = json.loads(s)
        if type(JsonDict) != dict:
            msg = (
                'List of datasets downloaded from SOAR:'
                ' Has been successfully interpreted as JSON,'
                ' but is not a dictionary as expected.'
            )
            L.error(msg)
            raise Exception(msg)

        return JsonDict

    def download_latest_dataset(
        self, dataItemId, fileParentPath,
        expectedFileName=None, expectedFileSize=None,
    ):
        '''
        Download the latest version of a particular dataset.

        NOTE: I have not been able to figure out how to download a dataset of a
        specified version. /Erik P G Johansson 2021-01-11

        Parameters
        ----------
        dataItemId : String
            Specifies the exact dataset except version.
            Unsure if this is an officially defined concept. "data_item_id"
            appears as column name in SOAR's tables.
            Empirically roughly DATASET_ID + date: Filename minus _Vxx.cdf
            (x=digit).
            Ex: solo_L2_epd-ept-south-rates_20200730
        fileParentPath :
        expectedFileName : String, None
        expectedFileSize : Number, None

        Returns
        -------
        filePath

        EXCEPTIONS
        ==========
        HTTPError
            If invalid dataItemId.
        Exception
            If expectedFileName or expectedFileSize do not match the
            downloaded file.

        NOTE: Function selects filename based on HTTP request result.
        '''
        '''
        TODO-DEC: How handle pre-existing files?
            PROPOSAL: Policy argument.
        PROPOSAL: Somehow log download speed bytes/s.
        PROPOSAL: Somehow predict time left.
        PROPOSAL: No exception for downloading unexpected file. Return
                  boolean(s) instead.
        '''

        L = logging.getLogger(__name__)

        # NOTE: Not a real constant since a string is inserted into it.
        def get_URL(dataItemId, level):
            if level in ('LL01', 'LL02'):
                product_type = 'LOW_LATENCY'
            else:
                product_type = 'SCIENCE'

            return (
                f'{const.SOAR_TAP_URL}/data?'
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
        L.info(f'Calling URL: {url}')

        HttpResponse = urllib.request.urlopen(url)
        fileName = self._extract_HTTP_response_filename(HttpResponse)

        # ~ASSERTION
        if expectedFileName:
            if expectedFileName != fileName:
                raise Exception(
                    f'Filename returned from HTTP response "{fileName}" is '
                    f'not equal to expected filenames "{expectedFileName}".',
                )

        filePath = os.path.join(fileParentPath, fileName)

        erikpgjohansson.solo.asserts.path_is_available(filePath)

        # Download file from `HttpResponse` and save it as `filePath`
        # -----------------------------------------------------------
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

    @staticmethod
    def _extract_HTTP_response_filename(HttpResponse):
        '''
        Extract filename of a to-be-downloaded file from an HttpResponse.

        Asserts that response contains a filename. Likely not a perfect
        parsing given a limited understanding of HTTP, but likely good enough.
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


@codetiming.Timer('download_SOAR_DST', logger=None)
def download_SOAR_DST(downloader: Downloader, CacheJsonFilePath=None):
    '''
    Download table of datasets (+metadata) available for download at SOAR.

    Besides downloading the original JSON via Downloader, this function
    (1) provides optional caching of the downloaded JSON file,
    (2) converts JSON file to a DST.

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
    '''
    assert isinstance(downloader, Downloader)

    L = logging.getLogger(__name__)
    if CacheJsonFilePath and os.path.isfile(CacheJsonFilePath):
        # CASE: Caching enabled AND there is a cache file.

        # ===========================================
        # Retrieve SOAR datasets table from JSON file
        # ===========================================
        with open(CacheJsonFilePath) as f:
            with codetiming.Timer(name='json.load()', logger=None):
                # NOTE: Reading file is fast (as opposed to
                # writing file / json.load()).
                JsonDict = json.load(f)
                if type(JsonDict) != dict:
                    msg = (
                        f'Downloaded list of datasets from SOAR:'
                        f' Has been successfully\n'
                        f'    (1) loaded from file (cache) at'
                        f' "{CacheJsonFilePath}", and\n'
                        f'    (2) interpreted as JSON,\n'
                        f'but it is not a dictionary as expected.'
                    )
                    L.error(msg)
                    raise Exception(msg)
    else:
        # CASE: There shall be no caching.
        # ============================
        # Download SOAR datasets table
        # ============================
        JsonDict = downloader.download_raw_SOAR_datasets_table()

    if CacheJsonFilePath and not os.path.isfile(CacheJsonFilePath):
        # Caching enabled AND there is no cache file

        # =====================================
        # Save SOAR datasets table to JSON file
        # =====================================
        with open(CacheJsonFilePath, mode='w', encoding='utf-8') as f:
            with codetiming.Timer(name='json.dump()', logger=None):
                # NOTE: Takes time.
                json.dump(JsonDict, f, indent=2)

    dst = _convert_raw_SOAR_datasets_table(JsonDict)

    return dst, JsonDict


@codetiming.Timer('_convert_raw_SOAR_datasets_table', logger=None)
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
    INT_COLUMN_NAMES = {'file_size'}
    # Columns that should be converted string-->string
    # (numpy array of objects).
    STRING2INT_COLUMN_NAMES = {'item_id'}

    STR2DATETIME_COLUMN_NAMES = {'begin_time', 'archived_on'}

    TIME_STR_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

    assert type(JsonDict) == dict

    L = logging.getLogger(__name__)
    # IMPLEMENTATION NOTE: Useful since function may take a lot of time.
    L.info('Converting raw list of SOAR datasets to DST.')

    metadataList   = JsonDict['metadata']
    dataTuples     = JsonDict['data']
    del JsonDict

    columnNameList = [
        columnMetadataDict['name'] for columnMetadataDict in metadataList
    ]
    dc_na = {}
    for iCol in range(len(columnNameList)):
        colName    = columnNameList[iCol]
        columnList = [value[iCol] for value in dataTuples]
        nRows = len(columnList)

        # Convert data to numpy array, depending on column.

        if colName in INT_COLUMN_NAMES:
            columnArray = np.array(columnList, dtype='int64')

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

        dc_na[columnNameList[iCol]] = columnArray

    # =================================
    # Add extra column "begin_time_FN"
    # =================================
    filenameArray = dc_na['file_name']
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
            assert (fileNameSuffix in const.FILE_SUFFIX_IGNORE_LIST), (
                f'Can neither parse SOAR file name "{fileName}", nor recognize'
                f' the file suffix "{fileNameSuffix}" as a file type'
                ' that should be ignored.'
            )

    dc_na['begin_time_FN'] = beginTimeFnArray

    return erikpgjohansson.solo.soar.dst.DatasetsTable(dc_na)
