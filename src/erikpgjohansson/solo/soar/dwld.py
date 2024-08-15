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


Created by Erik P G Johansson 2020-10-12, IRF Uppsala, Sweden.
'''


import codetiming
import datetime
import erikpgjohansson.solo.asserts
import erikpgjohansson.solo.soar.const as const
import erikpgjohansson.solo.soar.dst
import erikpgjohansson.solo.metadata
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
    PROPOSAL: Move out _convert_JSON_SDT_to_DST()
        PRO: Fits description of module.
        TODO-DEC: Whereto?
            PROPOSAL: utils
            PROPOSAL: New module ~retrieve, ~access
                CON: Just one function.

PROPOSAL: download_JSON_SDT() should return JSON string.
    CON: Harder to hard code return values for mock object in test code.
'''


class Downloader:
    '''Abstract class for class that handles all communication with SOAR.
    This is to permit the use of "mock object" for automated testing.
    '''
    def download_JSON_SDT(self, instrument: str):
        raise NotImplementedError()

    def download_latest_dataset(
        self, dataItemId, dirPath,
        expectedFileName=None, expectedFileSize=None,
    ):
        raise NotImplementedError()


class SoarDownloader(Downloader):
    '''Class that handles all actual (i.e. not simulated) communication with
    SOAR.'''

    @codetiming.Timer('download_JSON_SDT', logger=None)
    def download_JSON_SDT(self, instrument: str):
        '''
        Download complete list of datasets from SOAR, and return the resulting
        JSON table as a Python data structure.

        NOTE: Uses SOAR list "v_public_files" which presumably contains all
              datasets, including older dataset versions. It does include LL02
              and ANC.

        NOTE: This call is slow.
        NOTE: Only downloads SDT for one instrument at a time in
              order to reduce the size of the returned list.

        Returns
        -------
        JsonDc : Representation of SOAR dataset list.
        --
        NOTE: begin_time may contain string "null".
        NOTE: item_version == string, e.g. "V02".
        '''
        assert type(instrument) is str

        L = logging.getLogger(__name__)

        # URL to JSON SDT (science + LL + "kernel type files")
        # ===============================================================
        # URL that covers all instruments.
        # 2022-10-18: "Works" except that SOAR has a bug that makes it
        # return a valid JSON table that is ~truncated if the HTTP request
        # exceeds a timeout of ~62 s.
        # url = (
        #     f'{const.SOAR_TAP_URL}/tap/sync?REQUEST=doQuery'
        #     '&LANG=ADQL&FORMAT=json&QUERY=SELECT+*+FROM+v_public_files'
        # )
        # ---------------------------------------------------------------------
        # URL to a smaller table, covering only multiple specified
        # instruments, in order to avoid SOAR bug that truncates the
        # datasets list. These instruments must thus be consistent with (a
        # superset of) the instruments for which "datasetsSubsetFunc" can
        # ever return true.
        # url = (
        #     f'{const.SOAR_TAP_URL}/tap/sync?REQUEST=doQuery'
        #     '&LANG=ADQL&FORMAT=json&QUERY=SELECT+*+FROM+v_public_files+WHERE+'
        #     '((instrument=\'EPD\') OR'
        #     ' (instrument=\'MAG\') OR'
        #     ' (instrument=\'SWA\'))'
        # ).replace(' ', '%20').replace('\'', '%27')
        # ---------------------------------------------------------------------
        # URL to SDT for ONE instrument. This reduces the size of the table
        # to not trigger above SOAR bug.
        # --
        # NOTE: Should be possible to only download the latest version datasets
        # using v_sc_data_item (for science) and v_ll_data_item (for LL),
        # but this splits across instruments (IRFU mirror syncs both SWA
        # science and SWA LL data).
        url = (
            f'{const.SOAR_TAP_URL}/tap/sync?REQUEST=doQuery'
            '&LANG=ADQL&FORMAT=json&QUERY=SELECT+*+FROM+v_public_files+WHERE+'
            f'instrument=\'{instrument}\''
        ).replace('\'', '%27')

        L.info(f'Calling URL: {url}')
        HttpResponse = urllib.request.urlopen(url)

        s = HttpResponse.read().decode()
        L.info(
            f'JSON SDT (SOAR Datasets Table)'
            f' downloaded from SOAR for {instrument} :'
            f' Size: {len(s)} bytes',
        )

        JsonDc = json.loads(s)
        if type(JsonDc) is not dict:
            msg = (
                f'JSON SDT (SOAR Datasets Table)'
                f' downloaded from SOAR for {instrument} :'
                f' Has been successfully interpreted as JSON'
                f' but is not a dictionary at the root level as expected.'
            )
            L.error(msg)
            raise Exception(msg)

        return JsonDc

    def download_latest_dataset(
        self, dataItemId, dirPath,
        expectedFileName=None, expectedFileSize=None,
    ):
        '''
        Download the latest version of a particular dataset.

        Parameters
        ----------
        dataItemId : String
            Specifies the exact dataset except version.
            Unsure if this is an officially defined concept. "data_item_id"
            appears as column name in JSON SDTs.
            Empirically roughly DSID + date: Filename minus _Vxx.cdf
            (x=digit).
            Ex: solo_L2_epd-ept-south-rates_20200730
        dirPath :
        expectedFileName : String, None
        expectedFileSize : Number, None

        Returns
        -------
        filePath

        EXCEPTIONS
        ==========
        HTTPError
            If invalid dataItemId sent to SOAR.
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
        PROPOSAL: Change to downloading specific version. Can probably be done.
            TODO-DEC: How?
                Ex:
                http://soar.esac.esa.int/soar-sl-tap/data?retrieval_type=PRODUCT&QUERY=SELECT+filepath,filename+FROM+soar.v_sc_repository_file+WHERE+filename='solo_L2_epd-sis-a-rates-slow_20200615_V05.cdf'
            PRO: More reliable for calling code if SOAR updates version
                 between decision to download specific version, and actual
                 download.
        PROPOSAL: Use compress=true to download *.gz files instead.
            https://www.cosmos.esa.int/web/soar/data-requests
            PRO: Could make the downloads faster.
            CON: Has to decompress locally.
                PRO: More complex code.
                NOTE: Can be done in separate process.
            NOTE: Quick experiment shows little compression ==>
                  little potential improvement.
                525K solo_L1_epd-epthet2-nom-close_20200819_V01.cdf
                464K solo_L1_epd-epthet2-nom-close_20200819_V01.cdf.gz
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

        assert type(dataItemId) is str

        # Extract level from item ID.
        d1 = erikpgjohansson.solo.metadata.parse_item_ID(dataItemId)
        if d1 is None:
            raise Exception(f'Can not parse dataItemId="{dataItemId}"')
        _, level, _, _ = erikpgjohansson.solo.metadata.parse_DSID(
            d1['DSID'],
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

        filePath = os.path.join(dirPath, fileName)

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


@codetiming.Timer('download_SDT_DST', logger=None)
def download_SDT_DST(downloader: Downloader):
    '''
    Download table of datasets (+metadata) from SOAR.

    Besides downloading the original JSON via Downloader, this function
    (1) splits up the download into separate downloads for separate
        instruments (to avoid SOAR bug), and then combines them into one table,
    (2) converts JSON file to a DST.

    Returns
    -------
    dst : dict[colName][iRow] = numpy.ndarray
        colName : String as in JSON table metadata.
            ['archived_on', 'begin_time', 'data_type', 'file_name',
            'file_size', 'instrument', 'item_id', 'item_version',
            'processing_level']  /2020-10-12
            item_version : Ex: 'V01'
            begin_time   : Ex: '2020-09-28 00:00:33.0'. NOTE: String

    NOTE: See notes at top of file.
    NOTE: Same dataset may have multiple versions in list.
    '''
    assert isinstance(downloader, Downloader)

    ls_dst = []
    for instrument in erikpgjohansson.solo.soar.const.LS_SOAR_INSTRUMENTS:
        dc_json = downloader.download_JSON_SDT(instrument)
        ls_dst.append(_convert_JSON_SDT_to_DST(dc_json))

    all_dst = ls_dst[0]
    for dst in ls_dst[1:]:
        all_dst = all_dst + dst

    return all_dst


@codetiming.Timer('_convert_JSON_SDT_to_DST', logger=None)
def _convert_JSON_SDT_to_DST(JsonDc):
    '''
    Convert downloaded JSON SDT to better format.

    NOTE: Works, but seems too slow. 2022-01-04: 75 s for entire SDT.

    Parameters
    ----------
    JsonDc
        JSON-like representation of SDT.

    Returns
    -------
    dst : DST
    '''
    '''
    PROPOSAL: Use entirely independent column names.
        PRO: Can use own naming convention.
            PRO: Can indicate format.
                Ex: Numeric (instead of string), time format.

    PROPOSAL: Assert that files not recognized by
        erikpgjohansson.solo.parse_dataset_filename() are really not datasets.
        PROPOSAL: Assert that there is a non-null begin_time.

    PROPOSAL: Abolish creation of "begin_time_FN". Seems unnecessary.
              "begin_time" probably works.
        TODO: Check.
    '''
    # Columns that should be converted int-->int
    INT_TO_INT_COLUMN_NAMES = {'file_size'}
    # Columns that should be converted string-->string
    # (numpy array of objects).
    STRING_TO_STRING_COLUMN_NAMES = {'item_id'}
    # Columns that should be converted
    # string --> datetime64
    STR_TO_DT64_COLUMN_NAMES = {'begin_time', 'archived_on'}

    assert type(JsonDc) is dict

    L = logging.getLogger(__name__)
    # IMPLEMENTATION NOTE: Useful since function may take a lot of time.
    L.info('Converting downloaded JSON SDT (SOAR Datasets Table) to DST.')

    metadataList = JsonDc['metadata']
    dataTuples   = JsonDc['data']
    del JsonDc

    # =====================================================================
    # For every column in the JSON SDT, create one column in the DST. For
    # columns recognized by name, convert types as specified.
    # =====================================================================
    # NOTE: Implementation should permit the JSON SDT to add or remove columns
    # for future compatibility.
    columnNameList = [
        columnMetadataDc['name'] for columnMetadataDc in metadataList
    ]
    dc_na = {}
    for iCol in range(len(columnNameList)):
        colName    = columnNameList[iCol]
        columnList = [value[iCol] for value in dataTuples]
        nRows = len(columnList)

        # ================================================
        # Convert data to numpy array, depending on column
        # ================================================
        if colName in INT_TO_INT_COLUMN_NAMES:
            columnArray = np.array(columnList, dtype='int64')

        elif colName in STRING_TO_STRING_COLUMN_NAMES:
            columnArray = np.array(columnList, dtype=object)

        elif colName in STR_TO_DT64_COLUMN_NAMES:
            columnArray = np.full(
                nRows, np.datetime64('NaT'), dtype='datetime64[ms]',
            )
            # NOTE: Must iterate over rows to handle special string "null".
            for iRow in range(len(columnList)):
                if columnList[iRow] != 'null':
                    columnArray[iRow] = np.datetime64(columnList[iRow], 'ms')

        elif colName == 'item_version':
            for value in columnList:
                assert value[0] == 'V'
            columnArray = np.array(
                [int(s[1:]) for s in columnList], dtype='int64',
            )

        else:
            columnArray = np.array(columnList, dtype=object)

        dc_na[columnNameList[iCol]] = columnArray

    # =================================
    # Add extra column "begin_time_FN"
    # =================================
    filenameArray = dc_na['file_name']
    beginTimeFnArray = np.full(
        nRows, np.datetime64('nat'), dtype='datetime64[ms]',
    )
    for iRow in range(nRows):
        fileName = filenameArray[iRow]
        di = erikpgjohansson.solo.metadata.parse_dataset_filename(fileName)
        # IMPORTANT NOTE: parse_dataset_filename() might fail for datasets
        # which have a valid non-null begin_time. Is therefore dependent on how
        # well-implemented that function is.

        if di and len(di['time vector 1']) == 6:
            # CASE: Can parse dataset filename and time interval string.

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
