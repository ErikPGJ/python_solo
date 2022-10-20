import erikpgjohansson.solo.soar.const
import erikpgjohansson.solo.soar.dwld
import erikpgjohansson.solo.soar.tests as tests
import json
import logging
import numpy as np
import os.path
import pathlib
import sys
import tempfile
import zipfile


def test_download_latest_dataset(tmp_path):
    '''
    NOTE: Downloads from internet. Assumes that certain files are available
          online at SOAR. If these files are missing from SOAR, then the test
          fails even if the code is OK.
    '''
    i_test = 0

    # Normalize. Can be string if called from non-pytest.
    tmp_path = pathlib.Path(tmp_path)

    downloader = erikpgjohansson.solo.soar.dwld.SoarDownloader()

    def test(dataItemId):
        '''
        IMPLEMENTATION NOTE: The test deliberately does NOT accept arguments
        expectedFileName and expectedFileSize since they vary with dataset
        version, i.e. are more likely to vary with time.
        '''
        nonlocal i_test
        dirPath = pathlib.Path(tmp_path) / f'test_{i_test}'
        i_test = i_test + 1
        os.mkdir(dirPath)

        print(
            f'ATEST: Downloading online data from SOAR:'
            f' dataItemId={dataItemId}',
        )
        actFilePath = downloader.download_latest_dataset(
            dataItemId, dirPath,
            expectedFileName=None, expectedFileSize=None,
        )
        assert os.path.isfile(actFilePath)

    test('solo_L1_epd-sis-a-rates-slow_20200813')
    test('solo_L1_epd-step-nom-close_20200813')

    # Test LL
    test('solo_LL02_mag_20220621T000205-20220622T000204')
    test('solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025')


JSON_SDTs_ZIP_FILENAME = \
    'JSON_SDTs_2022-10-19T15.30.16.zip'


def test_convert_JSON_SDT_to_DST(tmp_path):
    '''NOTE: Indirectly tests _convert_JSON_SDT_to_DST().'''
    '''
    PROPOSAL: Only include uncompressed JSON files. Let git handle the
              compression instead.
        PRO: Simpler code.
        PRO: git can take advantage of small diffs betweej JSON file versions.
    '''

    tmp_path = pathlib.Path(tmp_path)

    def test_SOAR_tables():
        '''Complex test. Unzips JSON files from SOAR and loads the data
        into MockDownloader.
        '''
        # ----------
        # Setup test
        # ----------
        zip_file = pathlib.Path(__file__).parent \
            / JSON_SDTs_ZIP_FILENAME
        with zipfile.ZipFile(zip_file, 'r') as z:
            z.extractall(tmp_path)

        dc_json_dc = {}
        for instrument in erikpgjohansson.solo.soar.const.LS_SOAR_INSTRUMENTS:
            path = tmp_path / tests.JSON_SDT_filename(
                instrument,
            )
            with open(path) as f:
                dc_json_dc[instrument] = json.load(f)

        md = tests.MockDownloader(dc_json_dc=dc_json_dc)

        # ---------------
        # Test if crashes
        # ---------------
        _ = erikpgjohansson.solo.soar.dwld.download_SDT_DST(md)

    def test_hardcoded_tables():
        '''Not completely sure how complete test should be. Mostly tests the
        conversions of component values.'''
        json_data_ls = [[
            "2021-11-04T12:08:05.314", "null", "SCI",
            "solo_L0_epd-step-ll_0680054400-0680140799_V02.bin"
            "", 2746275, "EPD",
            "solo_L0_epd-step-ll_0680054400-0680140799", "V02", "L0",
        ]]
        md = tests.MockDownloader(dc_json_data_ls={'EPD': json_data_ls})
        dst = erikpgjohansson.solo.soar.dwld.download_SDT_DST(md)

        na = dst['begin_time']
        assert na.shape == (1,)
        assert np.issubdtype(na.dtype, np.datetime64)
        assert np.isnan(na[0])

        na = dst['item_version']
        assert np.issubdtype(na.dtype, np.integer)
        assert na[0] == 2

        na = dst['file_size']
        assert np.issubdtype(na.dtype, np.integer)
        assert na[0] == 2746275

        na = dst['item_id']
        assert np.issubdtype(na.dtype, object)
        assert na[0] == 'solo_L0_epd-step-ll_0680054400-0680140799'
        # NOTE: begin_time_FN is undefined since dataset filename parsing
        #       does not yet support parsing that kind of time interval
        #       string (yet; has never been needed).

        json_data_ls = [[
            "2021-04-23T21:51:31.704", "2021-04-21T22:02:08.0", "LL",
            "solo_LL02_swa-his-rat_20210421T220208-20210422T215738_V01C.cdf",
            1258995, "SWA",
            "solo_LL02_swa-his-rat_20210421T220208-20210422T215738", "V01",
            "LL02",
        ]]
        md = tests.MockDownloader(dc_json_data_ls={'SWA': json_data_ls})
        dst = erikpgjohansson.solo.soar.dwld.download_SDT_DST(md)

        na = dst['begin_time_FN']
        assert np.issubdtype(na.dtype, np.datetime64)
        assert na == np.datetime64('2021-04-21T22:02:08.000')

    test_SOAR_tables()
    test_hardcoded_tables()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    # t = tempfile.TemporaryDirectory()
    # test_download_latest_dataset(t.name)

    t = tempfile.TemporaryDirectory()
    test_convert_JSON_SDT_to_DST(t.name)
