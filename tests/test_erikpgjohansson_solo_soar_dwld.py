import erikpgjohansson.solo.soar.const
import erikpgjohansson.solo.soar.dwld
import erikpgjohansson.solo.soar.tests as tests
import json
import numpy as np
import pathlib
import zipfile


JSON_SDTs_ZIP_FILENAME = \
    'JSON_SDTs_2022-10-19T15.30.16.zip'
'''Linux utility "unzip" (version "20 April 2009 (v6.0)"; Ubuntu 22.04) can
not open file. The automated tests work. /2024-02-15

Message:
""""
Archive:  JSON_SDTs_2022-10-19T15.30.16.zip
   skipping: EPD_v_public_files.json  need PK compat. v6.3 (can do v4.6)
   skipping: MAG_v_public_files.json  need PK compat. v6.3 (can do v4.6)
   skipping: SWA_v_public_files.json  need PK compat. v6.3 (can do v4.6)
""""
'''


def test_convert_JSON_SDT_to_DST(tmp_path):
    '''NOTE: Indirectly tests _convert_JSON_SDT_to_DST().'''
    '''
    PROPOSAL: Only include uncompressed JSON files. Let git handle the
              compression instead.
        PRO: Simpler code.
        PRO: git can take advantage of small diffs betweej JSON file versions.
    '''

    tmp_path = pathlib.Path(tmp_path)

    def test_stored_actual_SDTs():
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

    def test_manual_SDTs():
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

    test_stored_actual_SDTs()
    test_manual_SDTs()
