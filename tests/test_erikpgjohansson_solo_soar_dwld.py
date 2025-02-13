import erikpgjohansson.solo.soar.const
import erikpgjohansson.solo.soar.dwld
import erikpgjohansson.solo.soar.tests as tests
import json
import numpy as np
import pathlib
import zipfile


JSON_SDTs_ZIP_FILENAME = 'JSON_SDTs_2025-02-12T19.23.58.zip'
'''
Can not open file with Linux utility unzip, but the tests work.

>> unzip JSON_SDTs_2025-02-12T19.23.58.zip
Archive:  JSON_SDTs_2025-02-12T19.23.58.zip
   skipping: EPD_v_public_files.json  need PK compat. v6.3 (can do v4.6)
   skipping: MAG_v_public_files.json  need PK compat. v6.3 (can do v4.6)
   skipping: SWA_v_public_files.json  need PK compat. v6.3 (can do v4.6)
'''


def _get_SoarDownloaderTest(zip_file, tmp_path):
    with zipfile.ZipFile(zip_file, 'r') as z:
        z.extractall(tmp_path)

    dc_json_dc = {}
    for instrument in erikpgjohansson.solo.soar.const.LS_SOAR_INSTRUMENTS:
        path = tmp_path / tests.JSON_SDT_filename(
            instrument,
        )
        with open(path) as f:
            dc_json_dc[instrument] = json.load(f)

    return tests.SoarDownloaderTest(dc_json_dc=dc_json_dc)


def test_convert_JSON_SDT_to_DST___actual_saved_SDTs(tmp_path):
    '''Complex test. Unzips JSON files from SOAR and loads the data
    into SoarDownloaderTest.
    '''
    # ----------
    # Setup test
    # ----------
    zip_file = pathlib.Path(__file__).parent / JSON_SDTs_ZIP_FILENAME
    sodl = _get_SoarDownloaderTest(zip_file, tmp_path)

    # ---------------
    # Test if crashes
    # ---------------
    _ = erikpgjohansson.solo.soar.dwld.download_SDT_DST(sodl)


def test_convert_JSON_SDT_to_DST___manual_SDTs(tmp_path):
    '''NOTE: Indirectly tests _convert_JSON_SDT_to_DST().'''
    '''
    PROPOSAL: Only include uncompressed JSON files. Let git handle the
              compression instead.
        PRO: Simpler code.
        PRO: git can take advantage of small diffs betweej JSON file versions.
    '''

    '''Not completely sure how complete test should be. Mostly tests the
    conversions of component values.'''
    json_data_ls = [[
        "2021-11-04T12:08:05.314", "null", "SCI",
        "solo_L0_epd-step-ll_0680054400-0680140799_V02.bin"
        "", 2746275, "EPD",
        "solo_L0_epd-step-ll_0680054400-0680140799", "V02", "L0",
    ]]
    sodl = tests.SoarDownloaderTest(dc_json_data_ls={'EPD': json_data_ls})
    dst = erikpgjohansson.solo.soar.dwld.download_SDT_DST(sodl)

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
    sodl = tests.SoarDownloaderTest(dc_json_data_ls={'SWA': json_data_ls})
    dst = erikpgjohansson.solo.soar.dwld.download_SDT_DST(sodl)

    na = dst['begin_time_FN']
    assert np.issubdtype(na.dtype, np.datetime64)
    assert na == np.datetime64('2021-04-21T22:02:08.000')
