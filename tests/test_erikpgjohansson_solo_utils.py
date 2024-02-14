import erikpgjohansson.solo.utils
import inspect
import pytest


def test_parse_dataset_filename():

    def test(filename, expResult):
        actFnResult = erikpgjohansson.solo.utils.parse_dataset_filename(
            filename,
        )
        assert actFnResult == expResult

        if actFnResult is not None:
            # =======================================================
            # Assert consistency between parse_dataset_filename() and
            # parse_item_ID()
            # =======================================================
            # II = Item ID
            actIiResult = erikpgjohansson.solo.utils.parse_item_ID(
                actFnResult['item ID'],
            )
            SHARED_KEYS = ('DATASET_ID', 'time vector 1')
            assert all(
                actIiResult[key] == actFnResult[key] for key in SHARED_KEYS
            )

    test(
        'solo_HK_rpw-bia_20200301_V01.cdf',
        {
            'DATASET_ID': 'SOLO_HK_RPW-BIA',
            'time interval string': '20200301',
            'version string': '01',
            'time vector 1': (2020, 3, 1, 0, 0, 0.0),
            'item ID': 'solo_HK_rpw-bia_20200301',
        },
    )

    test(
        'solo_L2_rpw-lfr-surv-cwf-e-cdag_20200213_V02.cdf',
        {
            'DATASET_ID': 'SOLO_L2_RPW-LFR-SURV-CWF-E',
            'time interval string': '20200213',
            'version string': '02',
            'time vector 1': (2020, 2, 13, 0, 0, 0.0),
            'item ID': 'solo_L2_rpw-lfr-surv-cwf-e_20200213',
        },
    )   # CDAG

    test(
        'solo_L1_rpw-bia-sweep-cdag_20200307T053018-20200307T053330_V01.cdf',
        {
            'DATASET_ID': 'SOLO_L1_RPW-BIA-SWEEP',
            'time interval string': '20200307T053018-20200307T053330',
            'version string': '01',
            'time vector 1': (2020, 3, 7, 5, 30, 18.0),
            'item ID': 'solo_L1_rpw-bia-sweep_20200307T053018-20200307T053330',
        },
    )

    test(
        'solo_L1_rpw-bia-current-cdag_20200301-20200331_V01.cdf',
        {
            'DATASET_ID': 'SOLO_L1_RPW-BIA-CURRENT',
            'time interval string': '20200301-20200331',
            'version string': '01',
            'time vector 1': (2020, 3, 1, 0, 0, 0.0),
            'item ID': 'solo_L1_rpw-bia-current_20200301-20200331',
        },
    )

    test(
        'solo_L0_epd-epthet2-ll_0699408000-0699494399_V02.bin', {
            'DATASET_ID': 'SOLO_L0_EPD-EPTHET2-LL',
            'time interval string': '0699408000-0699494399',
            'version string': '02',
            'time vector 1': (699408000,),
            'item ID': 'solo_L0_epd-epthet2-ll_0699408000-0699494399',
        },
    )

    # NOTE: "V03I".
    test(
        'solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025_V03I'
        '.cdf',
        {
            'DATASET_ID': 'SOLO_LL02_EPD-HET-SOUTH-RATES',
            'time interval string': '20200813T000026-20200814T000025',
            'version string': '03',
            'time vector 1': (2020, 8, 13, 0, 0, 26.0),
            'item ID': 'solo_LL02_epd-het-south-rates_20200813T000026'
                       '-20200814T000025',
        },
    )

    # NOTE: ".fits"
    # solo_L1_eui-fsi174-image_20200806T083130185_V01.fits
    # NOTE: Case needs to be supported for making
    # erikpgjohansson.solo.download_SDT_DST() more rigorous.
    test(
        'solo_L1_eui-fsi174-image_20200806T083130185_V01.fits',
        {
            'DATASET_ID': 'SOLO_L1_EUI-FSI174-IMAGE',
            'time interval string': '20200806T083130185',
            'version string': '01',
            'time vector 1': (2020, 8, 6, 8, 31, 30.185),
            'item ID': 'solo_L1_eui-fsi174-image_20200806T083130185',
        },
    )

    # Non-dataset.
    test('solo_L1_eui-fsi174-image_20200806T083130185_V01.txt', None)


def test_parse_item_ID():
    def test(itemId, expResult):
        actResult = erikpgjohansson.solo.utils.parse_item_ID(itemId)
        assert actResult == expResult

    test(
        'solo_HK_rpw-bia_20200301', {
            'DATASET_ID': 'SOLO_HK_RPW-BIA',
            'time vector 1': (2020, 3, 1, 0, 0, 0.0),
        },
    )
    test(
        'solo_L2_rpw-lfr-surv-cwf-e_20200213', {
            'DATASET_ID': 'SOLO_L2_RPW-LFR-SURV-CWF-E',
            'time vector 1': (2020, 2, 13, 0, 0, 0.0),
        },
    )
    test(
        'solo_L1_rpw-bia-sweep_20200307T053018-20200307T053330', {
            'DATASET_ID': 'SOLO_L1_RPW-BIA-SWEEP',
            'time vector 1': (2020, 3, 7, 5, 30, 18.0),
        },
    )
    test(
        'solo_L0_epd-epthet2-ll_0699408000-0699494399', {
            'DATASET_ID': 'SOLO_L0_EPD-EPTHET2-LL',
            'time vector 1': (699408000,),
        },
    )
    test(
        'solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025', {
            'DATASET_ID': 'SOLO_LL02_EPD-HET-SOUTH-RATES',
            'time vector 1': (2020, 8, 13, 0, 0, 26.0),
        },
    )

    test('abc', None)
    test(
        'solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025a',
        None,
    )


def test_parse_time_interval_str():
    def test(s, expResult):
        if inspect.isclass(expResult) and issubclass(expResult, Exception):
            expException = expResult
            with pytest.raises(expException):
                erikpgjohansson.solo.utils._parse_time_interval_str(s)
        else:
            expTv = expResult
            actTv = erikpgjohansson.solo.utils._parse_time_interval_str(s)
            assert len(actTv) == 6
            assert all(type(x) is int for x in actTv[0:5])
            assert type(actTv[5]) is float
            assert actTv == expTv

    test('20200623',                          (2020, 6, 23,  0,  0,  0.0))
    test('20200623T112233',                   (2020, 6, 23, 11, 22, 33.0))
    test('20200623T1122331',                  (2020, 6, 23, 11, 22, 33.1))
    test('20200623T11223312',                 (2020, 6, 23, 11, 22, 33.12))
    test('20200623T112233123',                (2020, 6, 23, 11, 22, 33.123))
    test('20200623T1122331234',               Exception)
    test('20200623-20200624',                 (2020, 6, 23,  0,  0,  0.0))
    test('20200623T112233-20200624T112233',   (2020, 6, 23, 11, 22, 33.0))
    test('20200623T1122331-20200624T1122331', Exception)


def test_parse_DATASET_ID():

    def test(datasetId, expResult):
        actResult = erikpgjohansson.solo.utils.parse_DATASET_ID(datasetId)
        assert actResult == expResult

    test(
        'SOLO_L2_RPW-LFR-SBM2-CWF-E',
        ('SOLO', 'L2', 'RPW', 'RPW-LFR-SBM2-CWF-E'),
    )
    test(
        'SOLO_LL02_EPD-HET-SOUTH-RATES',
        ('SOLO', 'LL02', 'EPD', 'EPD-HET-SOUTH-RATES'),
    )
    test(
        'SOLO_LL02_MAG',
        ('SOLO', 'LL02', 'MAG', 'MAG'),
    )

    with pytest.raises(Exception):
        erikpgjohansson.solo.utils.parse_DATASET_ID(
            'SOLO_L2_RPW-LFR-SBM2-CWF-E-CDAG',
        )
    with pytest.raises(Exception):
        erikpgjohansson.solo.utils.parse_DATASET_ID(
            'solo_l2_rpw-lfr-sbm2-cwf-e',
        )


if __name__ == '__main__':
    test_parse_dataset_filename()
    test_parse_item_ID()
    test_parse_time_interval_str()
    test_parse_DATASET_ID()
