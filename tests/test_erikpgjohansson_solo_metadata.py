import erikpgjohansson.solo.metadata
import inspect
import pytest


def test_DatasetFilename():

    def test(filename, expDsfn):
        actDsfn = \
            erikpgjohansson.solo.metadata.DatasetFilename.parse_filename(
                filename,
            )
        assert actDsfn == expDsfn

        if actDsfn is not None:
            # ==============================================================
            # Assert consistency between
            # erikpgjohansson.solo.metadata.DatasetFilename.parse_filename()
            # and erikpgjohansson.solo.metadata.parse_item_ID()
            # ==============================================================
            # II = Item ID
            actIiResult = erikpgjohansson.solo.metadata.parse_item_ID(
                actDsfn.itemId,
            )
            assert actIiResult['DSID']          == actDsfn.dsid
            assert actIiResult['time vector 1'] == actDsfn.timeVector1

    test(
        'solo_HK_rpw-bia_20200301_V01.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_HK_RPW-BIA',
            timeIntervalStr='20200301',
            versionStr='01',
            timeVector1=(2020, 3, 1, 0, 0, 0.0),
            itemId='solo_HK_rpw-bia_20200301',
        ),
    )

    test(
        'solo_L2_rpw-lfr-surv-cwf-e-cdag_20200213_V02.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L2_RPW-LFR-SURV-CWF-E',
            timeIntervalStr='20200213',
            versionStr='02',
            timeVector1=(2020, 2, 13, 0, 0, 0.0),
            itemId='solo_L2_rpw-lfr-surv-cwf-e_20200213',
        ),
    )   # CDAG

    test(
        'solo_L1_rpw-bia-sweep-cdag_20200307T053018-20200307T053330_V01.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L1_RPW-BIA-SWEEP',
            timeIntervalStr='20200307T053018-20200307T053330',
            versionStr='01',
            timeVector1=(2020, 3, 7, 5, 30, 18.0),
            itemId='solo_L1_rpw-bia-sweep_20200307T053018-20200307T053330',
        ),
    )

    test(
        'solo_L1_rpw-bia-current-cdag_20200301-20200331_V01.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L1_RPW-BIA-CURRENT',
            timeIntervalStr='20200301-20200331',
            versionStr='01',
            timeVector1=(2020, 3, 1, 0, 0, 0.0),
            itemId='solo_L1_rpw-bia-current_20200301-20200331',
        ),
    )

    test(
        'solo_L0_epd-epthet2-ll_0699408000-0699494399_V02.bin',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L0_EPD-EPTHET2-LL',
            timeIntervalStr='0699408000-0699494399',
            versionStr='02',
            timeVector1=(699408000,),
            itemId='solo_L0_epd-epthet2-ll_0699408000-0699494399',
        ),
    )

    # NOTE: "V03I".
    test(
        'solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025_V03I'
        '.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_LL02_EPD-HET-SOUTH-RATES',
            timeIntervalStr='20200813T000026-20200814T000025',
            versionStr='03',
            timeVector1=(2020, 8, 13, 0, 0, 26.0),
            itemId='solo_LL02_epd-het-south-rates_20200813T000026'
                       '-20200814T000025',
        ),
    )

    # NOTE: ".fits"
    # solo_L1_eui-fsi174-image_20200806T083130185_V01.fits
    # NOTE: Case needs to be supported for making
    # erikpgjohansson.solo.download_SDT_DST() more rigorous.
    test(
        'solo_L1_eui-fsi174-image_20200806T083130185_V01.fits',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L1_EUI-FSI174-IMAGE',
            timeIntervalStr='20200806T083130185',
            versionStr='01',
            timeVector1=(2020, 8, 6, 8, 31, 30.185),
            itemId='solo_L1_eui-fsi174-image_20200806T083130185',
        ),
    )

    # Non-dataset.
    test('solo_L1_eui-fsi174-image_20200806T083130185_V01.txt', None)


def test_parse_item_ID():
    def test(itemId, expResult):
        actResult = erikpgjohansson.solo.metadata.parse_item_ID(itemId)
        assert actResult == expResult

    test(
        'solo_HK_rpw-bia_20200301', {
            'DSID': 'SOLO_HK_RPW-BIA',
            'time vector 1': (2020, 3, 1, 0, 0, 0.0),
        },
    )
    test(
        'solo_L2_rpw-lfr-surv-cwf-e_20200213', {
            'DSID': 'SOLO_L2_RPW-LFR-SURV-CWF-E',
            'time vector 1': (2020, 2, 13, 0, 0, 0.0),
        },
    )
    test(
        'solo_L1_rpw-bia-sweep_20200307T053018-20200307T053330', {
            'DSID': 'SOLO_L1_RPW-BIA-SWEEP',
            'time vector 1': (2020, 3, 7, 5, 30, 18.0),
        },
    )
    test(
        'solo_L0_epd-epthet2-ll_0699408000-0699494399', {
            'DSID': 'SOLO_L0_EPD-EPTHET2-LL',
            'time vector 1': (699408000,),
        },
    )
    test(
        'solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025', {
            'DSID': 'SOLO_LL02_EPD-HET-SOUTH-RATES',
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
                erikpgjohansson.solo.metadata._parse_time_interval_str(s)
        else:
            expTv = expResult
            actTv = erikpgjohansson.solo.metadata._parse_time_interval_str(s)
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


def test_parse_DSID():

    def test(dsid, expResult):
        actResult = erikpgjohansson.solo.metadata.parse_DSID(dsid)
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
        erikpgjohansson.solo.metadata.parse_DSID(
            'SOLO_L2_RPW-LFR-SBM2-CWF-E-CDAG',
        )
    with pytest.raises(Exception):
        erikpgjohansson.solo.metadata.parse_DSID(
            'solo_l2_rpw-lfr-sbm2-cwf-e',
        )


if __name__ == '__main__':
    test_DatasetFilename()
    test_parse_item_ID()
    test_parse_time_interval_str()
    test_parse_DSID()
