import erikpgjohansson.solo.metadata
import pytest


def test_DatasetFilename___repr():
    dsfn = erikpgjohansson.solo.metadata.DatasetFilename(
        dsid='SOLO_L3_EPD-EPT-1DAY',
        time_interval_str='2024',
        version_str='11',
        timeVector1=(2024, 1, 1, 0, 0, 0.0),
        itemId='solo_L3_epd-ept-1day_2024',
    )

    act_str = dsfn.__repr__()
    assert type(act_str) is str


def test_DatasetFilename():

    def test(filename, exp_dsfn):
        act_dsfn = \
            erikpgjohansson.solo.metadata.DatasetFilename.parse_filename(
                filename,
            )
        assert act_dsfn == exp_dsfn

        if act_dsfn is not None:
            # ==============================================================
            # Assert consistency between
            # erikpgjohansson.solo.metadata.DatasetFilename.parse_filename()
            # and erikpgjohansson.solo.metadata.parse_item_ID()
            # ==============================================================
            # II = Item ID
            actIiResult = erikpgjohansson.solo.metadata.parse_item_ID(
                act_dsfn.itemId,
            )
            assert actIiResult['DSID']          == act_dsfn.dsid
            assert actIiResult['time vector 1'] == act_dsfn.timeVector1

    test(
        'solo_L3_epd-ept-1day_2024_V11.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L3_EPD-EPT-1DAY',
            time_interval_str='2024',
            version_str='11',
            timeVector1=(2024, 1, 1, 0, 0, 0.0),
            itemId='solo_L3_epd-ept-1day_2024',
        ),
    )

    test(
        'solo_L3_epd-ept-1hour_202301_V01.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L3_EPD-EPT-1HOUR',
            time_interval_str='202301',
            version_str='01',
            timeVector1=(2023, 1, 1, 0, 0, 0.0),
            itemId='solo_L3_epd-ept-1hour_202301',
        ),
    )

    test(
        'solo_HK_rpw-bia_20200301_V01.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_HK_RPW-BIA',
            time_interval_str='20200301',
            version_str='01',
            timeVector1=(2020, 3, 1, 0, 0, 0.0),
            itemId='solo_HK_rpw-bia_20200301',
        ),
    )

    test(
        'solo_L2_rpw-lfr-surv-cwf-e-cdag_20200213_V02.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L2_RPW-LFR-SURV-CWF-E',
            time_interval_str='20200213',
            version_str='02',
            timeVector1=(2020, 2, 13, 0, 0, 0.0),
            itemId='solo_L2_rpw-lfr-surv-cwf-e_20200213',
        ),
    )   # CDAG

    test(
        'solo_L1_rpw-bia-sweep-cdag_20200307T053018-20200307T053330_V01.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L1_RPW-BIA-SWEEP',
            time_interval_str='20200307T053018-20200307T053330',
            version_str='01',
            timeVector1=(2020, 3, 7, 5, 30, 18.0),
            itemId='solo_L1_rpw-bia-sweep_20200307T053018-20200307T053330',
        ),
    )

    test(
        'solo_L1_rpw-bia-current-cdag_20200301-20200331_V01.cdf',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L1_RPW-BIA-CURRENT',
            time_interval_str='20200301-20200331',
            version_str='01',
            timeVector1=(2020, 3, 1, 0, 0, 0.0),
            itemId='solo_L1_rpw-bia-current_20200301-20200331',
        ),
    )

    test(
        'solo_L0_epd-epthet2-ll_0699408000-0699494399_V02.bin',
        erikpgjohansson.solo.metadata.DatasetFilename(
            dsid='SOLO_L0_EPD-EPTHET2-LL',
            time_interval_str='0699408000-0699494399',
            version_str='02',
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
            time_interval_str='20200813T000026-20200814T000025',
            version_str='03',
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
            time_interval_str='20200806T083130185',
            version_str='01',
            timeVector1=(2020, 8, 6, 8, 31, 30.185),
            itemId='solo_L1_eui-fsi174-image_20200806T083130185',
        ),
    )

    # Non-dataset.
    test('solo_L1_eui-fsi174-image_20200806T083130185_V01.txt', None)


def test_parse_item_ID():
    def test(item_id, exp_rv):
        act_rv = erikpgjohansson.solo.metadata.parse_item_ID(item_id)
        assert act_rv == exp_rv

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
    def test(s, exp_tv):
        if type(exp_tv) is tuple and len(exp_tv) == 6:
            assert all(type(x) is int for x in exp_tv[0:5])
            assert type(exp_tv[5]) is float
        if type(exp_tv) is tuple and len(exp_tv) == 1:
            assert type(exp_tv[0]) is int

        act_tv = erikpgjohansson.solo.metadata._parse_time_interval_str(s)

        assert act_tv == exp_tv

    test('20200623',                          (2020, 6, 23,  0,  0,  0.0))
    test('20200623T112233',                   (2020, 6, 23, 11, 22, 33.0))
    test('20200623T1122331',                  (2020, 6, 23, 11, 22, 33.1))
    test('20200623T11223312',                 (2020, 6, 23, 11, 22, 33.12))
    test('20200623T112233123',                (2020, 6, 23, 11, 22, 33.123))
    test('20200623T1122331234',               None)
    test('20200623-20200624',                 (2020, 6, 23,  0,  0,  0.0))
    test('20200623T112233-20200624T112233',   (2020, 6, 23, 11, 22, 33.0))
    test('20200623T1122331-20200624T1122331', None)
    test('0000000000-0000086399',             (0,))
    test('0000000003-0000086399',             (3,))


def test_parse_DSID():

    def test(dsid, exp_rv):
        act_rv = erikpgjohansson.solo.metadata.parse_DSID(dsid)
        assert act_rv == exp_rv

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
