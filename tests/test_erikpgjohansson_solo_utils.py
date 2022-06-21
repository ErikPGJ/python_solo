import erikpgjohansson.solo.utils
import pytest



def test_parse_dataset_filename():

    def test(filename, expResult):
        actResult = erikpgjohansson.solo.utils.parse_dataset_filename(filename)
        assert actResult == expResult

    test(
        'solo_HK_rpw-bia_20200301_V01.cdf',
        {
            'DATASET_ID':'SOLO_HK_RPW-BIA',
            'time interval string':'20200301',
            'version string':'01',
            'time vector 1':(2020, 3, 1, 0, 0, 0.0),
            'item ID':'solo_HK_rpw-bia_20200301',
        },
    )

    test(
        'solo_L2_rpw-lfr-surv-cwf-e-cdag_20200213_V02.cdf',
        {
            'DATASET_ID':'SOLO_L2_RPW-LFR-SURV-CWF-E',
            'time interval string':'20200213',
            'version string':'02',
            'time vector 1':(2020, 2, 13, 0, 0, 0.0),
            'item ID':'solo_L2_rpw-lfr-surv-cwf-e_20200213',
        },
    )   # CDAG

    test(
        'solo_L1_rpw-bia-sweep-cdag_20200307T053018-20200307T053330_V01.cdf', {
        'DATASET_ID':'SOLO_L1_RPW-BIA-SWEEP',
        'time interval string':'20200307T053018-20200307T053330',
        'version string':'01',
        'time vector 1':(2020, 3, 7, 5, 30, 18.0),
        'item ID':'solo_L1_rpw-bia-sweep_20200307T053018-20200307T053330',
        },
    )

    test(
        'solo_L1_rpw-bia-current-cdag_20200301-20200331_V01.cdf', {
        'DATASET_ID':'SOLO_L1_RPW-BIA-CURRENT',
        'time interval string':'20200301-20200331',
        'version string':'01',
        'time vector 1':(2020, 3, 1, 0, 0, 0.0),
        'item ID':'solo_L1_rpw-bia-current_20200301-20200331',
        },
    )

    # NOTE: "V03I".
    test(
        'solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025_V03I.cdf', {
        'DATASET_ID':'SOLO_LL02_EPD-HET-SOUTH-RATES',
        'time interval string':'20200813T000026-20200814T000025',
        'version string':'03',
        'time vector 1':(2020, 8, 13, 0, 0, 26.0),
        'item ID':'solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025',
        },
    )

    # NOTE: ".fits"
    # solo_L1_eui-fsi174-image_20200806T083130185_V01.fits
    # NOTE: Case needs to be supported for making
    # erikpgjohansson.solo.download_SOAR_DST() more rigorous.
    test(
        'solo_L1_eui-fsi174-image_20200806T083130185_V01.fits', {
        'DATASET_ID':'SOLO_L1_EUI-FSI174-IMAGE',
        'time interval string':'20200806T083130185',
        'version string':'01',
        'time vector 1':(2020, 8, 6, 8, 31, 30.185),
        'item ID':'solo_L1_eui-fsi174-image_20200806T083130185',
        },
    )

    # Non-dataset.
    test('solo_L1_eui-fsi174-image_20200806T083130185_V01.txt', None)






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
    with pytest.raises(Exception):
        erikpgjohansson.solo.utils.parse_DATASET_ID(
            'SOLO_L2_RPW-LFR-SBM2-CWF-E-CDAG',
        )
    with pytest.raises(Exception):
        erikpgjohansson.solo.utils.parse_DATASET_ID(
            'solo_l2_rpw-lfr-sbm2-cwf-e',
        )



if __name__ == '__main__':
    test_parse_DATASET_ID()
    test_parse_dataset_filename()
    pass
