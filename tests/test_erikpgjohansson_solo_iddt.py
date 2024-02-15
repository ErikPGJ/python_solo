import erikpgjohansson.solo.iddt
import pytest


def test_get_IDDT_subdir():

    def test(filename, kwargs, expResult):
        actResult = erikpgjohansson.solo.iddt.get_IDDT_subdir(
            filename, **kwargs,
        )
        assert actResult == expResult

    test(
        'SOLO_L2_RPW-LFR-SBM2-CWF-E_20200401_V01.cdf',
        {}, 'rpw/L2/lfr_sbm2_wf_e/2020/04',
    )
    test(
        'solo_L2_rpw-lfr-surv-bp1_20201001_V02.cdf',
        {}, 'rpw/L2/lfr_bp/2020/10',
    )
    test(
        'solo_L2_rpw-lfr-surv-bp1-cdag_20201001_V02.cdf',
        {}, 'rpw/L2/lfr_bp/2020/10',
    )

    test(
        'solo_L2_mag-rtn-normal-1-minute_20200601_V02.cdf',
        {'dtdnInclInstrument': False},
        'mag/L2/rtn-normal-1-minute/2020/06',
    )
    test(
        'solo_L2_mag-rtn-normal-1-minute_20200601_V02.cdf',
        {'dtdnInclInstrument': True},
        'mag/L2/mag-rtn-normal-1-minute/2020/06',
    )

    test(
        'solo_L2_epd-step-burst_20200703T232228-20200703T233728_V02.cdf',
        {}, 'epd/L2/epd-step-burst/2020/07',
    )

    test(
        'solo_L2_swa-eas1-nm3d-psd_20200708T060012-20200708T120502_V01.cdf',
        {}, 'swa/L2/swa-eas1-nm3d-psd/2020/07',
    )
    test(
        'solo_L1_swa-eas-OnbPartMoms_20200820T000000-20200820T235904_V01.cdf',
        {}, 'swa/L1/2020/08/20',
    )

    test(
        'solo_LL02_epd-ept-north-rates_20200813T000026-20200814T000025_V03I'
        '.cdf',
        {}, 'epd/LL02/2020/08/13',
    )

    # Non-datasets (according to filename).
    test('solo_L2_rpw-lfr-surv-bp1-cdag_20201001_V02.CDF', {}, None)
    test('SOLO_L2_RPW-LFR-SBM2-CWF-E-CDAG',                {}, None)

    # Datasets (according to filename) that can not be handled.
    with pytest.raises(Exception):
        erikpgjohansson.solo.iddt.get_IDDT_subdir(
            'solo_HK_rpw-bia_20201209_V01.cdf',
        )


def test_convert_DATASET_ID_to_DTDN():

    def test(datasetId, kwargs, expResult):
        actResult = erikpgjohansson.solo.iddt.convert_DATASET_ID_to_DTDN(
            datasetId, **kwargs,
        )
        assert actResult == expResult

    def test_exc(datasetId, kwargs):
        with pytest.raises(Exception):
            erikpgjohansson.solo.iddt.convert_DATASET_ID_to_DTDN(
                datasetId, **kwargs,
            )

    test('SOLO_L2_RPW-LFR-SBM2-CWF-E',          {}, 'lfr_sbm2_wf_e')
    test('SOLO_L2_RPW-LFR-SURV-CWF-E',          {}, 'lfr_wf_e')
    test('SOLO_L2_RPW-LFR-SURV-CWF-E-1-SECOND', {}, 'lfr_wf_e')
    test('SOLO_L2_MAG-SRF-BURST',               {}, 'srf-burst')
    test('SOLO_L2_MAG-RTN-NORMAL-1-MINUTE',     {}, 'rtn-normal-1-minute')

    test('SOLO_L3_RPW-BIA-DENSITY-10-SECONDS',  {}, 'lfr_density')
    test('SOLO_L3_RPW-BIA-SCPOT-10-SECONDS',    {}, 'lfr_scpot')
    test('SOLO_L3_RPW-BIA-EFIELD-10-SECONDS',   {}, 'lfr_efield')
    test('SOLO_L3_RPW-TNR-FP',                  {}, 'tnr_fp')

    ####################
    # includeInstrument
    ####################
    # RPW
    for includeInstrument in [False, True]:
        test(
            'SOLO_L3_RPW-BIA-EFIELD', {'includeInstrument': includeInstrument},
            'lfr_efield',
        )

    # Non-RPW
    test(
        'SOLO_L2_EPD-HET-SUN-RATES', {'includeInstrument': False},
        'het-sun-rates',
    )
    test(
        'SOLO_L2_EPD-HET-SUN-RATES', {'includeInstrument': True},
        'epd-het-sun-rates',
    )

    #############
    # Exceptions
    #############
    test_exc('SOLO_L1_EPD-SIS-B-HEHIST', {})
    test_exc('SOLO_L2_RPW-LFR-SBM2-CWF-E-CDAG', {})
    test_exc('solo_l2_rpw-lfr-sbm2-cwf-e', {})


if __name__ == '__main__':
    test_get_IDDT_subdir()
    test_convert_DATASET_ID_to_DTDN()
