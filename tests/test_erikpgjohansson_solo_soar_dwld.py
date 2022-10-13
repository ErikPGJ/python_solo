import erikpgjohansson.solo.soar.dwld
import os.path
import pathlib
import tempfile


def test_download_latest_dataset(tmp_path):
    '''
    NOTE: Downloads from internet. Assumes that certain files are available
    online at SOAR. If these files are missing from SOAR, then the test
    fails even if the code is OK.
    '''
    i_test = 0

    # Normalize. Can be string if called from non-pytest.
    tmp_path = pathlib.Path(tmp_path)

    def test(dataItemId):
        '''
        IMPLEMENTATION NOTE: The test deliberately does NOT accept arguments
        expectedFileName and expectedFileSize since they vary with dataset
        version, i.e. are more likely to vary with time.
        '''
        nonlocal i_test
        fileParentPath = pathlib.Path(tmp_path) / f'test_{i_test}'
        i_test = i_test + 1
        os.mkdir(fileParentPath)

        print(f'Downloading online data from SOAR: dataItemId={dataItemId}')
        actFilePath = erikpgjohansson.solo.soar.dwld.download_latest_dataset(
            dataItemId, fileParentPath,
            expectedFileName=None, expectedFileSize=None,
            debugCreateEmptyFile=False,
        )
        assert os.path.isfile(actFilePath)

    # item_id
    # solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025
    # soarSubsetLvDst['item_id']
    # array(['solo_L1_epd-sis-a-rates-slow_20200813',
    #        'solo_L1_epd-sis-a-rates-medium_20200813',
    #        'solo_L1_epd-epthet2-quicklook_20200813',
    #        'solo_L1_epd-sis-b-hehist_20200813',
    #        'solo_L1_epd-sis-b-rates-medium_20200813',
    #        'solo_L1_epd-epthet2-sc_20200813',
    #        'solo_L1_epd-step-quicklook_20200813',
    #        'solo_L1_epd-step-nom-close_20200813'], dtype=object)
    # soarSubsetLvDst['file_name']
    # array(['solo_L1_epd-sis-a-rates-slow_20200813_V02.cdf',
    #        'solo_L1_epd-sis-a-rates-medium_20200813_V01.cdf',
    #        'solo_L1_epd-epthet2-quicklook_20200813_V02.cdf',
    #        'solo_L1_epd-sis-b-hehist_20200813_V02.cdf',
    #        'solo_L1_epd-sis-b-rates-medium_20200813_V01.cdf',
    #        'solo_L1_epd-epthet2-sc_20200813_V01.cdf',
    #        'solo_L1_epd-step-quicklook_20200813_V02.cdf',
    #        'solo_L1_epd-step-nom-close_20200813_V01.cdf'], dtype=object)

    test('solo_L1_epd-sis-a-rates-slow_20200813')
    test('solo_L1_epd-step-nom-close_20200813')

    # Test LL
    test('solo_LL02_mag_20220621T000205-20220622T000204')
    test('solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025')


if __name__ == '__main__':
    t = tempfile.TemporaryDirectory()
    test_download_latest_dataset(t.name)
    pass
