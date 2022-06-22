import erikpgjohansson.solo.soar.soar
import os.path
import pytest


def test_download_latest_dataset(tmp_path_factory):
    '''
    NOTE: Downloads from internet. Assumes that certain files are available
    online at SOAR.
    '''
    def test(dataItemId):
        '''
        IMPLEMENTATION NOTE: The test deliberately does NOT accept arguments
        expectedFileName and expectedFileSize since they vary with dataset
        version, i.e. are more likely to vary with time.
        '''
        fileParentPath = tmp_path_factory.mktemp('test', numbered=True)

        print(f'Downloading online data from SOAR: dataItemId={dataItemId}')
        actFilePath = erikpgjohansson.solo.soar.soar.download_latest_dataset(
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


    # Does not work since code can not yet handle LL.
    if 0:
        test('solo_LL02_mag_20220621T000205-20220622T000204')
        test('solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025')


# if __name__ == '__main__':
#     tpf = pytest.TempPathFactory()
#     test_download_latest_dataset()
#     pass
