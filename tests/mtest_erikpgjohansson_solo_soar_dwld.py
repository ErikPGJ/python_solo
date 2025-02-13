'''
*Manual* test code for erikpgjohansson.solo.soar.dwld.

It is useful to *not* do this automatically with other tests since it
downloads data from SOAR (over the internet) which in turn:
* slows down tests,
* may increase the risk of having one's IP blacklisted by SOAR.
'''


import erikpgjohansson.solo.soar.const
import erikpgjohansson.solo.soar.dwld
import os.path
import pathlib
import tempfile


'''
PROPOSAL: Refactor all "mtests" into regular pytest tests, but have switch for
          enabling/disabling them.
    PROPOSAL: Global switch.
        TODO-DEC: Global wrt. what? git repo? erikpgjohansson.solo.soar?
    PROPOSAL: Constant in erikpgjohansson.solo.soar.const.
'''


def mtest_SoarDownloaderImpl_download_SDT_DST():
    sodl = erikpgjohansson.solo.soar.dwld.SoarDownloaderImpl()

    for instrument in erikpgjohansson.solo.soar.const.LS_SOAR_INSTRUMENTS:
        _ = sodl.download_JSON_SDT(instrument)


def mtest_SoarDownloaderImpl_download_latest_dataset(tmp_path):
    '''
    NOTE: Downloads from internet. Assumes that certain files are available
          online at SOAR. If these files are missing from SOAR, then the test
          fails even if the code is OK.
    '''
    i_test = 0

    # Normalize. Can be string if called from non-pytest.
    tmp_path = pathlib.Path(tmp_path)

    sodl = erikpgjohansson.solo.soar.dwld.SoarDownloaderImpl()

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
            f'Downloading online data from SOAR: dataItemId={dataItemId}',
        )
        actFilePath = sodl.download_latest_dataset(
            dataItemId, dirPath,
            expectedFileName=None, expectedFileSize=None,
        )
        assert os.path.isfile(actFilePath)

    test('solo_L1_epd-sis-a-rates-slow_20200813')    # 11037 bytes
    test('solo_L1_epd-step-nom-close_20200813')      # 65536 bytes

    # Test LL
    # test('solo_LL02_mag_20220621T000205-20220622T000204') # No longer works.
    test('solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025')


if __name__ == '__main__':
    if 1:
        mtest_SoarDownloaderImpl_download_SDT_DST()
    if 1:
        t = tempfile.TemporaryDirectory()
        mtest_SoarDownloaderImpl_download_latest_dataset(t.name)
