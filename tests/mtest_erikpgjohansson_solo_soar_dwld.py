'''Manual test code for
erikpgjohansson.solo.soar.dwld.SoarDownloader.download_latest_dataset().

It is useful to *not* do this automatically with other tests since it
downloads data from SOAR (over the internet) which:
* slows down tests,
* may increase the risk of having one's IP blacklisted by SOAR.
'''


import erikpgjohansson.solo.soar.dwld
import os.path
import pathlib
import tempfile


def mtest_SoarDownloader_download_latest_dataset(tmp_path):
    '''
    NOTE: Downloads from internet. Assumes that certain files are available
          online at SOAR. If these files are missing from SOAR, then the test
          fails even if the code is OK.
    '''
    i_test = 0

    # Normalize. Can be string if called from non-pytest.
    tmp_path = pathlib.Path(tmp_path)

    downloader = erikpgjohansson.solo.soar.dwld.SoarDownloader()

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
            f'ATEST: Downloading online data from SOAR:'
            f' dataItemId={dataItemId}',
        )
        actFilePath = downloader.download_latest_dataset(
            dataItemId, dirPath,
            expectedFileName=None, expectedFileSize=None,
        )
        assert os.path.isfile(actFilePath)

    test('solo_L1_epd-sis-a-rates-slow_20200813')
    test('solo_L1_epd-step-nom-close_20200813')

    # Test LL
    test('solo_LL02_mag_20220621T000205-20220622T000204')
    test('solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025')


if __name__ == '__main__':
    t = tempfile.TemporaryDirectory()
    mtest_SoarDownloader_download_latest_dataset(t.name)
