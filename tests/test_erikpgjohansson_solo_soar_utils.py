import datetime
import erikpgjohansson.solo.soar.dst
import erikpgjohansson.solo.soar.tests as tests
import erikpgjohansson.solo.soar.utils as utils
import numpy as np


'''
Split test_download_latest_datasets_batch() into multiple test functions.
'''


def test_download_latest_datasets_batch(tmp_path):
    dp = tests.DirProducer(tmp_path)

    def download_latest_datasets_batch(
        use_parallel_version, *args, **kwargs,
    ):
        if use_parallel_version:
            utils.download_latest_datasets_batch(*args, **kwargs)
        else:
            utils.download_latest_datasets_batch2(*args, **kwargs)

    def test0(use_parallel_version):
        test_dir = dp.get_new_dir()
        sodl = tests.SoarDownloaderTest(dc_json_dc={})
        download_latest_datasets_batch(
            use_parallel_version,
            sodl,
            na_item_id=np.array([], object),
            na_file_size=np.array([], 'int64'),
            outputDirPath=test_dir,
        )
        tests.assert_FS(test_dir, {})

    def test1(use_parallel_version):
        test_dir = dp.get_new_dir()
        sodl = tests.SoarDownloaderTest(
            dc_json_data_ls={
                'MAG': [
                    [
                        "2022-09-20T15:18:18.556", "2022-03-27T00:00:00.0",
                        "SCI",
                        "solo_L2_mag-rtn-normal_20220327_V01.cdf", 100000,
                        "MAG",
                        "solo_L2_mag-rtn-normal_20220327", "V01", "L2",
                    ],
                    [
                        "2020-09-23T13:30:07.018", "2020-08-04T00:00:25.0",
                        "LL",
                        "solo_LL02_mag"
                        "_20200804T000025-20200805T000024_V02I.cdf", 200000,
                        "MAG", "solo_LL02_mag_20200804T000025-20200805T000024",
                        "V02", "LL02",
                    ],
                    # NOTE: Duplicate item ID (different versions).
                    [
                        "2020-09-23T13:30:07.018", "2020-08-04T00:00:25.0",
                        "LL",
                        "solo_LL02_mag"
                        "_20200804T000025-20200805T000024_V01I.cdf", 300000,
                        "MAG", "solo_LL02_mag_20200804T000025-20200805T000024",
                        "V01", "LL02",
                    ],
                ],
            },
            # IMPLEMENTATION NOTE: Setting non-zero delays is mostly useful
            # when manually inspecting the logs. Can be set to zero otherwise.
            dc_item_id_delay={
                'solo_L2_mag-rtn-normal_20220327': 0.0,
                'solo_LL02_mag_20200804T000025-20200805T000024': 0.0,
            },
        )
        download_latest_datasets_batch(
            use_parallel_version,
            sodl,
            na_item_id=np.array(
                [
                    'solo_L2_mag-rtn-normal_20220327',
                    'solo_LL02_mag_20200804T000025-20200805T000024',
                ], object,
            ),
            na_file_size=np.array([100000, 200000], 'int64'),
            outputDirPath=test_dir,
        )
        tests.assert_FS(
            test_dir, {
                'solo_L2_mag-rtn-normal_20220327_V01.cdf':
                    100000,
                'solo_LL02_mag_20200804T000025-20200805T000024_V02I.cdf':
                    200000,
            },
        )

    for use_parallel_version in (False, True):
        test0(use_parallel_version)
        test1(use_parallel_version)


def test_download_latest_datasets_batch_log_progress():
    '''Only checking if crashing. Also for manual inspection of log
    messages.
    '''
    # IMPLEMENTATION NOTE: Want to use the current time (minus some), since
    # _download_latest_datasets_batch_log_progress() uses the current time
    # to estimate remaining time.
    dt_begin = datetime.datetime.now() - datetime.timedelta(seconds=10)
    MB = 2**20

    # Generic call.
    utils._download_latest_datasets_batch_log_progress(
        10, 1, MB, 0.1*MB, dt_begin,
    )

    # Zero progress. Leads to divide-by-zero internally.
    utils._download_latest_datasets_batch_log_progress(
        10, 0, MB, 0*MB, dt_begin,
    )


def test_find_latest_versions():

    def test(ls_item_id, itemVerNbrArray, exp_bLvArray):
        expResult = np.array(exp_bLvArray, dtype=bool)

        actResult = utils.find_latest_versions(
            np.array(ls_item_id,      dtype=object),
            np.array(itemVerNbrArray, dtype=int),
        )
        assert expResult.dtype == actResult.dtype
        assert expResult.shape == actResult.shape
        assert (expResult == actResult).all()
        np.testing.assert_array_equal(expResult, actResult)

    test([], [], [])
    test(['A'], [1], [1])
    test(['A', 'B'], [1, 1], [1, 1])
    test(
        ['A', 'A'], [1, 2],
        [0, 1],
    )
    test(
        ['A', 'B', 'A', 'C', 'B'], [1, 2, 3, 5, 4],
        [0, 0, 1, 1, 1],
    )
    test(
        ['C', 'B', 'A'], [1, 2, 3],
        [1, 1, 1],
    )


def test_log_DST():
    '''Test if crashes, and for manually inspecting the log output.'''
    def test(ls_file_size, ls_begin_time_fn, ls_instrument, processing_level):
        dc = {
            'file_size':
                np.array(ls_file_size, dtype='int64'),
            'begin_time_FN':
                np.array(ls_begin_time_fn, dtype='datetime64[ms]'),
            'instrument':
                np.array(ls_instrument, dtype=object),
            'processing_level':
                np.array(processing_level, dtype=object),
        }
        dst = erikpgjohansson.solo.soar.dst.DatasetsTable(dc)
        utils.log_DST(dst, '<title string>')

    DT64_NAT = np.datetime64('NaT')
    DT64_1 = np.datetime64('2020-01-01T00:00:00')
    DT64_2 = np.datetime64('2021-01-01T00:00:00')

    test([], [], [], [])
    test([1e6], [DT64_1], ['MAG'], ['L1'])
    test([1e6], [DT64_NAT], ['MAG'], ['L1'])
    test(
        [1e6, 10e6, 100e6, 1000e9],
        [DT64_1, DT64_1, DT64_2, DT64_NAT],
        ['MAG', 'EPD', 'EPD', 'EPD'],
        ['L1', 'L2', 'L2', 'L2'],
    )
