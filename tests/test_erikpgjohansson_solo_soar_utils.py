import erikpgjohansson.solo.soar.tests as tests
import erikpgjohansson.solo.soar.utils
import logging
import numpy as np
import sys
import tempfile


def test_download_latest_datasets_batch(tmp_path):
    dp = tests.DirProducer(tmp_path)

    def download_latest_datasets_batch(
        use_parallel_version, *args, **kwargs,
    ):
        if use_parallel_version:
            erikpgjohansson.solo.soar.utils.download_latest_datasets_batch(
                *args, **kwargs,
            )
        else:
            erikpgjohansson.solo.soar.utils.download_latest_datasets_batch2(
                *args, **kwargs,
            )

    def test0(use_parallel_version):
        test_dir = dp.get_new_dir()
        dl = tests.MockDownloader(dc_json_dc={})
        download_latest_datasets_batch(
            use_parallel_version,
            dl,
            itemIdArray=np.array([], object),
            fileSizeArray=np.array([], 'int64'),
            outputDirPath=test_dir,
            logFormat='long',
        )
        tests.assert_FS(test_dir, {})

    def test1(use_parallel_version):
        test_dir = dp.get_new_dir()
        dl = tests.MockDownloader(
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
            dl,
            itemIdArray=np.array(
                [
                    'solo_L2_mag-rtn-normal_20220327',
                    'solo_LL02_mag_20200804T000025-20200805T000024',
                ], object,
            ),
            fileSizeArray=np.array([100000, 200000], 'int64'),
            outputDirPath=test_dir,
            logFormat='long',
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

    # test1(True)


def test_find_latest_versions():

    def test(itemIdArray, itemVerNbrArray, exp_bLvArray):
        expResult = np.array(exp_bLvArray, dtype=bool)

        actResult = erikpgjohansson.solo.soar.utils.find_latest_versions(
            np.array(itemIdArray,     dtype=object),
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


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    t = tempfile.TemporaryDirectory()
    test_download_latest_datasets_batch(t.name)

    test_find_latest_versions()
