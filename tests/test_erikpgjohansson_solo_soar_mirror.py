'''
DATA STRUCTURE FOR REPRESENTING A DIRECTORY TREE
================================================
Tests use a simple data structure for representing directory trees that is
suitable for hardcoding. It includes files within it (excluding file
content, but including file sizes).
'''


import erikpgjohansson.solo.soar.mirror
import erikpgjohansson.solo.soar.dwld
import erikpgjohansson.solo.soar.tests as tests
import os


'''
PROPOSAL: Include optional (manually triggered) test which actually mirrors
          and downloads data from SOAR.
    CON: Can not assume exactly which datasets are available at SOAR.

PROPOSAL: Split up into multiple modules.
'''


def test_sync_0(tmp_path):
    root_dir = tmp_path
    sync_dir = os.path.join(root_dir, 'mirror')
    download_dir = os.path.join(root_dir, 'download')

    tests.setup_FS(
        root_dir, {
            'download': {},
            'mirror': {},
        },
    )

    sodl = tests.SoarDownloaderTest(
        dc_json_data_ls={
            'EPD':
                [[
                    "2020-09-23T13:47:11.73", "2020-08-13T00:00:26.0",
                    "LL", "solo_LL02_epd-het-south-rates"
                    "_20200813T000026-20200814T000025_V03I.cdf",
                    113, "EPD",
                    "solo_LL02_epd-het-south-rates"
                    "_20200813T000026-20200814T000025",
                    "V03", "LL02",
                ]],
        },
    )

    erikpgjohansson.solo.soar.mirror.sync(
        sync_dir=sync_dir,
        temp_download_dir=download_dir,
        dsss=tests.DatasetsSubsetEverything(),
        delete_outside_subset=False,
        n_max_datasets_net_remove=10,
        removal_dir=None,
        remove_removal_dir=False,
        sodl=sodl,
    )

    tests.assert_FS(
        root_dir,
        {
            'download': {},
            'mirror': {
                'epd': {
                    'LL02': {
                        '2020': {
                            '08': {
                                '13': {
                                    'solo_LL02_epd-het-south-rates'
                                    '_20200813T000026-20200814T000025'
                                    '_V03I.cdf': 113,
                                },
                            },
                        },
                    },
                },
            },
        },
    )


def test_sync_1(tmp_path):
    class DatasetsSubsetNotEui(
        erikpgjohansson.solo.soar.mirror.DatasetsSubset,
    ):
        def dataset_in_subset(self, instrument, level, begin_dt64, dsid):
            return instrument != 'EUI'

    L2_MAG_V02 = [
        "2022-04-12T16:39:03.935", "2020-07-20T00:00:00.0", "SCI",
        "solo_L2_mag-rtn-normal_20200720_V02.cdf", 108, "MAG",
        "solo_L2_mag-rtn-normal_20200720", "V02", "L2",
    ]
    L2_MAG_V03 = [
        "2022-04-12T16:39:03.935", "2020-07-20T00:00:00.0", "SCI",
        "solo_L2_mag-rtn-normal_20200720_V03.cdf", 103, "MAG",
        "solo_L2_mag-rtn-normal_20200720", "V03", "L2",
    ]
    L1_EUI = [
        "2022-05-12T14:44:29.828", "2020-08-06T00:04:00.993", "SCI",
        "solo_L1_eui-hrilya1216-image_20200806T000400993_V01.fits", 547,
        "EUI", "solo_L1_eui-hrilya1216-image_20200806T000400993", "V01",
        "L1",
    ]
    L3_BIA = [
        "2022-09-16T18:22:52.025", "2020-06-21T00:00:00.0", "SCI",
        "solo_L3_rpw-bia-efield-10-seconds_20200621_V02.cdf", 11, "RPW",
        "solo_L3_rpw-bia-efield-10-seconds_20200621", "V02", "L3",
    ]

    root_dir = tmp_path
    sync_dir = os.path.join(root_dir, 'mirror')
    download_dir = os.path.join(root_dir, 'download')
    tests.setup_FS(
        root_dir, {
            'download': {},
            'mirror': {
                'mag': {
                    'L2': {
                        'mag-rtn-normal': {
                            '2020': {
                                '07': {
                                    'solo_L2_mag-rtn-normal_20200720'
                                    '_V01.cdf': 100,
                                },
                            },
                        },
                    },
                },
                'rpw': {
                    'L3': {
                        'lfr_efield': {
                            '2020': {
                                '06': {
                                    # V01 will be removed, V02 kept.
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200621_V01.cdf': 10,
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200621_V02.cdf': 11,
                                },
                                '07': {
                                    # Will be removed since not in SOAR.
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200714_V01.cdf': 12,
                                },
                            },
                        },
                    },
                },
            },
        },
    )
    # IMPLEMENTATION NOTE: Putting some datasets in table for the wrong
    # instruments since download_SDT_DST() iterates over hardcoded
    # instrument list
    # erikpgjohansson.solo.soar.const.LS_SOAR_INSTRUMENTS.
    sodl = tests.SoarDownloaderTest(
        dc_json_data_ls={
            'MAG': [L2_MAG_V02, L2_MAG_V03],
            'EPD': [L1_EUI],
            'SWA': [L3_BIA],
        },
    )

    erikpgjohansson.solo.soar.mirror.sync(
        sync_dir=sync_dir,
        temp_download_dir=download_dir,
        dsss=DatasetsSubsetNotEui(),
        delete_outside_subset=False,
        n_max_datasets_net_remove=10,
        removal_dir=None,
        remove_removal_dir=False,
        sodl=sodl,
    )

    tests.assert_FS(
        root_dir,
        {
            'download': {},
            'mirror': {
                'mag': {
                    'L2': {
                        'mag-rtn-normal': {
                            '2020': {
                                '07': {
                                    'solo_L2_mag-rtn-normal_20200720'
                                    '_V03.cdf': 103,
                                },
                            },
                        },
                    },
                },
                'rpw': {
                    'L3': {
                        'lfr_efield': {
                            '2020': {
                                '06': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200621_V02.cdf': 11,
                                },
                                '07': {
                                    # Empty directory from removed dataset.
                                },
                            },
                        },
                    },
                },
            },
        },
    )


def test_offline_cleanup_0(tmp_path):
    # TODO: Check misplaced files.

    root_dir = tmp_path
    sync_dir = os.path.join(root_dir, 'mirror')
    download_dir = os.path.join(root_dir, 'download')
    removal_dir = os.path.join(root_dir, 'removal')

    tests.setup_FS(
        root_dir, {
            'download': {
                # V01 will be removed, V02 kept.
                # Not in mirror/.
                'solo_L3_rpw-bia-efield-10-seconds_20200601_V01.cdf': 20,
                'solo_L3_rpw-bia-efield-10-seconds_20200601_V02.cdf': 21,
                # Duplicate with mirror/.
                'solo_L3_rpw-bia-efield-10-seconds_20200714_V05.cdf': 12,
                # Later version than in mirror/.
                'solo_L3_rpw-bia-efield-10-seconds_20200801_V02.cdf': 30,
                # Older version than in mirror/.
                'solo_L3_rpw-bia-efield-10-seconds_20200901_V01.cdf': 40,
            },
            'mirror': {
                'rpw': {
                    'L3': {
                        'lfr_efield': {
                            '2020': {
                                '05': {
                                    # V01 will be removed, V02 kept.
                                    # Not in download/.
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200501_V01.cdf': 10,
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200501_V02.cdf': 11,
                                },
                                '07': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200714_V05.cdf': 12,
                                },
                                '08': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200801_V01.cdf': 30,
                                },
                                '09': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200901_V02.cdf': 40,
                                },
                            },
                        },
                    },
                },
            },
        },
    )

    erikpgjohansson.solo.soar.mirror.offline_cleanup(
        sync_dir, download_dir, tests.DatasetsSubsetEverything(),
        removal_dir=removal_dir, remove_removal_dir=True,
    )

    tests.assert_FS(
        root_dir, {
            'download': {},
            'mirror': {
                'rpw': {
                    'L3': {
                        'lfr_efield': {
                            '2020': {
                                '05': {
                                    # V01 will be removed, V02 kept.
                                    # Not in download/.
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200501_V02.cdf': 11,
                                },
                                '06': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200601_V02.cdf': 21,
                                },
                                '07': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200714_V05.cdf': 12,
                                },
                                '08': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200801_V02.cdf': 30,
                                },
                                '09': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200901_V02.cdf': 40,
                                },
                            },
                        },
                    },
                },
            },
        },
    )


def test_offline_cleanup_1(tmp_path):
    root_dir = tmp_path
    sync_dir = os.path.join(root_dir, 'mirror')
    download_dir = os.path.join(root_dir, 'download')
    removal_dir = os.path.join(root_dir, 'removal')

    tests.setup_FS(
        root_dir, {
            'download': {},
            'mirror': {
                # File in wrong location.
                'solo_L3_rpw-bia-efield-10-seconds_20200501_V01.cdf': 10,
            },
        },
    )
    erikpgjohansson.solo.soar.mirror.offline_cleanup(
        sync_dir, download_dir, tests.DatasetsSubsetEverything(),
        removal_dir=removal_dir, remove_removal_dir=True,
    )
    tests.assert_FS(
        root_dir, {
            'download': {},
            'mirror': {
                'rpw': {
                    'L3': {
                        'lfr_efield': {
                            '2020': {
                                '05': {
                                    'solo_L3_rpw-bia-efield-10-seconds'
                                    '_20200501_V01.cdf': 10,
                                },
                            },
                        },
                    },
                },
            },
        },
    )
