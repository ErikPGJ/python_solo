'''
DATA STRUCTURE FOR REPRESENTING A DIRECTORY TREE
================================================
Tests use a simple data structure for representing directory trees that is
suitable for hardcoding. It includes files within it (excluding file
content, but including file sizes).
'''


import erikpgjohansson.solo.soar.mirror
import erikpgjohansson.solo.soar.dwld
import logging
import os
import sys
import tempfile


'''
PROPOSAL: Change way of representing FS objects.
    PROPOSAL: Paths plus file size/None.
        PRO: More concise when having few files in deep subdirectories.
        PRO: Easier to compare paths.
        CON: May have to linebreak paths.

PROPOSAL: Recursive function for comparing dict representations of FS.
    assert act_dict_objs == exp_dict_objs

PROPOSAL: Move helper code to separate module?
'''


SOAR_JSON_METADATA_LS = \
    [
        {
            "name": "archived_on", "datatype": "char", "xtype": None,
            "arraysize": "*", "unit": None, "ucd": None, "utype": None,
        },
        {
            "name": "begin_time", "datatype": "char", "xtype": None,
            "arraysize": "*", "unit": None, "ucd": None, "utype": None,
        },
        {
            "name": "data_type", "datatype": "char", "xtype": None,
            "arraysize": "*", "unit": None, "ucd": None, "utype": None,
        },
        {
            "name": "file_name", "datatype": "char", "xtype": None,
            "arraysize": "*", "unit": None, "ucd": None, "utype": None,
        },
        {
            "name": "file_size", "datatype": "long", "xtype": None,
            "arraysize": None, "unit": None, "ucd": None, "utype": None,
        },
        {
            "name": "instrument", "datatype": "char", "xtype": None,
            "arraysize": "*", "unit": None, "ucd": None, "utype": None,
        },
        {
            "name": "item_id", "datatype": "char", "xtype": None,
            "arraysize": "*", "unit": None, "ucd": None, "utype": None,
        },
        {
            "name": "item_version", "datatype": "char", "xtype": None,
            "arraysize": "*", "unit": None, "ucd": None, "utype": None,
        },
        {
            "name": "processing_level", "datatype": "char", "xtype": None,
            "arraysize": "*", "unit": None, "ucd": None, "utype": None,
        },
    ]
'''Content of the JSON "metadata" branch in the SOAR JSON table.'''


def _get_SOAR_JSON_METADATA_LS_index(name):
    for i, entry_dict in enumerate(SOAR_JSON_METADATA_LS):
        if entry_dict['name'] == name:
            return i
    raise AssertionError()


I_ITEM_ID = _get_SOAR_JSON_METADATA_LS_index('item_id')
I_VERSION = _get_SOAR_JSON_METADATA_LS_index('item_version')
I_FILE_NAME = _get_SOAR_JSON_METADATA_LS_index('file_name')
I_FILE_SIZE = _get_SOAR_JSON_METADATA_LS_index('file_size')


class MockDownloader(erikpgjohansson.solo.soar.dwld.Downloader):
    '''Class that represents SOAR with a constant pre-defined set of
    datasets.
    '''

    def __init__(self, json_data_ls):
        '''

        Parameters
        ----------
        json_data_ls
            Representation of SOAR dataset metadata.
            Note: One can use entries from an actual SOAR datasets table as
            argument when calling the constructor before tests.
        '''

        assert type(json_data_ls) == list
        for entry_ls in json_data_ls:
            assert type(entry_ls) == list, 'Entry is not a list.'
            assert len(entry_ls) == 9

        self._json_data_ls = json_data_ls

    def download_raw_SOAR_datasets_table(self):
        ''''''
        return {
            "metadata": SOAR_JSON_METADATA_LS,
            "data": self._json_data_ls,
        }

    def download_latest_dataset(
        self, dataItemId, fileParentPath,
        expectedFileName=None, expectedFileSize=None,
    ):
        file_name, file_size = self._get_LV_file_name_size(dataItemId)
        file_path = os.path.join(fileParentPath, file_name)
        create_file(file_path, file_size)

    def _get_LV_file_name_size(self, data_item_id):

        highest_version = 0
        for entry_ls in self._json_data_ls:
            item_id = entry_ls[I_ITEM_ID]
            version = int(entry_ls[I_VERSION][1:])
            if (item_id == data_item_id) and (version > highest_version):
                file_name = entry_ls[I_FILE_NAME]
                file_size = entry_ls[I_FILE_SIZE]

        assert type(file_size) == int
        return file_name, file_size


def create_file(path, size):
    '''Create file of given size with nonsense content.'''
    assert not os.path.lexists(path)
    with open(path, 'wb') as f:
        f.write(b'0' * size)


def setup_FS(root_dir, dict_objs):
    '''Create specified directory tree of (nonsense) files and directories.
    FS = File System.

    Note: Function fully validates the input arguments.
    '''

    assert type(dict_objs) == dict

    # NOTE: Permits pre-existing directory.
    os.makedirs(root_dir, exist_ok=True)

    for obj_name, obj_content in dict_objs.items():
        assert type(obj_name) == str
        if type(obj_content) == int:
            create_file(os.path.join(root_dir, obj_name), obj_content)
        elif type(obj_content) == dict:
            # RECURSIVE CALL
            setup_FS(os.path.join(root_dir, obj_name), obj_content)
        else:
            raise Exception()


def assert_FS(root_dir, exp_dict_objs):
    '''Verify that a directory contains exactly the specified set of
    directories and files.'''

    def get_FS(root_dir):
        dict_fs = {}
        it = os.scandir(root_dir)
        for de in it:
            obj_name = de.name
            assert not de.is_symlink()
            if de.is_file():
                dict_fs[obj_name] = de.stat().st_size
            elif de.is_dir():
                dict_fs[obj_name] = get_FS(os.path.join(root_dir, obj_name))
            else:
                raise Exception('')

        return dict_fs

    act_dict_objs = get_FS(root_dir)
    assert act_dict_objs == exp_dict_objs, 'Directory tree is not as expected.'


def test_FS_helpers(tmp_path):
    '''Test internal helper functions.'''

    i_test = -1

    def test_eq(dict_objs):
        ''''''
        nonlocal i_test
        i_test += 1
        test_dir = os.path.join(tmp_path, f'test_{i_test}')

        setup_FS(test_dir, dict_objs)
        assert_FS(test_dir, dict_objs)

    # ====================================================
    # "Manual" tests of assert_FS() (not using setup_FS())
    # ====================================================
    test_dir = os.path.join(tmp_path, 'test_A')
    os.makedirs(test_dir)
    assert_FS(test_dir, {})

    test_dir = os.path.join(tmp_path, 'test_B')
    os.makedirs(os.path.join(test_dir, 'dir1/dir2'))
    create_file(os.path.join(test_dir, 'dir1/file1'), 22)
    create_file(os.path.join(test_dir, 'dir1/dir2/file2'), 11)
    assert_FS(
        test_dir, {
            'dir1': {
                'file1': 22,
                'dir2': {
                    'file2': 11,
                },
            },
        },
    )

    # ===========================================
    # Equality between setup_FS() and assert_FS()
    # ===========================================
    test_eq({})
    test_eq({'file': 0})
    test_eq({'file': 10})
    test_eq({'dir': {}})
    test_eq({
        'dir1': {},
        'file1': 123,
        'dir2': {
            'file2': 321,
        },
    })
    test_eq({
        'dir1': {},
        'file1': 123,
        'dir2': {
            'file2': 321,
            'file3': 321,
            'dir3': {
                'file4': 1,
            },
        },
    })


class DirProducer:
    def __init__(self, root_dir):
        self._root_dir = root_dir
        self._i_dir = 0

    def get_new_dir(self):
        path = os.path.join(self._root_dir, f'{self._i_dir}')
        self._i_dir += 1
        return path


def test_sync(tmp_path):
    '''
    def sync(
        syncDir, tempDownloadDir, datasetsSubsetFunc: typing.Callable,
        deleteOutsideSubset=False,
        downloadLogFormat='short',
        nMaxNetDatasetsToRemove=10,
        SoarTableCacheJsonFilePath=None,
        tempRemovalDir=None,
        removeRemovalDir=False,
        downloader: erikpgjohansson.solo.soar.dwld.Downloader
        = erikpgjohansson.solo.soar.dwld.SoarDownloader(),
    ):
    '''
    tdp = DirProducer(tmp_path)

    def DIF_everything(instrument, level, beginTime, datasetId):
        return True

    def test0():
        root_dir = tdp.get_new_dir()
        sync_dir = os.path.join(root_dir, 'mirror')
        download_dir = os.path.join(root_dir, 'download')

        setup_FS(
            root_dir, {
                'download': {},
                'mirror': {},
            },
        )

        md = MockDownloader(
            [[
                "2020-09-23T13:47:11.73", "2020-08-13T00:00:26.0", "LL",
                "solo_LL02_epd-het-south-rates"
                "_20200813T000026-20200814T000025_V03I.cdf",
                113, "EPD",
                "solo_LL02_epd-het-south-rates"
                "_20200813T000026-20200814T000025",
                "V03", "LL02",
            ]],
        )

        erikpgjohansson.solo.soar.mirror.sync(
            syncDir=sync_dir,
            tempDownloadDir=download_dir,
            datasetsSubsetFunc=DIF_everything,
            deleteOutsideSubset=False,
            downloadLogFormat='short',
            nMaxNetDatasetsToRemove=10,
            SoarTableCacheJsonFilePath=None,
            tempRemovalDir=None,
            removeRemovalDir=False,
            downloader=md,
        )

        assert_FS(
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

    def test1():
        def DIF(instrument, level, beginTime, datasetId):
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

        root_dir = tdp.get_new_dir()
        sync_dir = os.path.join(root_dir, 'mirror')
        download_dir = os.path.join(root_dir, 'download')
        setup_FS(
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
        md = MockDownloader(
            [L2_MAG_V02, L2_MAG_V03, L1_EUI, L3_BIA],
        )

        erikpgjohansson.solo.soar.mirror.sync(
            syncDir=sync_dir,
            tempDownloadDir=download_dir,
            datasetsSubsetFunc=DIF,
            deleteOutsideSubset=False,
            downloadLogFormat='short',
            nMaxNetDatasetsToRemove=10,
            SoarTableCacheJsonFilePath=None,
            tempRemovalDir=None,
            removeRemovalDir=False,
            downloader=md,
        )

        assert_FS(
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

    test0()
    test1()


def test_offline_cleanup(tmp_path):
    tdp = DirProducer(tmp_path)
    # TODO: Check misplaced files.

    def DIF(instrument, level, beginTime, datasetId):
        return True

    def test0():

        root_dir = tdp.get_new_dir()
        sync_dir = os.path.join(root_dir, 'mirror')
        download_dir = os.path.join(root_dir, 'download')
        removal_dir = os.path.join(root_dir, 'removal')

        setup_FS(
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
            sync_dir, download_dir, DIF, tempRemovalDir=removal_dir,
            removeRemovalDir=True,
        )

        assert_FS(
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

    def test1():
        root_dir = tdp.get_new_dir()
        sync_dir = os.path.join(root_dir, 'mirror')
        download_dir = os.path.join(root_dir, 'download')
        removal_dir = os.path.join(root_dir, 'removal')

        setup_FS(
            root_dir, {
                'download': {},
                'mirror': {
                    # File in wrong location.
                    'solo_L3_rpw-bia-efield-10-seconds_20200501_V01.cdf': 10,
                },
            },
        )
        erikpgjohansson.solo.soar.mirror.offline_cleanup(
            sync_dir, download_dir, DIF, tempRemovalDir=removal_dir,
            removeRemovalDir=True,
        )
        assert_FS(
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

    test0()
    test1()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    test_FS_helpers(tempfile.TemporaryDirectory().name)
    test_sync(tempfile.TemporaryDirectory().name)
    test_offline_cleanup(tempfile.TemporaryDirectory().name)
