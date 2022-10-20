'''
Help code for automatic tests.
'''

import datetime
import erikpgjohansson.solo.soar.dwld
import erikpgjohansson.solo.soar.const as const
import os
import pathlib
import urllib
import zipfile


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
'''Content of the JSON "metadata" branch in the SOAR JSON table. This
metadata is implicitly used when hardcoded tests do not explicitly specify
corresponding metadata.
'''


def _get_SOAR_JSON_metadata_ls_index(json_metadata_ls, name):
    for i, entry_dict in enumerate(json_metadata_ls):
        if entry_dict['name'] == name:
            return i
    raise AssertionError()


class MockDownloader(erikpgjohansson.solo.soar.dwld.Downloader):
    '''Class that represents SOAR with a constant pre-defined set of
    datasets.
    '''

    def __init__(self, dc_json_dc=None, dc_json_data_ls=None):
        '''
        Set data to be used as mock SOAR data.

        Parameters
        ----------
        dc_json_dc
            Dictionary. Data structure that represents the exact JSON SDT,
            i.e. e.g.
            [instrument]['data'][i_entry][i_column] = value.
            [instrument]['metadata'][i_entry][i_column] = ...
        dc_json_data_ls
            Dictionary. [instrument][i_entry][i_column] = value
            Representation of SOAR dataset metadata.
            Note: One can use entries from an actual JSON SDT to build a
            hardcoded argument when calling the constructor before tests.
        '''
        '''
        TODO-DEC: Too complicated for test code?
        '''
        use_json = dc_json_dc is not None
        use_json_data = dc_json_data_ls is not None

        # Assign dc_json_dc:
        if use_json and not use_json_data:
            dc_json_dc = dc_json_dc   # Correct value from the start.
        elif not use_json and use_json_data:
            dc_json_dc = {}
            for key, json_data_ls in dc_json_data_ls.items():
                dc_json_dc[key] = {
                    'metadata': SOAR_JSON_METADATA_LS,
                    'data': dc_json_data_ls[key],
                }
        else:
            raise ValueError()

        # Add empty data for unset instruments
        # ------------------------------------
        # IMPLEMENTATION NOTE: Must support arbitrary instruments in order
        # to
        # (1) keep the code automatically compatible with adding future
        #     instruments to LS_SOAR_INSTRUMENTS, and
        # (2) not require the tests to specify/hardcode SDTs for every
        #     instrument, even if there are no datasets (shorter hardcoded
        #     arguments).
        for instrument in const.LS_SOAR_INSTRUMENTS:
            if instrument not in dc_json_dc:
                dc_json_dc[instrument] = {
                    'metadata': SOAR_JSON_METADATA_LS,
                    'data': [],
                }

        # ASSERTION: dc_json_dc has the right format.
        assert type(dc_json_dc) == dict
        for instrument, json_dc in dc_json_dc.items():
            assert type(instrument) == str
            assert type(json_dc) == dict
            for entry_ls in json_dc['data']:
                assert type(entry_ls) == list, 'Entry is not a list.'
                assert len(entry_ls) == 9

        self._dc_json_dc = dc_json_dc

    def download_JSON_SDT(self, instrument: str):
        return self._dc_json_dc[instrument]

    def download_latest_dataset(
        self, data_item_id, dir_path,
        expectedFileName=None, expectedFileSize=None,
    ):
        file_name, file_size = self._get_LV_file_name_size(data_item_id)
        file_path = os.path.join(dir_path, file_name)
        create_file(file_path, file_size)

    def _get_LV_file_name_size(self, data_item_id):
        '''Get file name & size of latest version for specified item ID.'''
        highest_version = 0

        # Iterate over instruments
        for instr_json_dc in self._dc_json_dc.values():
            # Iterate over entries (datasets).

            md = instr_json_dc['metadata']
            i_item_id = _get_SOAR_JSON_metadata_ls_index(md, 'item_id')
            i_version = _get_SOAR_JSON_metadata_ls_index(md, 'item_version')
            i_file_name = _get_SOAR_JSON_metadata_ls_index(md, 'file_name')
            i_file_size = _get_SOAR_JSON_metadata_ls_index(md, 'file_size')

            for entry_ls in instr_json_dc['data']:
                item_id = entry_ls[i_item_id]
                version = int(entry_ls[i_version][1:])
                if (item_id == data_item_id) and (version > highest_version):
                    file_name = entry_ls[i_file_name]
                    file_size = entry_ls[i_file_size]

        assert type(file_size) == int
        return file_name, file_size


def JSON_SDT_filename(instrument):
    '''Generate file name for an JSON SDT file.

    Not using any timestamp so that file names are deterministic and files
    can be lcoated using the same file name.
    '''
    return f'{instrument}_v_public_files.json'


def download_zip_JSON_SDTs(output_dir):
    '''Download JSON SDTs to then manually save in git repo and that
    can then be used by automated tests later (without downloading them).'''

    '''
    NOTE: Empirically: LZMA gives the highest compression here.
    NOTE: LZMA zip files created here can not be decompressed with Linux
          utility "unzip" (irony, 2022-10-19) but can still be decompressed
          by Python code for the relevant automated tests.

    F. size  Method
    ----------------
    1523342 DEFLATED
    1069639 BZIP2
     961086 LZMA
    '''
    # COMPRESSION_METHOD = zipfile.ZIP_DEFLATED
    # COMPRESSION_METHOD = zipfile.ZIP_BZIP2
    COMPRESSION_METHOD = zipfile.ZIP_LZMA   # Has no compression level.
    COMPRESSION_LEVEL = 9

    output_dir = pathlib.Path(output_dir)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H.%M.%S")
    zip_file_path = output_dir / f'SDTs_{timestamp}.zip'

    with zipfile.ZipFile(
        zip_file_path, 'w',
        compression=COMPRESSION_METHOD, compresslevel=COMPRESSION_LEVEL,
    ) as z:
        for instrument in const.LS_SOAR_INSTRUMENTS:
            output_file = output_dir / JSON_SDT_filename(
                instrument,
            )

            print(
                f'Downloading SDT for instrument={instrument}.',
                end='',
            )
            urllib.request.urlretrieve(
                f'{const.SOAR_TAP_URL}/tap/sync?REQUEST=doQuery&LANG=ADQL'
                f'&FORMAT=json&QUERY=SELECT+*+FROM+v_public_files+WHERE'
                f'+instrument=\'{instrument}\'',
                output_file,
            )
            print(' -- Zipping', end='')
            z.write(output_file, arcname=output_file.name)
            print(' -- Done')


def create_file(path, size):
    '''Create file of given size with nonsense content.'''
    assert not os.path.lexists(path)
    with open(path, 'wb') as f:
        f.write(b'0' * size)


if __name__ == '__main__':
    download_zip_JSON_SDTs('/home/erjo/temp/temp')
