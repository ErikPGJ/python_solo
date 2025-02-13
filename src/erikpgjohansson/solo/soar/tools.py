'''
Tools which are not part of erikpgjohansson.solo.soar mirrors, but still
related.
'''


import erikpgjohansson.solo.soar.const
import erikpgjohansson.solo.soar.dwld
import erikpgjohansson.solo.soar.tests
import datetime
import pathlib
import zipfile


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

    No support for LZMA on brain
    ===========================
    Test fails on brain, and it appears at a cursory look that it is due to
    LZMA not being supported.  /Erik P G Johansson 2022-10-24
    """"
    tests/test_erikpgjohansson_solo_soar_dwld.py:144:
    _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
    tests/test_erikpgjohansson_solo_soar_dwld.py:81: in test_stored_actual_SDTs
        z.extractall(tmp_path)
    ../../nonstd_installs/pyenv/versions/3.9.5/lib/python3.9/zipfile.py:1633:
    in extractall
        self._extract_member(zipinfo, path, pwd)
    ../../nonstd_installs/pyenv/versions/3.9.5/lib/python3.9/zipfile.py:1686:
    in _extract_member
        with self.open(member, pwd=pwd) as source, \
    ../../nonstd_installs/pyenv/versions/3.9.5/lib/python3.9/zipfile.py:1559:
    in open
        return ZipExtFile(zef_file, mode, zinfo, pwd, True)
    ../../nonstd_installs/pyenv/versions/3.9.5/lib/python3.9/zipfile.py:797:
    in __init__
        self._decompressor = _get_decompressor(self._compress_type)
    ../../nonstd_installs/pyenv/versions/3.9.5/lib/python3.9/zipfile.py:698:
    in _get_decompressor
        _check_compression(compress_type)
    _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

    compression = 14

        def _check_compression(compression):
            if compression == ZIP_STORED:
                pass
            elif compression == ZIP_DEFLATED:
                if not zlib:
                    raise RuntimeError(
                        "Compression requires the (missing) zlib module")
            elif compression == ZIP_BZIP2:
                if not bz2:
                    raise RuntimeError(
                        "Compression requires the (missing) bz2 module")
            elif compression == ZIP_LZMA:
                if not lzma:
    >               raise RuntimeError(
                        "Compression requires the (missing) lzma module")
    E               RuntimeError: Compression requires the (missing) lzma
    module

    ../../nonstd_installs/pyenv/versions/3.9.5/lib/python3.9/zipfile.py:675:
    RuntimeError
    """"   brain, 2022-10-24
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
        for instrument in erikpgjohansson.solo.soar.const.LS_SOAR_INSTRUMENTS:
            output_file = \
                output_dir / erikpgjohansson.solo.soar.tests.JSON_SDT_filename(
                    instrument,
                )

            print(f'Downloading SDT for instrument={instrument}.', end='')
            s = erikpgjohansson.solo.soar.dwld.SoarDownloaderImpl.\
                download_JSON_SDT_JSON_string(instrument)
            with open(output_file, 'w') as f:
                f.write(s)

            print(' -- Zipping', end='')
            z.write(output_file, arcname=output_file.name)
            print(' -- Done')
