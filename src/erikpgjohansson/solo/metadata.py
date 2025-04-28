'''Module for handling dataset metadata, in particular filenames and
substrings thereof.

Filenaming conventions are described in
"Metadata Definition for Solar Orbiter Science Data", SOL-SGS-TN-0009.

Initially created by Erik P G Johansson 2020-10-16,
IRF Uppsala, Sweden.
'''


import erikpgjohansson.solo.str


'''
PROPOSAL: Function for parsing "item_id".
    PRO: Can use for deducing level, which can be used for handling special
         case of LL when downloading.
    PRO: Can use to select sync subset based on e.g. DSID (?).

PROPOSAL: parse_item_ID() to solo.soar.
    PRO: Is associated with SOAR, not SolO in general.
'''


# ========================
# Regular Expressions (RE)
# ========================
_RE_TIME_INTERVAL_STR  = '[0-9T-]{4,31}'
'''Matches all time interval strings, on all formats.'''
_RE_YYYY               = '[0-9]{4,4}'
_RE_YYYYMM             = '[0-9]{6,6}'
_RE_YYYYMMDD           = '[0-9]{8,8}'
_RE_YYYYMMDDThhmmss    = '[0-9]{8,8}T[0-9]{6,6}'
_RE_YYYYMMDDThhmmssddd = '[0-9]{8,8}T[0-9]{6,9}'
# L0 filenames contain OBT, not UTC. It is not known how many digits these
# may use, but empirically it is always ten.
_RE_OBT                = '[0-9]{10,10}'


class DatasetFilename:
    '''Class which represents (some of) the content of a dataset filename
    which follows official filenaming conventions.

    Note: The class does not contain all the information in the dataset
    filename.

    Note: Implementation inspired by irfu-matlab's
    solo.adm.dsfn.DatasetFilename but does not fully conform to it. That
    implementation also includes creating dataset filenames, handling
    redundant information through properties (calculated on demand), and is
    fully invertible. Any future expansions or refactoring to this dataset
    filename code should (probably) follow that design.
    '''
    '''
    PROPOSAL: Replace versionStr-->versionNbr.
        PROBLEM: LL "I" and "C" could maybe be considered part of "version".
    '''

    def __init__(self, dsid, timeIntervalStr, timeVector1, itemId, versionStr):
        # Always upper case. (Excludes CDAG.)
        self.dsid            = dsid
        self.timeIntervalStr = timeIntervalStr
        # Tuple.(year, month, day, hour, minute, second)
        self.timeVector1     = timeVector1
        # String. Ex: 'solo_HK_rpw-bia_20200301'
        # As defined by SDT.
        self.itemId          = itemId
        # String. Ex: '02'.
        self.versionStr      = versionStr

    def __eq__(self, other):
        '''Useful for tests.
        '''
        # NOTE: Do not permit any difference in class.
        if type(self) is not type(other):
            return False
        else:
            return self.__dict__ == other.__dict__

    def __repr__(self):
        s = (
            f'{type(self).__name__}('
            f'dsid="{self.dsid}", '
            f'timeIntervalStr="{self.timeIntervalStr}", '
            f'timeVector1={self.timeVector1}, '
            f'itemId="{self.itemId}", '
            f'versionStr="{self.versionStr}"'
            ')'
        )
        return s

    @staticmethod
    def parse_filename(filename):
        '''
        Parse dataset filename following official filenaming convention.

        NOTE: Does *not* support all official datasets (from all instruments),
        but a large subset of them.

        NOTE: Can amend code to return more filename fields eventually.

        NOTE: Includes added support for tolerating the RPW-specific
        consortium-internal "-cdag" extension to official filenaming
        conventions.

        NOTE: Only permits specific file extensions. Some file extensions
        will always result in filename no being recognized as a dataset
        despite that there are official datasets with those file extensions.
        The SOAR mirror uses
        erikpgjohansson.solo.soar.const.FILE_SUFFIX_IGNORE_LIST

        IMPLEMENTATION NOTE: The functionality kind of overlaps with
        parse_item_ID(), except that that function does not support the
        "-CDAG" extension. Can therefore not use parse_item_ID() in the
        implementation of this function.

        Parameters
        ----------
        filename : String
            Nominally file name following official dataset filenaming
            conventions.
            NOTE: Applies to datasets for all instruments (not just RPW).

        Returns
        -------
        If filename can not be parsed :
            None
        If filename can be parsed :
            Instance of `erikpgjohansson.solo.DatasetFilename`.

        Examples of in-flight dataset filenames
        ---------------------------------------
        solo_L3_epd-ept-1day_2024_V11.cdf
        solo_L3_epd-ept-1hour_202301_V01.cdf
        solo_HK_rpw-bia_20200301_V01.cdf
        solo_L2_rpw-lfr-surv-cwf-e-cdag_20200213_V01.cdf
        solo_L1_rpw-bia-sweep-cdag_20200307T053018-20200307T053330_V01.cdf
        solo_L2_epd-step-burst_20200703T232228-20200703T233728_V02.cdf
        # NOTE: Upper case in DSID outside level.
        solo_L1_swa-eas2-NM3D_20201027T000007-20201027T030817_V01.cdf
        # NOTE: LL, V03I
        solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025_V03I.cdf
        # NOTE: File suffix ".fits".
        # NOTE: Extra second decimals.
        solo_L1_eui-fsi174-image_20200806T083130185_V01.fits
        '''
        '''
        PROPOSAL: Return "timeVector2" (end of dataset time according to name).
            PROBLEM: How increment day (over month/year boundary) to find it?
        '''
        '''
        NOTE: Reg.exp. "[CIU]?" after version string appears to be
        required(?) for LL data, but is absent otherwise.
        /SOL-SGS-TN-0009 MetadataStandard



        """"""""
        The CDF files will be named according to the file naming convention
        described in [METADATA], with the exceptions that the filename shall
        contain the coarse SCET of the first and last record in the file in
        decimal, each padded to 10 digits, rather than any UTC timetag,
        the version number is the generation date and time of the file to
        minute resolution and incomplete files (see below) are given the
        suffix ‘I’ after the version number. For example, 1 incomplete day
        of low latency magnetic field vectors from MAG may be named:

        solo_LL01_mag-vec_0000000000-0000086399_V201810120000I.cdf
        """"""""

        /SOL-SGS-ICD-0004, "Solar Orbiter Interface Control Document for Low
        Latency Data CDF Files", 1/4, SOL-SGS-ICD-0004-LLCDFICD-1.4draft3.pdf



        """"""""
        The FITS files will be named according to the file naming convention
        described in [METADATA], with the exception that the filename shall
        contain the coarse OBT of the observation in the file, padded to 10
        digits, rather than any UTC timetag. The version number is the
        generation date and time of the file (in UTC) and (in)complete files
        are given the suffix (‘I’)‘C’ after the version number, files for
        which that distinction cannot be made are given the suffix ‘U’ (see
        Sect. 3.2.2).

        /.../

        For example, a PHI magnetogram may be named:

        solo_LL01_phi-fdt-magn_0000086399_V202001120000C.fits
        """"""""
        /SOL-SGS-ICD-0005, "Solar Orbiter Interface Control Document for Low
        Latency Data FITS Files", 1/5, SOL-SGS-ICD-0005-LLFITSICD-1.5draft.pdf
        '''
        ls_str, remaining_str, b_perfect_match = \
            erikpgjohansson.solo.str.regexp_str_parts(
                filename, [
                    '.*',                    # 0
                    '(|-cdag)',
                    '_',
                    _RE_TIME_INTERVAL_STR,   # 3
                    '_V',
                    '[0-9][0-9]+',           # 5
                    '[CIU]?',
                    r'\.(cdf|fits|bin)',
                ],
                -1, 'permit non-match',
            )

        if not b_perfect_match:
            return None

        # NOTE: Does not store any separate flag for CDAG/non-CDAG. Only
        #       tolerates it.
        itemId            = ''.join(ls_str[0:1] + ls_str[2:4])
        dsid              = ls_str[0].upper()
        time_interval_str = ls_str[3]
        versionStr        = ls_str[5]

        tv1 = _parse_time_interval_str(time_interval_str)
        if tv1 is None:
            return None

        dsfn = DatasetFilename(
            dsid=dsid, timeIntervalStr=time_interval_str,
            versionStr=versionStr, timeVector1=tv1, itemId=itemId,
        )
        return dsfn


def parse_item_ID(itemId: str):
    '''
    Parse an "item ID" as SOAR defines it.

    NOTE: Does not support the RPW consortium-internal "-CDAG" extension to
    the official filenaming conventions, since the "item ID" is not a
    filename, and is only relevant in the contect of SOAR.

    Parameters
    ----------
    itemId

    Returns
    -------
    Dictionary: if can parse string.
    None: if can not parse string.
    '''
    '''
    PROPOSAL: Class/namedtuple for return value.
    '''
    ls_str, remaining_str, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            itemId, [
                '.*',                    # 0
                '_',
                _RE_TIME_INTERVAL_STR,   # 2
            ],
            -1, 'permit non-match',
        )
    if not b_perfect_match:
        return None

    dsid              = ls_str[0].upper()
    time_interval_str = ls_str[2]
    tv1 = _parse_time_interval_str(time_interval_str)
    if tv1 is None:
        return None

    return {'DSID': dsid, 'time vector 1': tv1}


def _parse_time_interval_str(time_interval_str: str):
    '''
    Parse time interval string. Only return the *FIRST* timestamp if there
    are two.

    Parameters
    ----------
    time_interval_str:
        String on any one of the formats
            YYYYMMDD
            YYYYMMDD-YYYYMMDD
            YYYYMMDDThhmmssddd    # ddd = 0-3 second decimals
            YYYYMMDDThhmmss-YYYYMMDDThhmmss   # No second decimals

    Returns
    -------
    If can parse string as UTC.
        year, month, day, hour, minute (all int), second (float)
        If there are two UTC timestamps, only return value for the first
        timestamp.
    If can parse string as OBT-OBT.
        Length-1 tuple with OBT value (int).
    Else
        None
    '''

    # ====================================================================
    # "parse functions": Functions for converting time interval string (on
    #                    known format) to separate field values
    # ====================================================================

    def parse_YYYY(s):
        assert len(s) == 4
        year   = int(s[0:4])
        return (year,)   # Return size-1 tuple!

    def parse_YYYYMM(s):
        assert len(s) == 6
        year   = int(s[0:4])
        month  = int(s[4:6])
        return year, month

    def parse_YYYYMMDD(s):
        assert len(s) == 8
        year   = int(s[0:4])
        month  = int(s[4:6])
        day    = int(s[6:8])
        return year, month, day

    # ddd = Arbitrary number of decimals after integer seconds.
    def parse_YYYYMMDDThhmmssddd(s):
        assert len(s) >= 8+1+6
        year   = int(s[0:4])
        month  = int(s[4:6])
        day    = int(s[6:8])
        assert s[8] == 'T'
        hour   = int(s[9:11])
        minute = int(s[11:13])
        # IMPLEMENTATION NOTE: Substring representing seconds does not contain
        # any period. Can therefore not apply float() on it directly.
        seconds_str = s[13:]
        second = int(seconds_str) / 10**(len(seconds_str)-2)   # Always float
        return year, month, day, hour, minute, second

    def parse_OBT(s):
        assert len(s) == 10
        # NOTE: Returns length-1 tuple.
        return (int(s),)   # Return size-1 tuple!

    # ====================================================================
    # Try parsing time interval string, by trying one format after another
    # ====================================================================

    _, _, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            time_interval_str,
            [_RE_YYYY],
            1, 'permit non-match',
        )
    if b_perfect_match:
        return parse_YYYY(time_interval_str) + (1, 1, 0, 0, 0.0)

    _, _, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            time_interval_str,
            [_RE_YYYYMM],
            1, 'permit non-match',
        )
    if b_perfect_match:
        return parse_YYYYMM(time_interval_str) + (1, 0, 0, 0.0)

    _, _, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            time_interval_str,
            [_RE_YYYYMMDD],
            1, 'permit non-match',
        )
    if b_perfect_match:
        return parse_YYYYMMDD(time_interval_str) + (0, 0, 0.0)

    ls_str, _, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            time_interval_str,
            [_RE_YYYYMMDD, '-', _RE_YYYYMMDD],
            1, 'permit non-match',
        )
    if b_perfect_match:
        return parse_YYYYMMDD(ls_str[0]) + (0, 0, 0.0)

    ls_str, _, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            time_interval_str,
            [_RE_YYYYMMDDThhmmssddd],
            1, 'permit non-match',
        )
    if b_perfect_match:
        return parse_YYYYMMDDThhmmssddd(ls_str[0])

    ls_str, _, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            time_interval_str,
            [_RE_YYYYMMDDThhmmss, '-', _RE_YYYYMMDDThhmmss],
            1, 'permit non-match',
        )
    if b_perfect_match:
        return parse_YYYYMMDDThhmmssddd(ls_str[0])

    # LL01 filenames contain OBT, not UTC.
    ls_str, _, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            time_interval_str,
            [_RE_OBT, '-', _RE_OBT],
            1, 'permit non-match',
        )
    if b_perfect_match:
        return parse_OBT(ls_str[0])

    return None


def parse_DSID(dsid):
    '''
    Split a DSID into its constituent parts.


    Parameters
    ----------
    dsid : String
        Officially defined DSID. Uppercase.
        NOTE: Must not include -CDAG. Does however detect it to give special
        error message if found.


    Raises
    ------
    Exception
        If dsid is not a DSID.


    Returns
    -------
    data_source : String.
        Ex: 'SOLO', 'RGTS'
    level : String
        Ex: 'L2'
    instrument : String
        Ex: 'RPW''
    descriptor : String
        Ex: 'RPW-LFR-SBM2-CWF-E'
        NOTE: "descriptor" is wrong term?
    '''
    '''
    BUG?: Can not handle LL01 (does not exist?), LL03 (can not find any example
          yet).
    '''
    if not dsid.upper() == dsid:
        raise Exception(f'Not uppercase dsid="{dsid}"')

    # IMPLEMENTATION NOTE: Must search backwards because of -CDAG.
    # IMPLEMENTATION NOTE: "instrument" and "descriptor" can be identical.
    #   Ex: SOLO_LL02_MAG
    # NOTE: Only identifies -CDAG to giver custom error message.
    # NOTE: Have not seen any LL01 datasets. Not sure if such exist.
    ls_str, remaining_str, b_perfect_match = \
        erikpgjohansson.solo.str.regexp_str_parts(
            dsid,
            [
                '(SOLO|RGTS)',    # 0
                '_',
                '(LL02|LL03|L1|L1R|L2|L3)',    # 2
                '_',
                '[A-Z]+',            # 4
                '(|-[A-Z0-9-]+)',    # 5
                '(|-CDAG)',          # 6
            ],
            -1, 'permit non-match',
        )

    # ASSERTIONS
    if not b_perfect_match:
        raise Exception(f'Can not parse dsid="{dsid}".')
    cdag_str = ls_str[-1]
    if cdag_str:
        raise Exception(
            f'Illegal dsid="{dsid}" that contains "-CDAG".',
        )

    data_source = ls_str[0]
    level       = ls_str[2]
    instrument  = ls_str[4]
    descriptor  = ''.join(ls_str[4:6])
    # NOTE: Descriptor INCLUDES instrument.

    return data_source, level, instrument, descriptor
