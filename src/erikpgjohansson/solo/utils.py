'''
Initially created by Erik P G Johansson 2020-10-16, IRF Uppsala, Sweden.

Filenaming conventions are described in
"Metadata Definition for Solar Orbiter Science Data", SOL-SGS-TN-0009.
'''


import erikpgjohansson.solo.str


'''
BOGIQ
=====
PROPOSAL: Function for parsing "item_id".
    PRO: Can use for deducing level, which can be used for handling special
         case of LL when downloading.
    PRO: Can use to select sync subset based on e.g. DSID (?).

PROPOSAL: parse_item_ID() to solo.soar.
    PRO: Is associated with SOAR, not SolO in general.
'''


# Regular Expressions (RE)
RE_TIME_INTERVAL_STR  = '[0-9T-]{8,31}'
RE_YYYYMMDD           = '[0-9]{8,8}'
RE_YYYYMMDDThhmmss    = '[0-9]{8,8}T[0-9]{6,6}'
RE_YYYYMMDDThhmmssddd = '[0-9]{8,8}T[0-9]{6,9}'
# L0 filenames contain OBT, not UTC. It is not known how many digits these
# may use, but empirically it is always ten.
RE_OBT                = '[0-9]{10,10}'


def parse_dataset_filename(filename):
    '''
    Parse dataset filename following official filenaming convention.

    NOTE: Can amend code to return more filename fields eventually.
    NOTE: Includes added support for tolerating the RPW-specific
    consortium-internal "-cdag" extension to official filenaming conventions.

    IMPLEMENTATION NOTE: Functionality kind of overlaps with parse_item_ID(),
    except that that function does not support for "-CDAG" extension. Can
    therefore not use parse_item_ID() in the implementation of this function.

    Parameters
    ----------
    filename : String
        Nominally file name following official naming conventions.
        NOTE: Applies to datasets for all instruments (not just RPW).

    Returns
    -------
    If filename can not be parsed :
        None
    If filename can be parsed :
        Dictionary
            'DSID'                 : Always upper case. (Excludes CDAG.)
            'time interval string' :
            'time vector 1'        : Tuple. (year,month,day,hour,minute,second)
            'item ID'              : String. Ex: 'solo_HK_rpw-bia_20200301'
                                     As defined by SDT.
            'version string'       : String. Ex: '02'.


    Examples of in-flight dataset filenames
    ---------------------------------------
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
    PROPOSAL: Return "time vector 2" (end of dataset time according to name).
        PROBLEM: How increment day (over month/year boundary) to find it?

    PROPOSAL: Return namedtuple, not dictionary.
    '''
    # NOTE: Reg.exp. "[CIU]?" appears to be required(?) for LL data, but is
    # absent otherwise.
    # /SOL-SGS-TN-0009 MetadataStandard
    substrList, remainingStr, isPerfectMatch = \
        erikpgjohansson.solo.str.regexp_str_parts(
            filename, [
                '.*',              # 0
                '(|-cdag|-CDAG)', '_',
                RE_TIME_INTERVAL_STR,   # 3
                '_V',
                '[0-9][0-9]+',     # 5
                '[CIU]?', r'\.(cdf|fits|bin)',
            ],
            -1, 'permit non-match',
        )

    if not isPerfectMatch:
        return None

    # NOTE: No separate flag to capture CDAG/non-CDAG. Only tolerates it.
    # NOTE: Excludes CDAG=substrList[1].
    itemId = ''.join(substrList[0:1] + substrList[2:4])
    dsid            = substrList[0].upper()
    timeIntervalStr = substrList[3]
    versionStr      = substrList[5]
    try:
        tv1 = _parse_time_interval_str(timeIntervalStr)
    except Exception:
        return None

    d = {
        'DSID':                 dsid,
        'time interval string': timeIntervalStr,
        'version string':       versionStr,
        'time vector 1':        tv1,
        'item ID':              itemId,
    }
    return d


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
    substrList, remainingStr, isPerfectMatch = \
        erikpgjohansson.solo.str.regexp_str_parts(
            itemId, [
                '.*',              # 0
                '_',
                RE_TIME_INTERVAL_STR,   # 2
            ],
            -1, 'permit non-match',
        )
    if not isPerfectMatch:
        return None

    dsid            = substrList[0].upper()
    timeIntervalStr = substrList[2]
    try:
        tv1 = _parse_time_interval_str(timeIntervalStr)
    except Exception:
        return None

    return {'DSID': dsid, 'time vector 1': tv1}


def _parse_time_interval_str(timeIntervalStr: str):
    '''
    Parse time interval string.

    Parameters
    ----------
    timeIntervalStr:
        String on any one of the formats
            YYYYMMDD
            YYYYMMDD-YYYYMMDD
            YYYYMMDDThhmmssddd    # ddd = 0-3 second decimals
            YYYYMMDDThhmmss-YYYYMMDDThhmmss   # No second decimals

    Returns
    -------
    year, month, day, hour, minute (all int)
    second (float)

    Exception
    ---------
    If can not parse string.
    '''
    '''
    PROPOSAL: Refactor to return ~None if can not parse.
        PRO: Analogous with parse_dataset_filename().
        CON: Must reinterpret assertions.
    '''

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
        secondsStr = s[13:]
        second = int(secondsStr) / 10**(len(secondsStr)-2)   # Always float
        return year, month, day, hour, minute, second

    def parse_OBT(s):
        assert len(s) == 10
        # NOTE: Returns length-1 tuple.
        return (int(s),)   # Return size-1 tuple!

    _, _, isPerfectMatch = \
        erikpgjohansson.solo.str.regexp_str_parts(
            timeIntervalStr,
            [RE_YYYYMMDD],
            1, 'permit non-match',
        )
    if isPerfectMatch:
        return parse_YYYYMMDD(timeIntervalStr) + (0, 0, 0.0)

    substrList, _, isPerfectMatch = \
        erikpgjohansson.solo.str.regexp_str_parts(
            timeIntervalStr,
            [RE_YYYYMMDD, '-', RE_YYYYMMDD],
            1, 'permit non-match',
        )
    if isPerfectMatch:
        return parse_YYYYMMDD(substrList[0]) + (0, 0, 0.0)

    substrList, _, isPerfectMatch = \
        erikpgjohansson.solo.str.regexp_str_parts(
            timeIntervalStr,
            [RE_YYYYMMDDThhmmssddd],
            1, 'permit non-match',
        )
    if isPerfectMatch:
        return parse_YYYYMMDDThhmmssddd(substrList[0])

    substrList, _, isPerfectMatch = \
        erikpgjohansson.solo.str.regexp_str_parts(
            timeIntervalStr,
            [RE_YYYYMMDDThhmmss, '-', RE_YYYYMMDDThhmmss],
            1, 'permit non-match',
        )
    if isPerfectMatch:
        return parse_YYYYMMDDThhmmssddd(substrList[0])

    # L0 filenames contain OBT, not UTC.
    substrList, _, isPerfectMatch = \
        erikpgjohansson.solo.str.regexp_str_parts(
            timeIntervalStr,
            [RE_OBT, '-', RE_OBT],
            1, 'permit non-match',
        )
    if isPerfectMatch:
        return parse_OBT(substrList[0])

    raise Exception(f'Can not parse time interval string "{timeIntervalStr}".')


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
    dataSrc : String.
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
    substrList, remainingStr, isPerfectMatch = \
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
    if not isPerfectMatch:
        raise Exception(f'Can not parse dsid="{dsid}".')
    cdagStr = substrList[-1]
    if cdagStr:
        raise Exception(
            f'Illegal dsid="{dsid}" that contains "-CDAG".',
        )

    dataSrc    = substrList[0]
    level      = substrList[2]
    instrument = substrList[4]
    descriptor = ''.join(substrList[4:6])
    # NOTE: Descriptor INCLUDES instrument.

    return dataSrc, level, instrument, descriptor


if __name__ == '__main__':
    pass
