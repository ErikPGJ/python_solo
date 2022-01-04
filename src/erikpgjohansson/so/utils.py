# -*- coding: UTF-8 -*-
'''
Initially created by Erik P G Johansson 2020-10-16.
'''
'''
BOGIQ
=====
'''



import erikpgjohansson.str







def parse_dataset_filename(filename):
    '''
    Parse dataset filename following official filenaming convention.

    NOTE: Can amend code to return more filename fields eventually.


    Parameters
    ----------
    filename : String
        Nominally file name following official naming conventions.
        NOTE: Includes te RPW-specific consortium-internal "-cdag".
        NOTE: Applies to datasets for all instruments (not just RPW).


    Returns
    -------
    If filename can not be parsed :
        None
        NOTE: Does not raise exception.
    If filename can be parsed :
        Dictionary
            'DATASET_ID'           : Always upper case. (Excludes CDAG.)
            'time interval string' :
            'time vector 1'        : Tuple. (year,month,day,hour,minute,second)
            'item ID'              : String. Ex: 'solo_HK_rpw-bia_20200301'
                As defined by SOAR datasets table.
            'version string'       : String. Ex: '02'.


    Examples of in-flight dataset filenames
    ---------------------------------------
    solo_HK_rpw-bia_20200301_V01.cdf                   # NOTE: No -cdag.
    solo_L2_rpw-lfr-surv-cwf-e-cdag_20200213_V01.cdf   # NOTE: -cdag.
    solo_L1_rpw-bia-sweep-cdag_20200307T053018-20200307T053330_V01.cdf
    solo_L2_epd-step-burst_20200703T232228-20200703T233728_V02.cdf
    # NOTE: Upper case in DATASET_ID outside level.
    solo_L1_swa-eas2-NM3D_20201027T000007-20201027T030817_V01.cdf
    # NOTE: LL, V03I
    solo_LL02_epd-het-south-rates_20200813T000026-20200814T000025_V03I.cdf
    # NOTE: .fits
    solo_L1_eui-fsi174-image_20200806T083130185_V01.fits
    '''
    '''
    PROPOSAL: Return "time vector 2" (end of dataset time according to name).
        PROBLEM: How increment day (over month/year boundary) to find it?
    '''
    def parse_time_interval_str(timeIntervalStr):

        def parse_YYYYMMDD(s):
            assert len(s) == 8
            year   = int(s[0:4])
            month  = int(s[4:6])
            day    = int(s[6:8])
            return (year, month, day)

        # xyz = Any number of decimals after integer seconds.
        def parse_YYYYMMDDThhmmssxyz(s):
            assert len(s) >= 8+1+6
            assert s[8] == 'T'
            year   = int(  s[0:4])
            month  = int(  s[4:6])
            day    = int(  s[6:8])
            hour   = int(  s[9:11])
            minute = int(  s[11:13])
            # second = float(s[13:15])
            secondsStr = s[13:]
            second = int(secondsStr) / 10**(len(secondsStr)-2)   # Always float
            return (year, month, day, hour, minute, second)

        (substrList, remainingStr, isPerfectMatch) = erikpgjohansson.str.regexp_str_parts(
            timeIntervalStr,
            ['[0-9]{8,8}'],
            1, 'permit non-match')
        if isPerfectMatch:
            return parse_YYYYMMDD(timeIntervalStr) + (0, 0, 0.0)

        (substrList, remainingStr, isPerfectMatch) = erikpgjohansson.str.regexp_str_parts(
            timeIntervalStr,
            ['[0-9]{8,8}', '-', '[0-9]{8,8}'],
            1, 'permit non-match')
        if isPerfectMatch:
            return parse_YYYYMMDD(substrList[0]) + (0, 0, 0.0)

        (substrList, remainingStr, isPerfectMatch) = erikpgjohansson.str.regexp_str_parts(
            timeIntervalStr,
            ['[0-9]{8,8}T[0-9]{6,9}'],
            1, 'permit non-match')
        if isPerfectMatch:
            return parse_YYYYMMDDThhmmssxyz(substrList[0])

        (substrList, remainingStr, isPerfectMatch) = erikpgjohansson.str.regexp_str_parts(
            timeIntervalStr,
            ['[0-9]{8,8}T[0-9]{6,6}', '-', '[0-9]{8,8}T[0-9]{6,6}'],
            1, 'permit non-match')
        if isPerfectMatch:
            return parse_YYYYMMDDThhmmssxyz(substrList[0])

        raise Exception('Can not parse time interval string "{}" in filename "{}".'
                        .format(timeIntervalStr, filename))



    # NOTE: Reg.exp. "[CIU]?" required(?) for LL data, and is absent otherwise.
    # /SOL-SGS-TN-0009 MetadataStandard
    (substrList, remainingStr, isPerfectMatch) = erikpgjohansson.str.regexp_str_parts(
        filename,
        ['.*', '(|-cdag|-CDAG)', '_', '[0-9T-]{8,31}', '_V', '[0-9][0-9]+',
         '[CIU]?', r'(\.cdf|\.fits)'],
        -1, 'permit non-match')

    if not isPerfectMatch:
        return None

    # NOTE: No separate flag for CDAG/non-CDAG.
    #itemId = ''.join(substrList[0:4])   # NOTE: Includes CDAG. Bad?!
    itemId = ''.join(substrList[0:1] + substrList[2:4])   # NOTE: Excludes CDAG. Bad?!
    datasetId       = substrList[0].upper()
    timeIntervalStr = substrList[3]
    tv1             = parse_time_interval_str(timeIntervalStr)
    versionStr      = substrList[5]
    assert len(tv1) == 6
    assert type(tv1[5]) == float

    d = {'DATASET_ID':           datasetId,
         'time interval string': timeIntervalStr,
         'version string':       versionStr,
         'time vector 1':        tv1,
         'item ID':              itemId}
    return d







def parse_DATASET_ID(datasetId):
    '''
    Split a DATASET_ID into its constituent parts.


    Parameters
    ----------
    datasetId : String
        Officially defined DATASET_ID. Uppercase.
        NOTE: Must not include -CDAG. Does however detect it to give special
        error message if found.


    Raises
    ------
    Exception
        If datasetId is not a DATASET_ID.


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
    PROPOSAL: Return dictionary instead.
        TODO-DEC: Term for last segment.
            NOTE: "descriptor" could be bad term.
    BUG?: Can not handle LL01 (does not exist?), LL03 (can not find any example yet).
    '''
    if not datasetId.upper() == datasetId:
        raise Exception('Not uppercase datasetId="{}"'.format(datasetId))

    # IMPLEMENTATION NOTE: Must search backwards because of -CDAG.
    # NOTE: Only identifies -CDAG to giver custom error message.
    (substrList, remainingStr, isPerfectMatch) = erikpgjohansson.str.regexp_str_parts(
        datasetId,
        ['(SOLO|RGTS)', '_', '(LL02|HK|L1|L1R|L2|L3)', '_',
         '[A-Z]+', '-[A-Z0-9-]+', '(|-CDAG)'],
        -1, 'permit non-match')

    # ASSERTIONS
    if not isPerfectMatch:
        raise Exception('Can not parse datasetId="{}".'.format(datasetId))
    cdagStr = substrList[-1]
    if cdagStr:
        raise Exception(
            'Illegal datasetId="{}" that contains "-CDAG".'.format(datasetId))

    dataSrc    = substrList[0]
    level      = substrList[2]
    descriptor = ''.join(substrList[4:6])
    instrument = substrList[4]
    # NOTE: Does not return descriptor _without_ instrument.

    return (dataSrc, level, instrument, descriptor)







if __name__ == '__main__':
    pass