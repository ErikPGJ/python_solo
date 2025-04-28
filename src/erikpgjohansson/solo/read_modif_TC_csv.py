'''
Utility for (human) reading arbitrary many, non-overlapping RPW TC .csv files
(e.g. solo_STP124_rpw_20201130_20201206_V01.csv).

Prints filtered, modified human-readable version of files to stdout.

Meant to replace bash script so_find_TC_sweeps (2020-12-14: Obsoleted) and be
called from the bash command line using a wrapper script.


SHOULD PRINT
============
~sweeps
~uploaded sweep tables
~bias current settings.

Initially created 2020-11-26 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import csv
import datetime
import operator
import re


'''
TODO-DEC: Name?
    PROPOSAL: ~read, ~filter, ~modif, ~interpret
    PROPOSAL: ~TC, ~csv
    NOTE: Should be similar to bash wrapper script.
    PROPOSAL: Imply that it is not a ~generic python function.

PROPOSAL: Automatically derive column widths.
PROPOSAL: Include uploads of new sweep tables.
PROPOSAL: References to examples.
PROPOSAL: Select to only see commands for selected categories
    Ex: sweeps, bias current settings, uploaded tables.
PROPOSAL: Always print timestamp for the most recent timestamp when the TC row
          does not have a timestamp.
    PRO: Can potentially remove some commands.
    PROPOSAL: Label somehow.
        PROPOSAL: Separate column
        PROPOSAL: Prefix/suffix
        PROPOSAL: Prefix ">" i.e. "time more than".

so_read_modif_TC_csv.py   $(ls -1 solo_STP*/*.csv | grep -v BRIDGE_BACK) | lesr
'''


CSV_EXECUTION_TIME_FORMAT = '%Y-%jT%H:%M:%S.%f'

BASH_HELP_TEXT = """
Utility for reading TC .csv files
(e.g. solo_STP124_rpw_20201130_20201206_V01.csv).
Prints filtered, modified version of file to stdout.

Wrapper around python code.

Parameters: [-s -t -b] <csv files>


    -s           Include sweeps
    -t           Include load sweep table
    -b           Include bias current settings
    <csv files>  solo_STP129_rpw_20210104_20210110_V01.csv etc.
                 Files do not need to be sorted in time (code will sort files).
                 Asserts that files do not overlap in time.

    NOTE: No flag. ==> Display no information.

Script initially created 2020-11-26 by Erik P G Johansson.
"""


def bash_wrapper(argList):
    '''
    ~Bash wrapper meant to parse arguments suitable for being called from bash,
    i.e. all arguments are strings, Using conventional flags.


    Parameters
    ----------
    argList[iArg] = Argument string.
        [-s] [-t] [-b]
        Arbitrary list of paths to TC .csv files.
    '''

    #################
    # User help text
    #################
    if len(argList) == 1 and argList[0] == '--help':
        print(BASH_HELP_TEXT)

        # Return instead of quit(0) so that function can in principle be called
        # from other python code without quitting.
        return 0

    # ===============
    # Parse arguments
    # ===============
    includeSweeps         = False
    includeLoadSweepTable = False
    includeBiasSetting    = False
    while True:
        if not argList:
            # CASE: No more arguments left.
            break
        elif (argList[0] == '-s'):
            includeSweeps         = True
        elif (argList[0] == '-t'):
            includeLoadSweepTable = True
        elif (argList[0] == '-b'):
            includeBiasSetting    = True
        else:
            # CASE: There are arguments left, and the first (remaining)
            # argument is not a flag.
            break
        argList = argList[1:]

    fileList = argList
    del argList

    main(
        fileList,
        includeSweeps         = includeSweeps,
        includeLoadSweepTable = includeLoadSweepTable,
        includeBiasSetting    = includeBiasSetting,
    )


def main(
    fileList,
    includeSweeps         = False,
    includeLoadSweepTable = False,
    includeBiasSetting    = False,
):
    '''
    Main function

    Prints filtered & modified contents of specified files.
    Sorts the files in time before printing.
    Asserts that files do not overlap in time.
    '''

    fileDcList = []
    for filePath in fileList:
        fileDc = read_TC_csv_file(filePath)
        fileDc['filePath'] = filePath
        fileDcList.append(fileDc)

    # Sort files in (increasing) time order.
    fileDcList.sort(key=operator.itemgetter('DtFirst'))

    # ASSERT: Files do not overlap in time coverage.
    # (Assumes they are sorted in time internally.)
    for (fileDc1, fileDc2) in zip(fileDcList[:-1], fileDcList[1:]):
        filePath1 = fileDc1['filePath']
        filePath2 = fileDc2['filePath']
        assert fileDc1['DtLast'] < fileDc2['DtFirst'], \
            f'Files\n{filePath1}\nand\n{filePath2} overlap in time.'

    # ========================================
    # Derive max column width for every column
    # ========================================
    # Useful for printing.
    # NOTE: Derived for all rows, regardless of whether they are actually
    #       printed or not.
    # NOTE: Does NOT assume that files have the same column names.
    #
    # TODO-NI: Is this code needed?
    #     fileDc['colMaxWidthDc'] should contain the max column widths
    #     already.
    # colMaxWidthDc = {}
    # for fileDc in fileDcList:
    #     csvColMaxWidthDc = fileDc['colMaxWidthDc']
    #     for (colName, csvCmw) in csvColMaxWidthDc.items():
    #         #l = len(colMaxWidthDc[colName])
    #         if colName in colMaxWidthDc:
    #             cmw = max(csvCmw, csvColMaxWidthDc[colName])
    #         else:
    #             cmw = csvCmw
    #         colMaxWidthDc[colName] = cmw
    colMaxWidthDc = fileDc['colMaxWidthDc']

    # =================================
    # Print filtered and modified table
    # =================================
    for fileDc in fileDcList:
        s = '-' * len(fileDc['filePath'])
        print(s)
        print(fileDc['filePath'])   # DEBUG
        print(s)

        dataDc = fileDc['dataDc']

        for iRow in range(fileDc['nDataRows']):
            seqDescr = dataDc['Seq. Descr.'][iRow]
            descr    = dataDc['Description'][iRow]

            # Empirically:
            # ============
            # 'Configure and execute the BIAS sweep Part'
            #       gives sweep on one probe.
            #       Only "Part [123]" have been observed.
            #       NOTE: Sometimes "Part 2" is excluded.
            # 'Run BIAS Calibration Part'
            #       (substring) gives calibration sweep on one probe?!
            #       Only "Part [12]" have been observed.
            #
            # CP_DPU_BIA_SWEEP_STEP_NR
            #       preceedes loading sweep table.
            # CP_DPU_BIA_SWEEP_STEP_CUR
            #       gives sweep table na.
            # TC_DPU_LOAD_BIAS_SWEEP
            #       precedes sweep table and has the last time stamp before it.
            #
            # TC_DPU_SET_BIAS
            #       (substring) has the last timestamp before bias current
            #       setting on one probe.
            # CP_BIA_SET_BIAS
            #       (substring) gives bias current on one probe.
            #       Sometimes combined with "Raw" ==> non-readable(?) na.

            p = False   # Whether to print this row.
            if includeSweeps:
                # Includes both sweeps and calibration sweeps.
                p = p or re.fullmatch(
                    'Configure and execute the BIAS sweep Part .*',
                    seqDescr,
                )
                p = p or re.fullmatch(
                    'Run BIAS Calibration Part .*',
                    seqDescr,
                )
            if includeLoadSweepTable:
                p = p or 'CP_DPU_BIA_SWEEP_STEP_NR'    == descr
                p = p or 'CP_DPU_BIA_SWEEP_STEP_CUR'   == descr
                p = p or 'TC_DPU_LOAD_BIAS_SWEEP'      == descr
            if includeBiasSetting:
                # p = p or re.fullmatch('TC_DPU_SET_BIAS[123]', descr)
                p = p or re.fullmatch('CP_BIA_SET_BIAS[123]', descr)
            # p = p or 'CP_BIA_SET_MODE_SET_MX_MODE' == descr

            if p:
                print_row(dataDc, colMaxWidthDc, iRow)


def print_row(dataDc, colWidthDc, iRow):
    '''
    Print one-row representation of the content of one CSV row.

    Ex:
        2379, (2021-01-17)   2021-017T09:19:12.000000, ...
        2381, (2021-01-17) >=2021-017T09:19:12.000000, ...
    Date in brackets is the same date on other format.
    ">=" refers to that timestamp is copied from neareast earlier timestamp.
    First column i CSV row number.

    NOTE: Automatic column width-handling is likely inefficient, but not too
    inefficient.
    NOTE: Does not handle leap seconds.

    Arguments
    ---------
    colWidthDc :
    '''

    # Find nearest preceding row (or this row) with timestamp.
    execTimeStr    = ''
    iRowTimestamp  = iRow
    preExecTimeStr = ''
    while (not execTimeStr) & (iRowTimestamp >= 0):
        execTimeStr = dataDc['Execution Time'][iRowTimestamp]
        if execTimeStr:
            break
        else:
            preExecTimeStr = '>='
            iRowTimestamp -= 1

    if execTimeStr:
        Dt = datetime.datetime.strptime(
            execTimeStr,
            CSV_EXECUTION_TIME_FORMAT,
        )

        # YMD = Year-Month-Day (as opposed to Year-Doy)
        execTimeYmdStr = Dt.strftime('(%Y-%m-%d)')
    else:
        execTimeYmdStr = ''

    # =========================
    # Select and format columns
    # =========================
    strList = []
    strList.append(f'{iRow:4}')
    strList.append(
        '{0:12} {1:2}{2:{3}}'.format(
            execTimeYmdStr,
            preExecTimeStr,
            execTimeStr,
            colWidthDc['Execution Time'],
        ),
    )
    strList.append(
        '{0:{1}}'.format(
            dataDc['Description'][iRow],
            colWidthDc['Description'],
        ),
    )
    strList.append(
        '{0:{1}}'.format(
            dataDc['MD'][iRow],
            colWidthDc['MD'],
        ),
    )
    # IMPLEMENTATION NOTE: colWidthDc['SSID'] is much greater than necessary
    # for values actually printed (empirically).
    # Therefore uses hardcoded column width.
    strList.append(
        '{:7}'.format(
            dataDc['SSID'][iRow],
        ),
    )
    strList.append(
        '{0:{1}}'.format(
            dataDc['Seq. Descr.'][iRow],
            colWidthDc['Seq. Descr.'],
        ),
    )

    # =====
    # Print
    # =====
    s = ', '.join(strList)
    print(s)


def read_TC_csv_file(filePath):
    '''
    Wrapper around read_CSV_file() with more domain-specific code.

    Parameters
    ----------
    filePath

    Returns
    -------
    dict : dataDc extended with various related data.

    '''
    fileDc = read_CSV_file(filePath)

    execTimeStrList = fileDc['dataDc']['Execution Time']

    execTimeStr1 = next(s for s in execTimeStrList if s)
    execTimeStr2 = next(s for s in reversed(execTimeStrList) if s)

    DtFirst = datetime.datetime.strptime(
        execTimeStr1, CSV_EXECUTION_TIME_FORMAT,
    )
    DtLast  = datetime.datetime.strptime(
        execTimeStr2, CSV_EXECUTION_TIME_FORMAT,
    )
    assert DtFirst <= DtLast

    fileDcExtra = {
        'DtFirst':  DtFirst,
        'DtLast':   DtLast,
    }
    fileDc = {**fileDc, **fileDcExtra}

    return fileDc


def read_CSV_file(filePath):
    '''
    Somewhat generic CSV reader.

    Parameters
    ----------
    filePath

    Returns
    -------
    fileDc : Data and various metadata
        ['dataDc'][colName][iRow]
            String
        ['nDataRows']
        ['colMaxWidthDc'][colName]
            Scalar integer. Max number of characters for any string in this
            column.
    '''
    with open(filePath, newline='') as fileObj:
        fileReader = csv.reader(fileObj, delimiter=',', quotechar='"')

        # Create strListList[iRow][iCol]
        strListList = []
        for strList in fileReader:
            strListList.append(strList)
            # print(', '.join(rowList))

        colNameList = strListList[0]
        nCols     = len(strListList[0])
        nFileRows = fileReader.line_num   # Rows in CSV file, not rows of data.

    # ASSERTION: Unique column names.
    assert len(colNameList) == len(set(colNameList))

    # ASSERTION: Same number of columns on every row
    for iFileRow in range(1, nFileRows):
        nColsThisRow = len(strListList[iFileRow])
        assert nColsThisRow == nCols, \
            'Number of columns on row {} (0=first) (nColsThisRow={})'.format(
                iFileRow, nColsThisRow,
            )\
            + ' differs from preceding rows (nCols={}).\nFile: {}'.format(
                nCols, filePath,
            )

    # Create: dataDc[colName][iRow]
    dataDc        = {}
    colMaxWidthDc = {}
    for colName, iCol in zip(colNameList, range(nCols)):
        dataDc[colName] = [
            strListList[iCsvRow][iCol] for iCsvRow in range(1, nFileRows)
        ]
        colMaxWidthDc[colName] = max(len(s) for s in dataDc[colName])

    return {
        'dataDc':        dataDc,
        'nDataRows':     nFileRows-1,
        'colMaxWidthDc': colMaxWidthDc,
    }


# TEST
if 0:
    TEST_FILE_122 = \
        '/nonhome_data/SOLAR_ORBITER/data_roc/Timeline_apriori_csv/2020/' \
        'solo_STP122_rpw_20201116_20201122_V03/' \
        'solo_STP122_rpw_20201116_20201122_V03.csv'
    TEST_FILE_123 = \
        '/nonhome_data/SOLAR_ORBITER/data_roc/Timeline_apriori_csv/2020/' \
        'solo_STP123_rpw_20201123_20201129_V01/' \
        'solo_STP123_rpw_20201123_20201129_V01.csv'

    # Has quoted commas. ==> Must read quoted text.
    # Ex: "Configure the Bias (mode, and relay) "
    TEST_FILE_102 = \
        '/nonhome_data/SOLAR_ORBITER/data_roc/Timeline_apriori_csv/2020/' \
        'solo_STP102_rpw_20200629_20200705_V03/' \
        'solo_STP102_rpw_20200629_20200705_V03.csv'

    # main([TEST_FILE_122, TEST_FILE_123])
    # main([TEST_FILE_123, TEST_FILE_122])
    # main([TEST_FILE_102])
    main([
        '-b',
        '/nonhome_data/SOLAR_ORBITER/data_roc/Timeline_apriori_csv/2020/'
        'solo_STP1_rpw_20200224_20200301_V01/'
        'solo_STP1_rpw_Rolls_20200225_20200228.csv',
    ])

    # main([])
    # main([TEST_FILE_123, TEST_FILE_123])
