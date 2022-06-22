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
BOGIQ
=====
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

    fileDictList = []
    for filePath in fileList:
        fileDict = read_TC_csv_file(filePath)
        fileDict['filePath'] = filePath
        fileDictList.append(fileDict)

    # Sort files in (increasing) time order.
    fileDictList.sort(key=operator.itemgetter('DtFirst'))

    # ASSERT: Files do not overlap in time coverage.
    # (Assumes they are sorted in time internally.)
    for (fileDict1, fileDict2) in zip(fileDictList[:-1], fileDictList[1:]):
        filePath1 = fileDict1['filePath']
        filePath2 = fileDict2['filePath']
        assert fileDict1['DtLast'] < fileDict2['DtFirst'], \
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
    #     fileDict['colMaxWidthDict'] should contain the max column widths
    #     already.
    # colMaxWidthDict = {}
    # for fileDict in fileDictList:
    #     csvColMaxWidthDict = fileDict['colMaxWidthDict']
    #     for (colName, csvCmw) in csvColMaxWidthDict.items():
    #         #l = len(colMaxWidthDict[colName])
    #         if colName in colMaxWidthDict:
    #             cmw = max(csvCmw, csvColMaxWidthDict[colName])
    #         else:
    #             cmw = csvCmw
    #         colMaxWidthDict[colName] = cmw
    colMaxWidthDict = fileDict['colMaxWidthDict']

    # =================================
    # Print filtered and modified table
    # =================================
    for fileDict in fileDictList:
        s = '-' * len(fileDict['filePath'])
        print(s)
        print(fileDict['filePath'])   # DEBUG
        print(s)

        dataDict = fileDict['dataDict']

        for iRow in range(fileDict['nDataRows']):
            seqDescr = dataDict['Seq. Descr.'][iRow]
            descr    = dataDict['Description'][iRow]

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
            #       gives sweep table value.
            # TC_DPU_LOAD_BIAS_SWEEP
            #       precedes sweep table and has the last time stamp before it.
            #
            # TC_DPU_SET_BIAS
            #       (substring) has the last timestamp before bias current
            #       setting on one probe.
            # CP_BIA_SET_BIAS
            #       (substring) gives bias current on one probe.
            #       Sometimes combined with "Raw" ==> non-readable(?) value.

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
                print_row(dataDict, colMaxWidthDict, iRow)


def print_row(dataDict, colWidthDict, iRow):
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
    colWidthDict :
    '''

    # Find nearest preceding row (or this row) with timestamp.
    execTimeStr    = ''
    iRowTimestamp  = iRow
    preExecTimeStr = ''
    while (not execTimeStr) & (iRowTimestamp >= 0):
        execTimeStr = dataDict['Execution Time'][iRowTimestamp]
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
            colWidthDict['Execution Time'],
        ),
    )
    strList.append(
        '{0:{1}}'.format(
            dataDict['Description'][iRow],
            colWidthDict['Description'],
        ),
    )
    strList.append(
        '{0:{1}}'.format(
            dataDict['MD'][iRow],
            colWidthDict['MD'],
        ),
    )
    # IMPLEMENTATION NOTE: colWidthDict['SSID'] is much greater than necessary
    # for values actually printed (empirically).
    # Therefore uses hardcoded column width.
    strList.append(
        '{:7}'.format(
            dataDict['SSID'][iRow],
        ),
    )
    strList.append(
        '{0:{1}}'.format(
            dataDict['Seq. Descr.'][iRow],
            colWidthDict['Seq. Descr.'],
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
    dict : dataDict extended with various related data.

    '''
    fileDict = read_CSV_file(filePath)

    execTimeStrList = fileDict['dataDict']['Execution Time']

    execTimeStr1 = next(s for s in execTimeStrList if s)
    execTimeStr2 = next(s for s in reversed(execTimeStrList) if s)

    DtFirst = datetime.datetime.strptime(
        execTimeStr1, CSV_EXECUTION_TIME_FORMAT,
    )
    DtLast  = datetime.datetime.strptime(
        execTimeStr2, CSV_EXECUTION_TIME_FORMAT,
    )
    assert DtFirst <= DtLast

    fileDictExtra = {
        'DtFirst':  DtFirst,
        'DtLast':   DtLast,
    }
    fileDict = {**fileDict, **fileDictExtra}

    return fileDict


def read_CSV_file(filePath):
    '''
    Somewhat generic CSV reader.

    Parameters
    ----------
    filePath

    Returns
    -------
    fileDict : Data and various metadata
        ['dataDict'][colName][iRow]
            String
        ['nDataRows']
        ['colMaxWidthDict'][colName]
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
        assert nColsThisRow == nCols,\
            'Number of columns on row {} (0=first) (nColsThisRow={})'.format(
                iFileRow, nColsThisRow,
            )\
            + ' differs from preceding rows (nCols={}).\nFile: {}'.format(
                nCols, filePath,
            )

    # Create: dataDict[colName][iRow]
    dataDict        = {}
    colMaxWidthDict = {}
    for colName, iCol in zip(colNameList, range(nCols)):
        dataDict[colName] = [
            strListList[iCsvRow][iCol] for iCsvRow in range(1, nFileRows)
        ]
        colMaxWidthDict[colName] = max(len(s) for s in dataDict[colName])

    return {
        'dataDict':        dataDict,
        'nDataRows':       nFileRows-1,
        'colMaxWidthDict': colMaxWidthDict,
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
