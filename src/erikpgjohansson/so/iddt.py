# -*- coding: UTF-8 -*-
'''
Functionality for handling IDDT.


SHORTENINGS
===========
IDDT = IRFU (SolO) Datasets Directory Tree.
    "SolO" is excluded from the name since it is implicit from parent package "so".
    The way datasets are organized for SolO L2 & L3 datasets at IRFU.
    Directory paths: <instrument>/<DTDN>/<year>/<month>/<dataset file>.
DTDN = Data Type Directory Name
    Standardized (sub)directory name used for subset of DATASET_IDs in
    the "IRFU SolO data directory structure".
    Ex: lfr_wf_e


Initially created 2020-10-26 by Erik P G Johansson, IRF Uppsala, Sweden.
'''
'''
BOGIQ
=====
PROPOSAL: Better module name (so.*) or shortening for IDDT.
    PROPOSAL: irfudirtree
    PROPOSAL: irfudirstruct
    PROPOSAL: idds/iddt irfu datasets dir struct/tree
    ~irfu,~org,~data,~structure,~dir,~dir tree, ~format,~instrument data,~datasets
    PROPOSAL: Something that implies that it does not imply the directory
        trees that follow other convention: L1, L1R.
        PROPOSAL: ~type-sorted
        PROPOSAL: Module should (in principle) cover how all directory trees
            are organized, but have a separate acronym for the ~type-sorted
            directory trees L2, L3.
'''



import erikpgjohansson.so.utils
import erikpgjohansson.so.str
import os.path
import shutil



def get_IDDT_subdir(filename, dtdnInclInstrument=True, instrDirCase='lower'):
    '''
    Derive the relative subdirectory path used by IRFU-standarized directory
    structures for L1, L1R, L2 & L3 SolO datasets, given a dataset filename.

    NOTE: The current implementation is a temporary, awaiting the final one.
    NOTE: Can not handle HK (assertion) which also should not be needed.
          Could easily be extended to handle that.

    Initially created 2020-10-15 by Erik P G Johansson.


    Parameters
    ----------
    filename : String
        Dataset filename on any standard format

    Returns
    -------
    relative_directory path : String
        Relative directory path. Ex: 'RPW/L2/lfr_bp/2020/10'
        None : If can not parse filename.

    '''
    '''
    Examples of in-flight dataset filenames
    ---------------------------------------
    solo_HK_rpw-bia_20200301_V01.cdf                   # NOTE: No -cdag.
    solo_L2_rpw-lfr-surv-cwf-e-cdag_20200213_V01.cdf   # NOTE: -cdag.
    solo_L2_mag-rtn-burst_20200601_V02.cdf
    solo_L2_mag-rtn-normal-1-minute_20200601_V02.cdf
    solo_L2_mag-rtn-normal_20200601_V02.cdf
    solo_L2_mag-srf-burst_20200601_V02.cdf
    solo_L2_epd-step-burst_20200703T232228-20200703T233728_V02.cdf
    solo_L1_epd-sis-b-hehist_20200930_V01.cdf
    solo_L2_swa-eas1-nm3d-psd_20200708T060012-20200708T120502_V01.cdf
    '''
    '''
    PROPOSAL: DATASET_ID+time (year+month) as argument?
        PRO: Entire filename is not used (version, cdag)
    '''
    assert type(dtdnInclInstrument) == bool

    d = erikpgjohansson.so.utils.parse_dataset_filename(filename)
    if not d:
        return None
    datasetId = d['DATASET_ID']
    tv1       = d['time vector 1']

    (junk, level, instrument, descriptor) = erikpgjohansson.so.utils.parse_DATASET_ID(datasetId)

    yearStr  = '{:04}'.format(tv1[0])
    monthStr = '{:02}'.format(tv1[1])
    domStr   = '{:02}'.format(tv1[2])
    if   instrDirCase == 'upper':
        instrDirName = instrument.upper()    # .lower() really unnecessary.
    elif instrDirCase == 'lower':
        instrDirName = instrument.lower()
    else:
        raise Exception('Illegal argument value instrDirCase={0}'.format(instrDirCase))



    if level in ['L2', 'L3']:
        dtdn = erikpgjohansson.so.iddt.convert_DATASET_ID_to_DTDN(
            datasetId, includeInstrument=dtdnInclInstrument)
        return os.path.join(instrDirName, level, dtdn, yearStr, monthStr)
    elif level in ['L1', 'L1R']:
        return os.path.join(instrDirName, level, yearStr, monthStr, domStr)
    else:
        # NOTE: Includes HK.
        raise Exception('Can not generate IDDT subdirectory for level={0}.'.format(level))







def convert_DATASET_ID_to_DTDN(datasetId, includeInstrument=False):
    '''
    Convert DATASET_ID --> DTDN
    '''

    # DTDN = Data Type Directory Name
    # [i][j][0] = DATASET_ID
    # [i][j][1] = DTDN
    L2_L3_DSI_TO_DTDN = (
        ({'SOLO_L2_RPW-LFR-SURV-CWF-B',
          'SOLO_L2_RPW-LFR-SURV-SWF-B'},  'lfr_wf_b'),
        ({'SOLO_L2_RPW-LFR-SBM1-CWF-E',
          'SOLO_L2_RPW-LFR-SBM2-CWF-E',
          'SOLO_L2_RPW-LFR-SURV-CWF-E',
          'SOLO_L2_RPW-LFR-SURV-CWF-E-1-SECOND',
          'SOLO_L2_RPW-LFR-SURV-SWF-E'},  'lfr_wf_e'),
        ({'SOLO_L2_RPW-LFR-SURV-ASM'},    'lfr_asm'),
        ({'SOLO_L2_RPW-LFR-SURV-BP1',
          'SOLO_L2_RPW-LFR-SURV-BP2'},    'lfr_bp'),
        ({'SOLO_L2_RPW-TDS-LFM-CWF-B',
          'SOLO_L2_RPW-TDS-LFM-CWF-E',
          'SOLO_L2_RPW-TDS-LFM-RSWF-B',
          'SOLO_L2_RPW-TDS-LFM-RSWF-E',
          'SOLO_L2_RPW-TDS-LFM-PSDSM'}, 'tds_lfm'),   # AMBIGUOUS for some: tds_lfm or tds_wf_b/e
        ({'SOLO_L2_RPW-TDS-SURV-HIST1D'}, 'tds_hist1d'),
        ({'SOLO_L2_RPW-TDS-SURV-HIST2D'}, 'tds_hist2d'),
        ({'SOLO_L2_RPW-TDS-SURV-MAMP'},   'tds_mamp'),
        ({'SOLO_L2_RPW-TDS-SURV-STAT'},   'tds_stat'),
        ({'SOLO_L2_RPW-TDS-SURV-RSWF-B',
          'SOLO_L2_RPW-TDS-SURV-TSWF-B'}, 'tds_wf_b'),
        ({'SOLO_L2_RPW-TDS-SURV-RSWF-E',
          'SOLO_L2_RPW-TDS-SURV-TSWF-E'}, 'tds_wf_e'),
        ({'SOLO_L2_RPW-HFR-SURV',
          'SOLO_L2_RPW-TNR-SURV'},        'thr'),
        ({'SOLO_L3_RPW-TNR-FP'},          'tnr_fp'),
        ({'SOLO_L3_RPW-BIA-EFIELD',
          'SOLO_L3_RPW-BIA-EFIELD-10-SECONDS'},  'lfr_efield'),
        ({'SOLO_L3_RPW-BIA-SCPOT',
          'SOLO_L3_RPW-BIA-SCPOT-10-SECONDS'},   'lfr_scpot'),
        ({'SOLO_L3_RPW-BIA-DENSITY',
          'SOLO_L3_RPW-BIA-DENSITY-10-SECONDS'}, 'lfr_density'))



    (dataSrc, level, instrument, descriptor
     ) = erikpgjohansson.so.utils.parse_DATASET_ID(datasetId)



    # ASSERTIONS
    if not datasetId.upper() == datasetId:
        raise Exception('Not uppercase datasetId="{0}"'.format(
            datasetId))
    if not level in set(['L2', 'L3']):
        raise Exception('Can not generate DTDN for level={0}, datasetId="{1}"'.format(
            level, datasetId))



    # __IF__ a special case applies (RPW L2, L3), then handle that.
    for (dsiSet, dtdn) in L2_L3_DSI_TO_DTDN:
        if datasetId in dsiSet:
            return dtdn    # NOTE: EXIT
    # ASSERTION: Previous handling of special cases has already handled
    # aLL RPW L2+L3 cases.
    if instrument == 'RPW' and level in ['L2', 'L3']:
        raise Exception('Can not handle datasetId="{}".'.format(datasetId))



    # CASE: No special case DATASET_ID --> DTDN

    # Derive DTDN from DATASET_ID descriptor.
    (substrList, remainingStr, isPerfectMatch) = erikpgjohansson.so.str.regexp_str_parts(
        descriptor, ['[A-Z]+', '-', '[A-Z0-9-]+'], 1, 'permit non-match')

    if not isPerfectMatch:
        raise Exception('Can not handle datasetId="{}".'.format(datasetId))

    if includeInstrument:
        dtdn = descriptor.lower()
    else:
        dtdn = substrList[2].lower()
    return dtdn







def copy_move_datasets_to_irfu_dir_tree(mode, sourceDir, destDir,
                                        dirCreationPermissions=0o775,
                                        dtdnInclInstrument=True,
                                        instrDirCase='lower'):
    '''
~Utility

Copy/move files to IRFU-standardized destination subdirectories under
specified destination root directory. Destination directories will be created
if not pre-existing.

NOTE: Created destination directories might not have the desired file
permissions.
NOTE: Can handle that source and destination directories are identical when
moving. The function can thus be used for reorganizing a pre-existing directory
structure.
    NOTE: Old directories will be kept, even if empty.
NOTE: Prints log messages.
NOTE: Can be useful when called separately from bash wrapper to automatically
organize/re-organize existing local datasets.


POSSIBLE ~BUG
=============
2020-10-30, nas24, brain, so_bicas_batch_cron manual
--
Can not copy files in large numbers on (at least) brain. Moving seems to always
work.
--
Copying file: /data/solo/data_irfu//generated_2020-10-30_20.08.43_manual/solo_L3_rpw-bia-efield-10-seconds_20200802_V01.cdf              --> /data/solo/data_irfu/latest/RPW/L3/bia-efield-10-seconds/2020/08
Copying file: /data/solo/data_irfu//generated_2020-10-30_20.08.43_manual/solo_L3_rpw-bia-scpot-10-seconds_20200807_V01.cdf               --> /data/solo/data_irfu/latest/RPW/L3/bia-scpot-10-seconds/2020/08
Traceback (most recent call last):
  File "/home/erjo/bin/global/so_copy_move_datasets_to_IRFU_dir_tree.py", line 49, in <module>
    main(sys.argv[1:])   # sys.argv[1:]        # NOTE: sys.argv[0] är inget CLI-argument. ==> Ignorera
  File "/home/erjo/bin/global/so_copy_move_datasets_to_IRFU_dir_tree.py", line 45, in main
    erikpgjohansson.so.iddt.copy_move_datasets_to_irfu_dir_tree(*argument_list)
  File "/home/erjo/python_copy/erikpgjohansson/so/iddt.py", line 344, in copy_move_datasets_to_irfu_dir_tree
    copy_move_file_fh(old_path, new_dir_path)
  File "/home/erjo/python_copy/erikpgjohansson/so/iddt.py", line 307, in <lambda>
    copy_file, 'Copying', old_path, new_dir_path)
  File "/home/erjo/python_copy/erikpgjohansson/so/iddt.py", line 301, in copy_move_file
    cm_func(old_path, new_dir_path)
  File "/home/erjo/python_copy/erikpgjohansson/so/iddt.py", line 288, in copy_file
    shutil.copy(old_path, new_dir_path)
  File "/usr/lib/python3.5/shutil.py", line 235, in copy
    copyfile(src, dst, follow_symlinks=follow_symlinks)
  File "/usr/lib/python3.5/shutil.py", line 115, in copyfile
    with open(dst, 'wb') as fdst:
PermissionError: [Errno 13] Permission denied: '/data/solo/data_irfu/latest/RPW/L3/bia-scpot-10-seconds/2020/08/solo_L3_rpw-bia-scpot-10-seconds_20200807_V01.cdf'
--
THEORY: NAS24 does not permit writing at random times.
NOTE: "Starting from Python 3.8 all functions involving a file copy
(copyfile(), copy(), copy2(), copytree(), and move()) may use
platform-specific “fast-copy” syscalls in order to copy the file more
efficiently (see bpo-33671). “fast-copy” means that the copying operation
occurs within the kernel, avoiding the use of userspace buffers in Python
as in “outfd.write(infd.read())”."
    https://docs.python.org/3/library/shutil.html#shutil-platform-dependent-efficient-copy-operations
NOTE: brain has python 3.5.2:


Parameters
----------
mode : String constant.
    'copy' or 'move'.
sourceDir : String
    Directory which will be searched RECURSIVELY for datasets.
    Non-parsable filenames will be ~ignored (no error; logged on stdout).
destDir   : String
    Can be identical to sourceDir.
dirCreationPermissions : Integer.
    File permissions for created directories (octal).
    Unclear what is appropriate.


Returns
-------
None.

'''
    '''
    ~PROBLEM: Not obvious how to log. Log paths? Just filenames? One/both?
    PROPOSAL: Log arguments.
        PROPOSAL: Log arguments in bash wrapper.
    PROPOSAL: Permit source FILES.
    PROPOSAL: Permit arbitrary number of source dirs/files.
    PROPOSAL: Handle file-writing bug.
        PROPOSAL: Some way of handling that copy/move might fail.
            PROPOSAL: Save failure in list and continue. Try again later.
        PROPOSAL: Use other copying function? Something closer to platform.
            NOTE: brain has python 3.5.2:
    PROPOSAL: Somehow ensure that destination is ~correct. Right level.
        PROPOSAL: Require instrument directory to pre-exist.
            Ex: RPW/
    PROPOSAL: Sort the copying order. Can be random (at least for real cases).

    BUG: 2020-11-06: Has failed twice when trying to copy and overwrite (not move and overwrite
    many datasets BIAS L2 & L3 (entire mission) on brain:nas24. Used shutil.copy().
        PROPOSAL: Detect failure.
            PROPOSAL: Wait and try again.
            PROPOSAL: Try copying that file later. Continue copying other files.
            PROPOSAL: Ultimately general timeout for max time waiting (truly idle).
        PROPOSAL: ~General function that copies with tricks.
        PROPOSAL: Temporary copy which is then moved.
    '''
    # ASSERTIONS
    if dirCreationPermissions > 0o777:
        # Useful for catch if mistakenly using hex literal instead of octal.
        raise Exception('Illegal dirCreationPermissions.')
    erikpgjohansson.so.asserts.is_dir(sourceDir)
    # NOTE: Without this assertion, the function will create the destination
    # directory (not just all the subdirectories)
    erikpgjohansson.so.asserts.is_dir(destDir)



    def copy_file(oldPath, newDirPath):
        # Should be able to handle:
        # * Old & new path being identical.
        # * Overwriting destination file.

        newPath = os.path.join(newDirPath, os.path.basename(oldPath))
        oldPath = os.path.realpath(oldPath)
        newPath = os.path.realpath(newPath)
        if oldPath == newPath:
            print('    Skipping unnecessary copy to itself.')
            return

        if 0:
            # Can not handle copying to itself (is that bad?!).
            shutil.copy(oldPath, newDirPath)
            #shutil.copy(oldPath, newPath)
        else:
            # EXPERIMENTAL: Attemp at bugfix for failing randomly when copying
            # many datasets (brain:nas24).
            # 2020-11-06: Does not seem to help.
            # IMPLEMENTATION NOTE: "cp" can not handle source and destination
            # being the same.
            # NOTE: Can not handle paths with apostrophe.
            cmd = "cp '{0}' '{1}'".format(oldPath, newDirPath)
            errorCode = os.system(cmd)
            if errorCode != 0:
                raise Exception('Copy command failed: {0}'.format(cmd))

    def move_file(oldPath, newDirPath):
        # NOTE: os.replace() more cross-platform than os.rename().
        # NOTE: Can handle old & new path being identical.
        # NOTE: os.rename requires destination to also be a file.
        newPath = os.path.join(newDirPath, os.path.basename(oldPath))
        os.replace(oldPath, newPath)

    # verbStr
    #   String to be printed: "Copying", "Moving".
    # maxLenOp : implicit argument.
    def copy_move_file(cmFunc, verbStr, oldPath, newDirPath):
        print('{0} file: {1:<{3}} --> {2}'.format(
            verbStr, oldPath, newDirPath, maxLenOp))
        cmFunc(oldPath, newDirPath)



    if mode == 'copy':
        copyMoveFileFh = lambda oldPath, newDirPath : copy_move_file(
            copy_file, 'Copying', oldPath, newDirPath)
    elif mode == 'move':
        copyMoveFileFh = lambda oldPath, newDirPath : copy_move_file(
            move_file, 'Moving', oldPath, newDirPath)
    else:
        raise Exception('Illegal mode="{}".'.format(mode))

    '''=================================================
    Collect directories to create and files to move/copy
    ====================================================
    IMPLEMENTATION NOTE: Not creating directories and moving files immediately
    in order to first find errors in order to avoid interrupting the procedure
    half-way.
    Ex: Finding DATASET_IDs that the code can not handle.'''
    pathTable = []
    maxLenOp = 0
    for (oldDirPath, _dirnameList, filenameList) in os.walk(sourceDir):
        for filename in filenameList:
            oldPath = os.path.join(oldDirPath, filename)
            maxLenOp = max(len(oldPath), maxLenOp)

            relDirPath = erikpgjohansson.so.iddt.get_IDDT_subdir(
                filename,
                dtdnInclInstrument=dtdnInclInstrument,
                instrDirCase=instrDirCase)
            if relDirPath:
                newDirPath = os.path.join(destDir, relDirPath)
                newPath    = os.path.join(newDirPath, filename)

                pathTable.append((newDirPath, oldPath, newPath))

            else:
                print('Can not identify file and therefore not copy/move it: {0}'.format(
                    oldPath))

    '''==============================
    Create directories and move files
    =============================='''
    for (newDirPath, oldPath, newPath) in pathTable:
        # NOTE: Can handle pre-existing destination directory.
        os.makedirs(newDirPath, mode=dirCreationPermissions, exist_ok=True)
        copyMoveFileFh(oldPath, newDirPath)
