'''
Functionality for handling IDDT.

See MISC_CONVENTIONS for shortenings.

Initially created 2020-10-26 by Erik P G Johansson, IRF Uppsala, Sweden.
'''


import dataclasses
import erikpgjohansson.solo.asserts
import erikpgjohansson.solo.utils
import erikpgjohansson.solo.str
import itertools
import logging
import os.path
import shutil


'''
BOGIQ
=====
PROPOSAL: Better module name (so.*) or shortening for IDDT.
    PROPOSAL: irfudirtree
    PROPOSAL: irfudirstruct
    PROPOSAL: idds/iddt irfu datasets dir struct/tree
        ~irfu, ~org, ~data, ~structure, ~dir, ~dir tree, ~format, ~instrument
        ~datasets
    PROPOSAL: Something that implies that it does not imply the directory
        trees that follow other convention: L1, L1R.
        PROPOSAL: ~type-sorted
        PROPOSAL: Module should (in principle) cover how all directory trees
            are organized, but have a separate acronym for the ~type-sorted
            directory trees L2, L3.
'''


@dataclasses.dataclass(frozen=True)
class DtdnEntry:
    setDsid : set
    dtdn : str

    def __post_init__(self):
        # setDsid
        assert type(self.setDsid) is set
        assert self.setDsid    # Assert not empty.
        for dsid in self.setDsid:
            # NOTE: Using erikpgjohansson.solo.utils.parse_DSID() as an
            # assertion on DSID.
            _, level, instrument, descriptor = \
                _ = erikpgjohansson.solo.utils.parse_DSID(dsid)
            assert level in ['L2', 'L3']
            # NOTE: Does not assert instrument, since might want special
            # treatment of non-RPW datasets too.

        assert type(self.dtdn) is str


_LS_DTDN_ENTRIES = (
    #######
    #  L2
    #######
    DtdnEntry(
        {
            'SOLO_L2_RPW-LFR-SURV-CWF-B',
            'SOLO_L2_RPW-LFR-SURV-SWF-B',
        }, 'lfr_wf_b',
    ),
    DtdnEntry(
        {
            'SOLO_L2_RPW-LFR-SURV-CWF-E',
            'SOLO_L2_RPW-LFR-SURV-CWF-E-1-SECOND',
            'SOLO_L2_RPW-LFR-SURV-SWF-E',
        }, 'lfr_wf_e',
    ),
    DtdnEntry({'SOLO_L2_RPW-LFR-SBM1-CWF-E'}, 'lfr_sbm1_wf_e'),
    DtdnEntry({'SOLO_L2_RPW-LFR-SBM2-CWF-E'}, 'lfr_sbm2_wf_e'),
    DtdnEntry({'SOLO_L2_RPW-LFR-SURV-ASM'}, 'lfr_asm'),
    DtdnEntry(
        {
            'SOLO_L2_RPW-LFR-SURV-BP1',
            'SOLO_L2_RPW-LFR-SURV-BP2',
        }, 'lfr_bp',
    ),
    DtdnEntry(
        {
            'SOLO_L2_RPW-TDS-LFM-CWF-B',
            'SOLO_L2_RPW-TDS-LFM-CWF-E',
            'SOLO_L2_RPW-TDS-LFM-RSWF-B',
            'SOLO_L2_RPW-TDS-LFM-RSWF-E',
            'SOLO_L2_RPW-TDS-LFM-PSDSM',
        }, 'tds_lfm',
    ),  # AMBIGUOUS for some: tds_lfm or tds_wf_b/e
    DtdnEntry({'SOLO_L2_RPW-TDS-SURV-HIST1D'}, 'tds_hist1d'),
    DtdnEntry({'SOLO_L2_RPW-TDS-SURV-HIST2D'}, 'tds_hist2d'),
    DtdnEntry({'SOLO_L2_RPW-TDS-SURV-MAMP'}, 'tds_mamp'),
    DtdnEntry({'SOLO_L2_RPW-TDS-SURV-STAT'}, 'tds_stat'),
    DtdnEntry(
        {
            'SOLO_L2_RPW-TDS-SURV-RSWF-B',
            'SOLO_L2_RPW-TDS-SURV-TSWF-B',
        }, 'tds_wf_b',
    ),
    DtdnEntry(
        {
            'SOLO_L2_RPW-TDS-SURV-RSWF-E',
            'SOLO_L2_RPW-TDS-SURV-TSWF-E',
        }, 'tds_wf_e',
    ),
    DtdnEntry(
        {
            'SOLO_L2_RPW-HFR-SURV',
            'SOLO_L2_RPW-TNR-SURV',
        }, 'thr',
    ),
    #######
    #  L3
    #######
    DtdnEntry({'SOLO_L3_RPW-TNR-FP'}, 'tnr_fp'),
    DtdnEntry(
        {
            'SOLO_L3_RPW-BIA-EFIELD',
            'SOLO_L3_RPW-BIA-EFIELD-10-SECONDS',
        }, 'lfr_efield',
    ),
    DtdnEntry(
        {
            'SOLO_L3_RPW-BIA-SCPOT',
            'SOLO_L3_RPW-BIA-SCPOT-10-SECONDS',
        }, 'lfr_scpot',
    ),
    DtdnEntry(
        {
            'SOLO_L3_RPW-BIA-DENSITY',
            'SOLO_L3_RPW-BIA-DENSITY-10-SECONDS',
        }, 'lfr_density',
    ),
)
'''Data structure that tabulates how to convert DSID-->DTDN for
special cases, including all RPW(?) cases. A general rule is used if the
conversion is not tabulated here.

NOTE: Can only convert DSID-->DTDN for L2 & L3.

[i][j][0] = Set of DSIDs
[i][j][1] = DTDN associated with above set of DSIDs.
'''

# ASSERT: _LS_DTDN_ENTRIES: No duplicated DSIDs.
lsDsid = tuple(
    itertools.chain.from_iterable(dtdne.setDsid for dtdne in _LS_DTDN_ENTRIES),
)
assert len(lsDsid) == len(set(lsDsid))


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
    PROPOSAL: DSID+time (year+month) as argument?
        PRO: Entire filename is not used (version, cdag)
    '''
    assert type(dtdnInclInstrument) is bool

    d = erikpgjohansson.solo.utils.parse_dataset_filename(filename)
    if not d:
        return None
    dsid = d['DSID']
    tv1  = d['time vector 1']

    junk, level, instrument, descriptor = \
        erikpgjohansson.solo.utils.parse_DSID(dsid)

    yearStr  = f'{tv1[0]:04}'
    monthStr = f'{tv1[1]:02}'
    domStr   = f'{tv1[2]:02}'   # DOM = Day-Of-Month
    if instrDirCase == 'upper':
        instrDirName = instrument.upper()    # .lower() really unnecessary.
    elif instrDirCase == 'lower':
        instrDirName = instrument.lower()
    else:
        raise Exception(f'Illegal argument value instrDirCase={instrDirCase}')

    if level in ['L2', 'L3']:
        dtdn = erikpgjohansson.solo.iddt.convert_DSID_to_DTDN(
            dsid, includeInstrument=dtdnInclInstrument,
        )
        return os.path.join(instrDirName, level, dtdn, yearStr, monthStr)
    elif level in ['LL02', 'LL03', 'L1', 'L1R']:
        return os.path.join(instrDirName, level, yearStr, monthStr, domStr)
    else:
        # NOTE: Includes HK.
        raise Exception(
            f'Can not generate IDDT subdirectory for level="{level}".',
        )


def convert_DSID_to_DTDN(dsid, includeInstrument=False):
    '''
    Convert DSID --> DTDN.

    Only applies to L2 & L3 since DTDNs are only defined for L2 & L3.

    Arguments:
        includeInstrument
            Whether non-RPW DTDNs should include the instrument name.
            Ex: "srf-burst" vs "mag-srf-burst".

    Return value:
        DTDN directory name
    '''
    # ASSERTIONS: Arguments
    if not dsid.upper() == dsid:
        raise Exception(f'Not uppercase dsid="{dsid}"')
    assert type(includeInstrument) is bool

    dataSrc, level, instrument, descriptor = \
        erikpgjohansson.solo.utils.parse_DSID(dsid)

    # ASSERTION: L2 or L3
    if level not in {'L2', 'L3'}:
        raise Exception(
            'Can not generate DTDN for level={level}, dsid="{dsid}"',
        )

    # __IF__ a tabulated special case applies, then handle that.
    for dtdne in _LS_DTDN_ENTRIES:
        if dsid in dtdne.setDsid:
            return dtdne.dtdn    # NOTE: EXIT

    # ASSERTION
    if instrument == 'RPW':
        raise Exception(f'Can not handle dsid="{dsid}".')

    ################
    # CASE: Not RPW
    ################
    # Previous handling of special cases has already handled all RPW cases.
    # ==> _LS_DTDN_ENTRIES must describe all relevant RPW DSIDs.

    # CASE: No special case applies when converting DSID --> DTDN
    # Derive DTDN from DSID descriptor using general rule.
    substrList, remainingStr, isPerfectMatch = \
        erikpgjohansson.solo.str.regexp_str_parts(
            descriptor, ['[A-Z]+', '-', '[A-Z0-9-]+'], 1, 'permit non-match',
        )

    if not isPerfectMatch:
        raise Exception(f'Can not handle dsid="{dsid}".')

    if includeInstrument:
        dtdn = descriptor.lower()
    else:
        dtdn = substrList[2].lower()

    return dtdn


def copy_move_datasets_to_IRFU_dir_tree(
    mode, sourceDir, destDir,
    dirCreationPermissions=0o775,
    dtdnInclInstrument=True,
    instrDirCase='lower',
):
    '''
    ~Utility

    Copy/move files to IRFU-standardized destination subdirectories under
    specified destination root directory. Destination directories will be
    created if not pre-existing.

    NOTE: Created destination directories might not have the desired file
          permissions.
    NOTE: Can handle that source and destination directories are identical when
          moving. The function can thus be used for reorganizing a pre-existing
          directory structure.
        NOTE: Old directories will be kept, even if empty.
    NOTE: Prints log messages.
    NOTE: Can be useful when called separately from bash wrapper to
          automatically organize/re-organize existing local datasets.


    POSSIBLE ~BUG
    =============
    2020-10-30, nas24, brain, so_bicas_batch_cron manual
    --
    Can not copy files in large numbers on (at least) brain. Moving seems to
    always work.
    --
    Copying file: /data/solo/data_irfu//generated_2020-10-30_20.08.43_manual/
    solo_L3_rpw-bia-efield-10-seconds_20200802_V01.cdf
    --> /data/solo/data_irfu/latest/RPW/L3/bia-efield-10-seconds/2020/08
    Copying file: /data/solo/data_irfu//generated_2020-10-30_20.08.43_manual/
    solo_L3_rpw-bia-scpot-10-seconds_20200807_V01.cdf
    --> /data/solo/data_irfu/latest/RPW/L3/bia-scpot-10-seconds/2020/08
    Traceback (most recent call last):
      File "/home/erjo/bin/global/so_copy_move_datasets_to_IRFU_dir_tree.py",
      line 49, in <module>
        main(sys.argv[1:])   # sys.argv[1:]
        # NOTE: sys.argv[0] är inget CLI-argument. ==> Ignorera
      File "/home/erjo/bin/global/so_copy_move_datasets_to_IRFU_dir_tree.py",
      line 45, in main
        erikpgjohansson.so.iddt.copy_move_datasets_to_irfu_dir_tree(*argument_list)
      File "/home/erjo/python_copy/erikpgjohansson/so/iddt.py", line 344,
      in copy_move_datasets_to_irfu_dir_tree
        copy_move_file_fh(old_path, new_dir_path)
      File "/home/erjo/python_copy/erikpgjohansson/so/iddt.py", line 307,
      in <lambda>
        copy_file, 'Copying', old_path, new_dir_path)
      File "/home/erjo/python_copy/erikpgjohansson/so/iddt.py", line 301,
      in copy_move_file
        cm_func(old_path, new_dir_path)
      File "/home/erjo/python_copy/erikpgjohansson/so/iddt.py", line 288,
      in copy_file
        shutil.copy(old_path, new_dir_path)
      File "/usr/lib/python3.5/shutil.py", line 235, in copy
        copyfile(src, dst, follow_symlinks=follow_symlinks)
      File "/usr/lib/python3.5/shutil.py", line 115, in copyfile
        with open(dst, 'wb') as fdst:
    PermissionError: [Errno 13] Permission denied:
    '/data/solo/data_irfu/latest/RPW/L3/bia-scpot-10-seconds/2020/08/
    solo_L3_rpw-bia-scpot-10-seconds_20200807_V01.cdf'
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

    BUG: 2020-11-06: Has failed twice when trying to copy and overwrite (not
                     move and overwrite many datasets BIAS L2 & L3 (entire
                     mission) on brain:nas24. Used shutil.copy().
        PROPOSAL: Detect failure.
            PROPOSAL: Wait and try again.
            PROPOSAL: Try copying that file later. Continue copying other
                      files.
            PROPOSAL: Ultimately general timeout for max time waiting (truly
                      idle).
        PROPOSAL: ~General function that copies with tricks.
        PROPOSAL: Temporary copy which is then moved.
    '''
    L = logging.getLogger(__name__)

    # ASSERTIONS
    if dirCreationPermissions > 0o777:
        # Useful for catch if mistakenly using hex literal instead of octal.
        raise Exception('Illegal dirCreationPermissions.')
    erikpgjohansson.solo.asserts.is_dir(sourceDir)
    # NOTE: Without this assertion, the function will create the destination
    # directory (not just all the subdirectories)
    erikpgjohansson.solo.asserts.is_dir(destDir)

    def copy_file(oldPath, newDirPath):
        # Should be able to handle:
        # * Old & new path being identical.
        # * Overwriting destination file.

        newPath = os.path.join(newDirPath, os.path.basename(oldPath))
        oldPath = os.path.realpath(oldPath)
        newPath = os.path.realpath(newPath)
        if oldPath == newPath:
            L.info('    Skipping unnecessary copy to itself.')
            return

        if 0:
            # Can not handle copying to itself (is that bad?!).
            shutil.copy(oldPath, newDirPath)
            # shutil.copy(oldPath, newPath)
        else:
            # EXPERIMENTAL: Attempt at bugfix for failing randomly when copying
            # many datasets (brain:nas24).
            # 2020-11-06: Does not seem to help.
            # IMPLEMENTATION NOTE: "cp" can not handle source and destination
            # being the same.
            # NOTE: Can not handle paths with apostrophe.
            cmd = f"cp '{oldPath}' '{newDirPath}'"
            errorCode = os.system(cmd)
            if errorCode != 0:
                raise Exception(f'Copy command failed: {cmd}')

    def move_file(oldPath, newDirPath):
        # NOTE: os.replace() is more cross-platform than os.rename().
        # NOTE: Can handle old & new path being identical.
        # NOTE: os.rename requires destination to also be a file.
        newPath = os.path.join(newDirPath, os.path.basename(oldPath))
        os.replace(oldPath, newPath)

    # verbStr
    #   String to be printed: "Copying", "Moving".
    # maxLenOp : implicit argument.
    def copy_move_file(cmFunc, verbStr, oldPath, newDirPath):
        L.info(f'{verbStr} file: {oldPath:<{maxLenOp}} --> {newDirPath}')

        cmFunc(oldPath, newDirPath)

    if mode == 'copy':
        # copyMoveFileFh = lambda oldPath, newDirPath : copy_move_file(
        #     copy_file, 'Copying', oldPath, newDirPath,
        # )
        def copyMoveFileFh(oldPath, newDirPath):
            copy_move_file(
                copy_file, 'Copying', oldPath, newDirPath,
            )
    elif mode == 'move':
        # copyMoveFileFh = lambda oldPath, newDirPath : copy_move_file(
        #     move_file, 'Moving', oldPath, newDirPath,
        # )
        def copyMoveFileFh(oldPath, newDirPath):
            copy_move_file(move_file, 'Moving', oldPath, newDirPath)
    else:
        raise Exception(f'Illegal mode="{mode}".')

    '''=================================================
    Collect directories to create and files to move/copy
    ====================================================
    IMPLEMENTATION NOTE: Not creating directories and moving files immediately
    in order to first find errors in order to avoid interrupting the procedure
    half-way.
    Ex: Finding DSIDs that the code can not handle.'''
    pathTable = []
    maxLenOp = 0
    for (oldDirPath, _dirnameList, filenameList) in os.walk(sourceDir):
        for filename in filenameList:
            oldPath = os.path.join(oldDirPath, filename)
            maxLenOp = max(len(oldPath), maxLenOp)

            relDirPath = erikpgjohansson.solo.iddt.get_IDDT_subdir(
                filename,
                dtdnInclInstrument=dtdnInclInstrument,
                instrDirCase=instrDirCase,
            )
            if relDirPath:
                newDirPath = os.path.join(destDir, relDirPath)
                newPath    = os.path.join(newDirPath, filename)

                pathTable.append((newDirPath, oldPath, newPath))

            else:
                L.info(
                    f'Can not identify file and therefore'
                    f' not copy/move it: {oldPath}',
                )

    # =================================
    # Create directories and move files
    # =================================
    for (newDirPath, oldPath, newPath) in pathTable:
        # NOTE: Can handle pre-existing destination directory.
        os.makedirs(newDirPath, mode=dirCreationPermissions, exist_ok=True)
        copyMoveFileFh(oldPath, newDirPath)
