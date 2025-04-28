'''
Module for the DST class.
'''


import codetiming
import collections
import datetime
import erikpgjohansson.solo.asserts
import erikpgjohansson.solo.metadata
import erikpgjohansson.solo.soar.utils
import logging
import numpy as np
import os


'''
'''


class DatasetsTable:
    '''Immutable "datasets table". Stores table of datasets.

    Encapsulates a dictionary in which each key is a string and each value
    is a 1D numpy array where all values have the same array shape.
    '''
    '''
    PROPOSAL: Change name of class.
        PRO: Is more generic than for using for datasets only.
        PRO: Avoid e.g. SDT_DST = SOAR Datasets Table DataSets Table
        PROPOSAL: Something more generic.
            ~Table, ~Dictionary, ~Arrays, ~Numpy, ~1D, ~List
            NAD=Numpy Array (1D) Dictionary
            DNA=Dictionary of Numpy Arrays
            NAT=Numpy Array Table
            TNA=Table of/using Numpy Arrays

    PROPOSAL: Change name "index()".
        get_subset()
        get_indices()
        PROPOSAL: Use __getitem__()
            PROPOSAL: Only for ranges.
    PROPOSAL: Change name "n()". -- DONE
        size()
            PRO: Consistent with NAs.
            CON: Can be conflated with number of keys/columns.
        n_rows()
            PRO: Can not be conflated with number of keys/columns.
    PROPOSAL: Use "None"/NaN for unknown values.
    '''

    def __init__(self, dc=None):
        if dc is None:
            dc = {}

        assert isinstance(dc, dict)
        for key, na in dc.items():
            assert type(key) is str
            erikpgjohansson.solo.soar.utils.assert_1D_NA(na)

        # Dictionary of numpy arrays.
        self._dc_na = {}

        # Number of elements per (1D) numpy array.
        self._n = None   # Undefined length.

        for key, na in dc.items():
            self._set_item(key, na)

    def __getitem__(self, key):
        return self._dc_na[key]

    def _set_item(self, key, na):
        '''
        Set new "column"/NA.
        NOTE: Can not overwrite previous entry (NA; assertion).
        '''
        # ASSERTIONS
        if key in self._dc_na:
            raise KeyError(f'There already is an entry for key="{key}".')
        assert type(na) is np.ndarray
        assert na.ndim == 1

        # Set/use self._n
        if self._n is None:
            self._n = na.shape[0]
        else:
            assert self._n == na.shape[0], (
                f'Adding array with a size that is incompatible with other'
                f' entries.'
                f' value.shape[0] = {na.shape[0]}, self._n = {self._n}.'
            )

        # Set self._dc_na
        self._dc_na[key] = na

    def index(self, na_bi: np.ndarray):
        '''
        Create new DST using the same keys and subset of arrays, defined by
        one shared set of indices.
        '''
        assert type(na_bi) is np.ndarray

        return DatasetsTable(
            {key: na[na_bi] for key, na in self._dc_na.items()},
        )

    @property
    def n_rows(self):
        '''
        Return the length of the stored arrays. "None" if
        there are no entries/arrays yet.

        Read-only property.
        '''
        '''
        PROPOSAL: Raise exception if there are no entries.
        '''
        return self._n

    def __add__(self, dst2):
        '''Add (concatenate) other DST with this DST.'''
        assert isinstance(dst2, DatasetsTable)
        assert self._dc_na.keys() == dst2._dc_na.keys()

        dc = {}
        for key in self._dc_na.keys():
            dc[key] = np.concatenate((self._dc_na[key], dst2[key]))

        return DatasetsTable(dc)


@codetiming.Timer('derive_DST_from_dir', logger=None)
def derive_DST_from_dir(root_dir):
    '''
    Derive a DST from a directory tree datasets. Searches directory
    recursively.

    NOTE: Ignores filenames that can not be parsed as datasets.


    Parameters
    ----------
    root_dir : String. Path to pre-existing directory.

    Returns
    -------
    dst
    '''
    erikpgjohansson.solo.asserts.is_dir(root_dir)

    ls_file_name            = []
    ls_file_path            = []
    ls_file_version_nbr     = []
    ls_item_id              = []
    ls_file_size            = []
    ls_instrument           = []
    ls_level                = []
    ls_begin_time_file_name = []    # Time derived from filename.
    for (dirPath, _subDirNameList, dirFileNamesList) in os.walk(root_dir):
        for fileName in dirFileNamesList:

            dsfn = \
                erikpgjohansson.solo.metadata.DatasetFilename.parse_filename(
                    fileName,
                )
            # IMPLEMENTATION NOTE:
            # erikpgjohansson.solo.metadata.DatasetFilename.parse_filename()
            # returns None for non-parsable filenames.
            if dsfn:
                _dataSrc, level, instrument, _descriptor = \
                    erikpgjohansson.solo.metadata.parse_DSID(dsfn.dsid)

                filePath = os.path.join(dirPath, fileName)
                ls_file_path        += [filePath]
                ls_file_name        += [fileName]
                ls_file_version_nbr += [int(dsfn.version_str)]
                ls_item_id          += [dsfn.item_id]
                ls_file_size        += [os.stat(filePath).st_size]
                ls_instrument       += [instrument]
                ls_level            += [level]

                # NOTE: datetime.datetime requires integer seconds+microseconds
                # in separate arguments (as integers). Filenames should only
                # contain time with microseconds=0 so we ignore them.
                tv1    = list(dsfn.tv1)
                tv1[5] = int(tv1[5])
                ls_begin_time_file_name += [datetime.datetime(*tv1)]

    dst = DatasetsTable({
        'file_name':        np.array(ls_file_name,        dtype=object),
        'file_path':        np.array(ls_file_path,        dtype=object),
        'item_version':     np.array(ls_file_version_nbr, dtype='int64'),
        'item_id':          np.array(ls_item_id,          dtype=object),
        'file_size':        np.array(ls_file_size,        dtype='int64'),
        'begin_time_FN':    np.array(
            ls_begin_time_file_name, dtype='datetime64[ms]',
        ),
        'instrument':       np.array(ls_instrument,       dtype=object),
        'processing_level': np.array(ls_level,            dtype=object),
    })
    # NOTE: Key name "processing_level" chosen to be in agreement with
    # erikpgjohansson.solo.soar.dwld.SoarDownloader.download_SDT_DST().
    return dst


def log_DST(dst: DatasetsTable, title: str):
    '''
    Log the content of a table of datasets. Assumes that it contains certain
    fields.
    '''
    '''
    PROPOSAL: Log amount of data per combination of level and instrument.
    PROPOSAL: Log amount of data per DSID.
    PROPOSAL: Log number of datasets per DSID.
    '''
    assert isinstance(dst, DatasetsTable)

    SEPARATOR_LENGTH = 80

    def sep():
        L.info('=' * SEPARATOR_LENGTH)

    def ssl(set_ls):
        '''Convert set or list/tuple to string for logging.'''
        # IMPLEMENTATION NOTE: Should not sort since that might not always
        # be desirable (e.g. logging combinations of level and instrument).
        # Caller should sort the argument first if needed.
        if set_ls:
            return ', '.join(set_ls)
        else:
            return '(none)'

    L = logging.getLogger(__name__)

    bytes_tot = dst['file_size'].sum()
    n_datasets = dst['file_size'].size
    gb_tot = bytes_tot / 2**30

    set_instr = set(dst['instrument'])
    set_level = set(dst['processing_level'])
    cnt_level_instr = collections.Counter(
        zip(dst['processing_level'], dst['instrument']),
    )

    # BTF = begin_time_FN
    na_btf = dst['begin_time_FN']
    assert na_btf.dtype == np.dtype('datetime64[ms]')
    na_btf_nonnull = na_btf[~np.isnat(na_btf)]
    n_btf_null = na_btf[np.isnat(na_btf)].size

    sep()
    L.info(title)
    L.info('-'*len(title))
    L.info(f'Number of datasets:       {n_datasets:d} datasets')
    L.info(f'Total amount of data:     {gb_tot:.2f} [GiB]')
    L.info(f'Unique instruments:       {ssl(sorted(set_instr))}')
    L.info(f'Unique processing levels: {ssl(sorted(set_level))}')
    L.info('begin_time_FN:')
    L.info(f'    #NaT: {n_btf_null} datasets')
    if na_btf_nonnull.size > 0:
        # NOTE: na_btf_nonnull.min() crashes if empty array.
        L.info(f'    Min: {na_btf_nonnull.min()}')
        L.info(f'    Max: {na_btf_nonnull.max()}')
    else:
        L.info('    (No datasets without NaT)')
    L.info('Unique combinations of instrument and processing level:')
    for (level, instr) in sorted(cnt_level_instr):
        n = cnt_level_instr[(level, instr)]
        L.info(f'    {level:4} {instr:3}: {n:5} datasets')
    sep()
