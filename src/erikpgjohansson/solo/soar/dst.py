'''
Module for the DST class.
'''


import numpy as np


'''

PROBLEM: Can not use erikpgjohansson.solo.soar.utils.assert_1D_NA() due to
         circular imports.
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
            # Can not due to circular imports.
            # erikpgjohansson.solo.soar.utils.assert_1D_NA(na)

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
