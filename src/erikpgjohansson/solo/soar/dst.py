import numpy as np


class DatasetsTable:
    '''Immutable "datasets table". Stores table of datasets.

    Encapsulates a dictionary in which each key is a string and each value
    is a same-sized 1D numpy array.
    '''
    '''
    PROPOSAL: Change name of class.
        PRO: Is more generic than for datasets.
    PROPOSAL: Change name "index()".
        get_subset()
    PROPOSAL: Change name "n()".
        size()
    PROPOSAL: Use "None"/NaN for unknown values.
    '''

    def __init__(self, dc={}):
        assert isinstance(dc, dict)
        for key, na in dc.items():
            assert type(key) == str
            # Can not due to circular imports.
            # erikpgjohansson.solo.soar.utils.assert_col_array(na)

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
        Set entry.
        NOTE: Can not overwrite previous entry.
        '''
        if key in self._dc_na:
            raise KeyError(f'There already is an entry for key="{key}".')
        assert type(na) == np.ndarray
        assert na.ndim == 1

        if self._n is None:
            self._n = na.shape[0]
        else:
            assert self._n == na.shape[0], (
                f'Adding array with a size that is incompatible with other'
                f' entries.'
                f' value.shape[0] = {na.shape[0]}, self._n = {self._n}.'
            )

        self._dc_na[key] = na

    def index(self, bi: np.ndarray):
        '''
        Create new DST using the same keys and subset of arrays, defined by
        one shared set of indices.
        '''
        assert type(bi) == np.ndarray

        return DatasetsTable({key: na[bi] for key, na in self._dc_na.items()})

    def n(self):
        '''
        Return the length of the stored arrays. "None" if
        there are no entries/arrays yet.'''
        '''
        PROPOSAL: Raise exception if there are no entries.
        '''
        return self._n

    def __add__(self, dst2):
        assert isinstance(dst2, DatasetsTable)
        assert self._dc_na.keys() == dst2._dc_na.keys()
        dc = {}
        for key in self._dc_na.keys():
            dc[key] = np.concatenate((self._dc_na[key], dst2[key]))

        return DatasetsTable(dc)
