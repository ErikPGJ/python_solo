import numpy as np


class DatasetsTable:
    '''Datasets table.

    Encapsulates a dictionary in which each entry is a same-sized 1D numpy
    array, and in which entries can not be overwritten.
    '''
    '''
    PROPOSAL: Initialize by submitting dictionary. Immutable.
    '''

    def __init__(self, dc={}):
        # Dictionary of numpy arrays.
        self._dc_na = {}

        # Number of elements per (1D) numpy array.
        self._n = None

        for key, na in dc.items():
            self[key] = na

    def __getitem__(self, key):
        return self._dc_na[key]

    def __setitem__(self, key, na):
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
                ' entries. '
                f'value.shape[0] = {na.shape[0]}, self._n = {self._n}.'
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
