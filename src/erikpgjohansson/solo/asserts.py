# -*- coding: UTF-8 -*-
'''
NOTE: Copy of subset of other module outside distribution package.

Initially created by Erik P G Johansson.
'''


import os



# Follows symlinks.
def is_dir(path):
    assert os.path.isdir(path), \
        f'"{path}" is not a path to a pre-existing directory.'.format(path)



def path_is_available(path):
    # os.path.lexists(path)
    # """"Return True if path refers to an existing path. Returns True for
    # broken symbolic links. Equivalent to exists() on platforms lacking os.lstat().""""
    assert not os.path.lexists(path), \
        'Path "{0}" unexpectedly refers to an existing object.'.format(path)



# NOTE: Includes dictionaries (iterations are over keys).
# NOTE: Strings are technically python sequences.
def is_nonstring_sequence(s):
    assert hasattr(type(s), '__iter__') & (type(s)!=str), \
        'Argument is not a non-string "sequence".'
