'''
Python script (not functions) intended to be called from the system shell.

Primarily intended to be called from a minimal wrapper bash script that also
specifies the correct Python environment.
'''


__author__ = "Erik P G Johansson, IRF"


import sys
import erikpgjohansson.solo.soar.mirrors.test_mirror


'''
'''


def main(ls_args):

    assert len(ls_args) == 0, 'Illegal number of arguments.'
    # NOTE: Python function check whether it is running on a legal machine.
    # Does not need to do that check here (or in bash).
    erikpgjohansson.solo.soar.mirrors.test_mirror.sync()


main(sys.argv[1:])  # NOTE: sys.argv[0] is not a CLI argument. ==> Ignore
