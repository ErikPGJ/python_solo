'''
Interfaces to function intended to be called from bash script.
'''


import erikpgjohansson.solo.iddt
import logging
import sys


def main(ls_cli_args):
    HELP_TEXT = """

    Move SolO datasets (using filenaming conventions) to an "IRFU-standard"
    directory tree.

    Wrapper around python code
    erikpgjohansson.solo.iddt.copy_move_datasets_to_IRFU_dir_tree().
    NOTE: Can not override instrDirCase and dtdnInclInstrument.


    Parameters: (move | copy) <source dir> <destination dir.>


    Script initially created 2020-10-16 by Erik P G Johansson.
    """

    #################
    # User help text
    #################
    if len(ls_cli_args) == 1 and ls_cli_args[0] == '--help':
        print(HELP_TEXT)
        # Return instead of quit(0) so that function can in principle be called
        # from other python code without quitting.
        return 0

    # ASSERTION
    if not len(ls_cli_args) == 3:
        raise Exception('Illegal number of arguments.')

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    erikpgjohansson.solo.iddt.copy_move_datasets_to_IRFU_dir_tree(
        mode=ls_cli_args[0],
        sourceDir=ls_cli_args[1],
        destDir=ls_cli_args[2],
    )


if __name__ == '__main__':
    main(sys.argv[1:])  # NOTE: sys.argv[0] is not a CLI argument. ==> Ignore
