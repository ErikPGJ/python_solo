'''
Python script (not functions) intended to be called from the system shell.

Primarily intended to be called from a minimal wrapper bash script that also
specifies the correct Python environment.
'''


__author__ = "Erik P G Johansson, IRF"


import sys
import erikpgjohansson.solo.soar.mirrors.irfu_mirror


'''
'''


HELP_TEXT = """

Trigger the syncing of the "official" IRFU-internal SOAR mirror.
Bash wrapper around python code erikpgjohansson.solo.irfu_soar_mirror.sync().

NOTE: Only meant to be run on brain.irfu.se, spis.irfu.se.
NOTE: Downloading from SOAR is slow and can easily take hours, or days,
      if adding significant amounts of data.

NOTE/BUG: No automatic logging of this function.
NOTE: so_irfu_soar_sync_cron is a bash wrapper with logging around this script.


Parameters: (none)


Script initially created 2021-01-19 by Erik P G Johansson.
        """


def main(ls_args):
    #################
    # User help text
    #################
    if len(ls_args) == 1 and ls_args[0] == '--help':
        print(HELP_TEXT)

        # Return instead of quit(0) so that function can in principle be called
        # from other python code without quitting.
        return 0

    # NOTE: Python function check whether it is running on a legal machine.
    # Does not need to do that check here (or in bash).
    erikpgjohansson.solo.soar.mirrors.irfu_mirror.sync()


main(sys.argv[1:])  # NOTE: sys.argv[0] is not a CLI argument. ==> Ignore
