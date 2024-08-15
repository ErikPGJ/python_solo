'''
Import package containing code for explicit applications of
the SOAR mirror code, e.g. domain-specific non-generic mirrors.
'''
'''
PROPOSAL: Should (ideally) be separate from erikpgjohansson.solo.soar.
    PRO: Consists of domain-specific applications of generic code.
    TODO-DEC: Where? Different distribution package?
              Outside of distribution packages?

PROPOSAL: Rename package/directory to imply non-generic code.
    Ex: Download specific datasets.
    PROPOSAL: ~usage
    PROPOSAL: ~applications/appl -- IMPLEMENTED
        PRO: More general.
        PRO: Implies more clearly that code is application-specific.

PROPOSAL: Move code outside of package.
    PRO: Is non-generic code for concrete configurations.
    PROPOSAL: Refactor into using configuration file instead of custom python
              scripts.
        PROBLEM: The "datasets_include_func" can not be turned into a
                 config file if one wants it to be completely generic.
        PROBLEM: Can not specify timestamped removal directories.
    PROPOSAL: Use single-file python scripts for each configuration.
        Can merge e.g. irfu_mirror.py and irfu_mirror_shell.py into one and
        manage the combined file manually outside of git repo.
        NOTE: Cf. JUICE/RPWI GS's IATP.

PROBLEM: pytest identifies test_* as test code.
    Ex: test_mirror*.py
    PROPOSAL: Other naming convention:
        mirror_<id>*.py
'''
