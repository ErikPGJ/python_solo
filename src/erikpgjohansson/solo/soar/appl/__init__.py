'''
Import package containing code for explicit, applications of
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

PROBLEM: pytest identifies test_* as test code.
    Ex: test_mirror*.py
    PROPOSAL: Other naming convention:
        mirror_<id>*.py
'''
