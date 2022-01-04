
This text is intended for people who want to use a SOAR data mirroring code
(SOAR = ESA's Solar Orbiter Archive), designed by Erik P G Johansson for IRF
Uppsala, Sweden


NOTES
=====
NOTE: The code was not originally written with the intent of being shared, or
as a standalone "package", but rather only for local needs and as a learning
experience in Python. Nonetheless, it is reasonably well-structured. It however
also makes use of more of my own generic code than I remembered when offering
it, in particular code for parsing dataset filenames (and string manipulation),
standardized assertions, and (custom) automatic test code (that can be
ignored). Nonetheless, the code might be useful, in part or in full. I am not a
long-time Python programmer (yet) so the code does not (yet) fully conform to
Python conventions & best practices. No guarantees.

NOTE: Code contains references to my own code for automatic tests which can be
ignored (should be replaced by e.g. pytest some day). Automatic test code is
named *___ATEST.

NOTE: "BOGIQ" just refers to my own thoughts on future changes. Can be ignored.

NOTE: SOAR downloads can be quite slow, empirically ~0.7 MiB/s! Initial batch
downloads can take SEVERAL DAYS. Logging (stdout) for the progress of downloads
is therefore detailed.

CAVEAT: Can only handle CDF datasets, and only ones that follow (official)
filenaming conventions that are already supported by
erikpgjohansson.so.parse_dataset_filename().



THEORETICAL PROCEDURE
=====================
In theory, to set up one own mirror, one only needs to
(1) Modify FILE_REMOVAL_COMMAND_LIST (hardcoded variable) configure which
    command to use for removing local files.
(2) Modify e.g. irfu_soar_mirror.py to configure one's own mirror.
(3) Call erikpgjohansson.so.irfu_soar_mirror.sync() to mirror data (triggers
    one sync against SOAR).



~CODE DESCRIPTION, IMPLEMENTATION
=================================
NOTE: Code uses SOAR's own interface for access:
http://soar.esac.esa.int/soar/#aio
--
Mirrors (1) a specified subset of (only) the latest versions of SOAR's
datasets, with (2) local datasets on a standardized directory structure. Code
downloads a list of datasets at SOAR and compares it with a compiled list of
local datasets.
--
NOTE: Code is designed to DELETE local datasets when necessary, but contains a
safety variable that specifies the net max number of datasets to delete at
once. One must also configure hardcoded variable FILE_REMOVAL_COMMAND_LIST to
make it work.
--
Known ~requirements:
    numpy (at least v1.18.5 or v1.19.5)
        NOTE: Uses numpy's datetime64 which is experimental and might change in
        the future.
    Works for Python 3.6, 3.9, but probably other versions too.
    Works on Linux, but should in principle work on other platforms too with
    minimal changes.
--
soar.py
    Download table of datasets in SOAR.
    Download latest version of one dataset (CDF).
soar_utils.py
    Do batch downloads (in general).
soar_mirror.py
    Code for mirroring.
irfu_soar_mirror.py, test_soar_mirror.py
    Contain simple customized function calls to perform one "sync".
    Effectively contains the configuration of the mirror used at IRFU.
    Can be used as an example when creating your own mirror.
    IMPORTANT NOTE: In theory, you should only need to modify this file
    to create your own mirror.
iddt.py
    Code which manages a standardized directory subtree in which to put the
    datasets. This directory structure imitates ROC's organization of datasets
    and is required by irfu-matlab's (not included) automatic reading of
    datasets (MATLAB).
