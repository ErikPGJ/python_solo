# Purpose and scope

This text is intended for people who want to use a SOAR data mirroring code
(SOAR = ESA's Solar Orbiter Archive), designed by Erik P G Johansson for IRF
Uppsala, Sweden



# Notes

SOAR downloads can be quite slow, empirically ~0.7 MiB/s! Initial batch downloads can take SEVERAL DAYS. Logging (stdout) for the progress of downloads is therefore detailed.

CAVEAT: SOAR mirror code can only handle CDF datasets, and only ones that follow those official filenaming conventions that are already supported by `erikpgjohansson.so.parse_dataset_filename()`.



# Procedure for setting up mirror, in theory

In theory, to set up one own mirror, one only needs to

1. Modify `FILE_REMOVAL_COMMAND_LIST` (hardcoded variable) which configures which command to use for removing local files.
2. Modify e.g. `irfu_soar_mirror.py` to configure your own mirror.
3. Call the modified version of `erikpgjohansson.so.irfu_soar_mirror.sync()` to mirror data (triggers one sync against SOAR).



# "Description" of code and implementation

The code uses SOAR's own interface for access:

`http://soar.esac.esa.int/soar/#aio`

Mirrors (1) a specified subset of (only) the latest versions of SOAR's
datasets, with (2) local datasets on a standardized directory structure. The code
downloads a list of datasets at SOAR and compares it with a compiled list of
local datasets.

The code is designed to DELETE local datasets when necessary, but contains a
safety variable that specifies the net max number of datasets to delete at
once. One must also configure hardcoded variable `FILE_REMOVAL_COMMAND_LIST` to
make it work.

Known requirements:

 * numpy (at least v1.18.5 or v1.19.5).
   NOTE: Uses numpy's datetime64 which is experimental and might change in
   the future.
 * Works for Python 3.6, 3.9, but probably other versions too.
 * Works on Linux, but should in principle work on other platforms too with minimal changes.



## Important source code files

### soar.py

Download table of datasets in SOAR. Download latest version of one dataset (CDF).

### soar_utils.py

Do batch downloads (in general).

### soar_mirror.py

Code for mirroring.

### irfu_soar_mirror.py, test_soar_mirror.py
Contain simple customized function calls to perform one "sync".
Effectively contains the configuration of the mirror used at IRFU.
Can be used as an example when creating your own mirror.

IMPORTANT NOTE: In theory, you should only need to modify this file
to create your own mirror.

### iddt.py

Code which manages a standardized directory subtree in which to put the
datasets. This directory structure imitates ROC's organization of datasets
and is required by irfu-matlab's (not included) automatic reading of
datasets (MATLAB).
