# Purpose and scope

This import package (directory) is intended for code related to mirroring SOAR data.

# "Description" of code and implementation

The code uses SOAR's own interface for access:

`http://soar.esac.esa.int/soar/#aio`

The code mirrors (1) a specified subset of (only) the latest versions of SOAR's
datasets, to (2) local datasets on a standardized directory structure. The code
downloads a list of datasets at SOAR and compares it with a compiled list of
local datasets.

The code is designed to DELETE local datasets when necessary, but contains a
safety variable that specifies the net max number of datasets to delete at
once.

# Known requirements

- Works for Python 3.6, 3.9, but probably other versions too.
- Works on Linux, but should in principle work on other platforms too with minimal changes.

# Procedure for setting up a mirror

In theory, to set up one's own mirror, one only needs to

1. Install this distribution package (`pip install`)
2. Modify `FILE_REMOVAL_COMMAND_LIST` (hardcoded variable) which configures which command to use for removing local files.
3. Create a modified and customized version of e.g. `irfu_mirror.py` to configure your own mirror.
4. Periodaically call the `sync()` function in the modified module to mirror data (triggers one synchronization against SOAR).

# Notes

SOAR downloads can be quite slow, about ~0.7 MiB/s! Initial batch downloads can take _several days_. Logging (stdout) for the progress of downloads is therefore detailed.

Caveat: As it stands, the SOAR mirror code can only handle CDF datasets, and only ones that follow those official filenaming conventions that are already supported by `erikpgjohansson.solo.parse_dataset_filename()`.

## Important source code files

### soar.py

Download table of datasets in SOAR. Download latest version of one dataset (CDF).

### soar_utils.py

Do batch downloads (in general).

### soar_mirror.py

Code for mirroring.

### irfu_mirror.py, test_mirror.py

Contain simple customized function calls to perform one "sync" call.
Can be used as an example when creating your own mirror.

IMPORTANT NOTE: In theory, you should only need to modify this file
to create your own mirror.

### iddt.py

Code which manages a standardized directory subtree in which to put the
datasets. This directory structure imitates ROC's organization of datasets
and is required by irfu-matlab's (not included MATLAB code) automatic reading of
datasets.
