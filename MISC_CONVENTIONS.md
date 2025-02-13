# Miscellaneous conventions

# Logging

Logging objects are obtained using `logging.getLogger(__name__)`.

# Naming convention

Naming should use abbreviations when applicable. Abbreviations in
function/method names are uppercase.

Classes are named using CamelCase (uppercase initial).

Constants use SCREAMING_SNAKE_CASE.

Naming convention for variables is inconsistent but follows either:
(1) camelCase for variables: uppercase initial for class variables(?);
otherwise lowercase initial, or
(2) snake_case

The long term goal is to only use convention (2) (?).

Use of "ls"/"dc" implies that what comes to the left (if anything) refers
to the entire ls/dc, and what come to the right refers to the content of
each component in the ls/dc.
Ex: dc_ls_x = dictionary of lists/tuples of "x".

# Shortenings

## Introduction

Miscellaneous abbreviations, shortenings used for documentation and
variable naming conventions.

## List of defined abbreviations and terms

data_item_id : SOAR uses this term in SOAR's datasets tables. Appears to be
same as "item_id".

Note: Can not find this term in SOL-SGS-TN-0009, "Metadata Definition for Solar
Orbiter Science Data", 2/6.

DC : Python Dictionary.

DOM : Day-Of-Month

DSFN : Class `erikpgjohansson.solo.DatasetFilename`.

DSID : "Dataset ID". A string constant which uniquely represents a type of
SolO dataset. Probably defined by LESIA for SolO/RPW datasets. Identical to
the beginning of filenames following the official mission-wide filenaming
convention (except for case).

In this code, DSID is defined to always:

- Be uppercase.
- Exclude "-CDAG".

Ex: `solo_L2_rpw-lfr-surv-cwf-e_20221006_V01.cdf` is a dataset (file) with
DSID = `SOLO_L2_RPW-LFR-SURV-CWF-E`.

DST : Instance of class `erikpgjohansson.solo.soar.dst.DatasetsTable`.

DT : Instance of class `datetime.datetime`.

DT64 : numpy array data type "`numpy.datetim64`".

DTDN : Dataset Type Directory Name. Standardized (sub)directory name used
for SolO L2 & L3 DSIDs in IDDT and RDDT. String constant which is a function of
the DSID.

- Ex: "lfr_wf_e".

- DTDNs are only defined for L2 and L3 DSIDs.
- There can be multiple DSIDs for the same DTDN.
- ROC defines DTDNs for _L2 & L3 RPW_ datasets via RDDT.
- DTDNs for (L2 & L3) non-RPW datasets are defined by
  `erikpgjohansson.solo.iddt.convert_DSID_to_DTDN()` and are thus
  more arbitrary.

IDDT : IRFU (SolO) Datasets Directory Tree. "SolO" is excluded from the
abbreviation since it is implicit from parent package "solo". Refers to a
standardized directory structure in which SolO datasets are organized at IRFU
and which is overlaps with RDDT. It is therefore important to compare the two.
See RDDT.

Note: IDDT must support SOAR datasets, and thus paths for LL and L1 datasets.

- IDDT:
  - Is not defined for HK.
  - L2 & L3
    - `<instrument>/<level>/<DTDN>/<year>/<month>/<dataset file>`
  - LL02, LL03, L1, L1R:
    - `<instrument>/<level>/<year>/<month>/<day>/<dataset file>`
    - Should possibly ideally use the same exception for L1/L1R SBM1/SBM2 as in
      ROC's directory structure? Otherwise identical to RDDT for L1, L1R.
  - Covers all SolO instruments.
- RDDT:
  - Only covers RPW datasets.
  - L2 & L3: Same as IDDT.
  - Non-L2/L3:
    - `{L1,L1R,L1_SBM,L1R_SBM}/<year>/<month>/<day>/<dataset file>`
      - Note: L1/L1R excludes (RPW) SBM1/SBM2.
      - Note: L1 sweeps are a special case.
      - Note: Subdirectories for days.
      - Note: No DTDN.
  - Defined also for other RPW datasets (HK), but that is not relevant here.

IRFU, IRF-U : Institutet för Rymdfysik, Uppsala department (Swedish Institute
of Space Physics).

Item ID, item_id : SOAR uses this term in SOAR's datasets tables. Subset of SolO
dataset file name in SolO's filenaming convention.
Type of return value from SOAR when requesting table of datasets.
Essentially DSID + time interval in official filename.

- Ex: `solo_L2_rpw-lfr-surv-cwf-e_20221006_V01.cdf` is a dataset (file) with
  item ID = `solo_L2_rpw-lfr-surv-cwf-e_20221006`.
- Ex:
  - `solo_L2_rpw-tds-surv-tswf-b_20200707`
  - `solo_L2_epd-ept-south-rates_20200720`
  - `solo_LL02_epd-het-sun-rates_20201012T000034-20201013T000034`

Note: Can not find this term in SOL-SGS-TN-0009, "Metadata Definition for Solar
Orbiter Science Data", 2/6.

L : `logging.Logger` object.

LS : Python list/tuple.

NA : Numpy Array.

NDT : numpy data type. A type used for setting numpy's `dtype` arguments.

RDDT : ROC's Dataset Directory Tree. Refers to a standardized directory
structure in which SolO datasets are organized by ROC. This is the same
directory structure that is used for datasets which are shared inside the RPW
consortium. See IDDT.

SDT : SOAR Datasets Table. Table over datasets (metadata) at SOAR, either all
or some subset thereof. May be stored as e.g. JSON data structure, or a DST.
Not to be confused with DST.

SOAR : Solar Orbiter ARchive. `https://soar.esac.esa.int/soar/`

SODL : Class `erikpgjohansson.solo.soar.dwld.SoarDownloader` and subclasses
thereof.

TD = Instance of class `datetime.timedelta`.

TV, Time Vector : Tuple of (year, month, day, hour, minute, second).
