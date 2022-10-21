# Shortenings

Miscellaneous abbreviations, shortenings used for documentation and
variable naming conventions.

data_item_id : SOAR uses this term in SOAR's datasets tables. Appears to be
same as "item_id".

DATASET_ID : Of a set unique string identifier constants that each specify a
type of SolO dataset.
Probably defined by LESIA for SolO/RPW datasets. Identical to the
beginning of filenames following the official mission-wide filenaming
convention. Always(?) uppercase.

- Ex: `solo_L2_rpw-lfr-surv-cwf-e_20221006_V01.cdf` is a dataset (file) with
  DATASET_ID = `SOLO_L2_RPW-LFR-SURV-CWF-E`.

DC : Dictionary (Python type).

DOM : Day-Of-Month

DST : Instance of class `erikpgjohansson.solo.soar.dst.DatasetsTable`.

DT = Instance of class `datetime.datetime`.

DTDN : Data Type Directory Name. Standardized (sub)directory name used
for subset of DATASET_IDs in the "IRFU SolO data directory structure". Ex:
lfr_wf_e.

IDTD : IRFU (SolO) Datasets Directory Tree. "SolO" is excluded from the
name since it is implicit from parent package "solo". The way datasets are
organized for SolO L2 & L3 datasets at IRFU. Directory paths:
`<instrument>/<DTDN>/<year>/<month>/<dataset file>`.

Item ID : SOAR uses this term in SOAR's datasets tables. Subset of SolO
dataset file name in SolO's filenaming convention.
Type of return value from SOAR when requesting table of datasets.
Essentially DATASET_ID + time interval in official filename.

- Ex: `solo_L2_rpw-lfr-surv-cwf-e_20221006_V01.cdf` is a dataset (file) with
  item ID = `solo_L2_rpw-lfr-surv-cwf-e_20221006`.
- Ex:
  - `solo_L2_rpw-tds-surv-tswf-b_20200707`
  - `solo_L2_epd-ept-south-rates_20200720`
  - `solo_LL02_epd-het-sun-rates_20201012T000034-20201013T000034`

NA : Numpy Array.

SOAR : Solar Orbiter ARchive. `https://soar.esac.esa.int/soar/`

SDT : SOAR Datasets Table. Table over datasets (metadata) at SOAR, either all
or some subset. Not to be confused with DST, which can be a way of storing e.g.
an SDT.

TD = Instance of class `datetime.timedelta`.
