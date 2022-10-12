# Shorternings

DATASET_ID : Of a set unique string identifier constants that each specify a
type of SolO dataset.
Probably defined by LESIA for SolO/RPW datasets. Identical to the
beginning of filenames following the official mission-wide filenaming
convention. Always(?) uppercase.
Ex: `solo_L2_rpw-lfr-surv-cwf-e_20221006_V01.cdf` is a dataset (file) with
DATASET_ID = `SOLO_L2_RPW-LFR-SURV-CWF-E`.

DOM : Day-Of-Month

DST : Instance of class `erikpgjohansson.solo.soar.dst.DatasetsTable`.

DTDN : Data Type Directory Name. Standardized (sub)directory name used
for subset of DATASET_IDs in the "IRFU SolO data directory structure". Ex:
lfr_wf_e.

IDTD : IRFU (SolO) Datasets Directory Tree. "SolO" is excluded from the
name since it is implicit from parent package "solo". The way datasets are
organized for SolO L2 & L3 datasets at IRFU. Directory paths:
`<instrument>/<DTDN>/<year>/<month>/<dataset file>`.

Item ID : Subset of SolO dataset file name in SolO's filenaming convention.
Used by SOAR. Essentially DATASET_ID + time interval.
Ex: `solo_L2_rpw-lfr-surv-cwf-e_20221006_V01.cdf` is a dataset (file) with
item ID = `solo_L2_rpw-lfr-surv-cwf-e_20221006`.

NA : Numpy Array.

SOAR : Solar Orbiter ARchive. `https://soar.esac.esa.int/soar/`
