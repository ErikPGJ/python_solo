# Solar Orbiter package

This directory constitutes a git repo and Python distribution package for
miscellaneous Solar Orbiter (SolO) code written and used by
Erik P G Johansson, IRF Uppsala (Swedish Institute of Space Physics;
`https://www.irf.se`), Sweden

In particular, it contains code for mirroring CDF datasets from SOAR (Solar
Orbiter Archive; `http://soar.esac.esa.int/soar/`). See
`src/erikpgjohansson/solo/soar/README_SOAR_MIRROR.md`.

## Important notes

This code is primarily made public for the purpose of sharing code for
mirroring SOAR data. The code was originally _not_ written with the intent of
being shared, nor was it intended as a standalone "package". It was originally
written for local needs and as a beginner's learning experience in Python.
Therefore, _the code is not fully generic, does not yet fully conform to many
Python conventions & best practices, and does not fully take advantage of
Python libraries as the author was not very experienced in Python at the time
of writing the original code_. For the same reason it is not fully documented,
and is not very well tested as a distribution package. Still, it is reasonably
well-structured and might be useful, in part or in full. It has been partially
cleaned up afterwards.

Source code comment sections labelled "BOGIQ" refers to the author's thoughts
on future changes. These can be ignored.
