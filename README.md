# Docker Manager for the Operational National Hydrologic Model

This python package provides and Command Line Interface for managing a set of Docker images used to run the
U.S. Geological Survey Operaional National Hydraulic Model.  A CONUS wide watershed model driven by gridmet climate
forcings on a daily basis.  In addition sub-seasonal to seasonal forecasts can also be run using the downscaled cfsv2
product of 48, 28-day ensembles, delivered daily, and seaonal forecasts using downscaled NMME product of 6-month forecasts,
delivered once per month.

<span style="color: red; font-weight: bold;">Attention:</span> This project is in the early stages of development.

## Getting started

To create a conda env:

```shell
mamba env create -f environment.yml
mamba activate pyonhm
poetry install
```

## Command Line Interface

```shell
Usage: pyonhm COMMAND

╭─ Admin Commands ───────────────────────────────────────────╮
│ Build images and load supporting data into volume          │
│                                                            │
│ build-images  Builds Docker images using the               │
│               DockerManager.                               │
│ load-data     Loads data using the DockerManager.          │
╰────────────────────────────────────────────────────────────╯
╭─ Operational Commands ─────────────────────────────────────╮
│ NHM daily operational model methods                        │
│                                                            │
│ fetch-op-results  Fetches operational results using the    │
│                   DockerManager.                           │
│ run-operational   Runs the operational simulation using    │
│                   the DockerManager.                       │
╰────────────────────────────────────────────────────────────╯
╭─ Sub-seasonal Forecast Commands ───────────────────────────╮
│ NHM sub-seasonal forecasts model methods                   │
│                                                            │
│ run-sub-seasonal  Runs the sub-seasonal operational        │
│                   simulation using the DockerManager.      │
╰────────────────────────────────────────────────────────────╯
╭─ Seasonal Forecast Commands ───────────────────────────────╮
│ NHM seasonal forecasts model methods                       │
│                                                            │
│ run-seasonal  Runs the seasonal operational simulation     │
│               using the DockerManager.                     │
╰────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────╮
│ --help,-h  Display this message and exit.                  │
│ --version  Display application version.                    │
╰────────────────────────────────────────────────────────────╯
```

## License

This project is licensed under the CC0 1.0 Universal public domain dedication.
[View the license here](./LICENSE.md)

## Diclaimer

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.
