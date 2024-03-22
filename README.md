# Docker Manager for the Operational National Hydrologic Model
This python package provides and Command Line Interface for managing a set of Docker images used to run the
U.S. Geological Survey Operaional National Hydraulic Model.  A CONUS wide watershed model driven by gridmet climate
forcings on a daily basis.  In addition sub-seasonal to seasonal forecasts can also be run using the downscaled cfsv2
product of 48 28-day ensembles delivered daily, and seaonal forecasts using downscaled NMME product of 6-month forecasts
delivered once per month.

## Getting started
To create a conda env:
```shell
mamba env create -f environment.yml
mamba activate pyonhm
poetry install
```

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