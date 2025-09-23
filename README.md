# Docker Manager for the Operational National Hydrologic Model

This python package provides and Command Line Interface for managing a set of Docker images used to run the
U.S. Geological Survey Operaional National Hydraulic Model.  A CONUS wide watershed model driven by gridmet climate
forcings on a daily basis.  In addition sub-seasonal to seasonal forecasts can also be run using the downscaled cfsv2
product of 48, 28-day ensembles, delivered daily, and seaonal forecasts using downscaled NMME product of 6-month forecasts,
delivered once per month.

<span style="color: red; font-weight: bold;">Attention:</span> This project is in the early stages of development,
particularly the forecasting elements.

## Getting started

To create a conda env:

```shell
mamba env create -f environment.yml
mamba activate pyonhm
poetry install
```

## Docker Volume Strategy

This project uses a Docker bind mount to share data between the host machine and the Docker services. This strategy allows for direct access to input and output files from your local filesystem.

### Configuration
You must set the `NHM_BIND_PATH` environment variable on your host system. This variable should point to an absolute path on your machine where you want to store the model data.

**Example:**
```shell
export NHM_BIND_PATH=/path/to/my/nhm_data
```

This local directory will be mounted to `/nhm` inside each Docker container. All services read from and write to this shared directory, which makes it easy to inspect results and manage data from your host machine.

## Command Line Interface

```text
Usage: pyonhm [OPTIONS] COMMAND [ARGS]...

╭─ Admin Commands ───────────────────────────────────────────────────────────╮
│ Build images and load supporting data into volume                          │
│                                                                            │
│ build-images          Builds all Docker images using the                     │
│                       DockerComposeManager.                                  │
│ load-data             Loads data using the DockerComposeManager.             │
╰────────────────────────────────────────────────────────────────────────────╯
╭─ Operational Commands ─────────────────────────────────────────────────────╮
│ NHM daily operational model methods                                        │
│                                                                            │
│ fetch-op-results      Fetches operational results using the DockerManager.   │
│ run-operational       Runs the operational simulation using the              │
│                       DockerComposeManager.                                  │
╰────────────────────────────────────────────────────────────────────────────╯
╭─ Sub-seasonal Forecast Commands ───────────────────────────────────────────╮
│ NHM sub-seasonal forecasts model methods                                   │
│                                                                            │
│ conv-output-to-zarr   Runs the sub-seasonal operational simulation using     │
│                       the DockerManager.                                     │
│ run-sub-seasonal      Runs the sub-seasonal operational simulation using     │
│                       the DockerManager.                                     │
│ run-update-cfsv2-data Runs the update of CFSv2 data using the specified      │
│                       method , either 'ensemble' or 'median'.                │
╰────────────────────────────────────────────────────────────────────────────╯
╭─ Seasonal Forecast Commands ───────────────────────────────────────────────╮
│ NHM seasonal forecasts model methods                                       │
│                                                                            │
│ run-seasonal          Runs the seasonal operational simulation using the     │
│                       DockerManager.                                         │
╰────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────────────────────╮
│ --help,-h             Display this message and exit.                       │
│ --version             Display application version.                         │
╰────────────────────────────────────────────────────────────────────────────╯
```

## GitLab CI/CD Orchestration (In-Progress)

An incomplete GitLab CI/CD pipeline is defined in `.gitlab-ci.yml` to automate the execution of the `pyonhm` model. The pipeline is designed to run on a custom GitLab Runner with `shell`, `docker`, and `conda` tags.

The pipeline consists of two main stages:

1.  **Setup**: This stage includes jobs for:
    *   `setup_miniforge`: Installs Miniforge (a minimal conda installer).
    *   `build_conda_env`: Creates the `pyonhm` conda environment from `environment.yml` and installs dependencies with `poetry`.
    *   `clone_repo_to_data`: Clones the project repository into a `/data` directory on the runner.
    *   `load_data`: Runs `pyonhm load-data` to download the necessary model data.
    *   `update_hotstart`: Runs `pyonhm run-operational` to get the latest hotstart file.

2.  **Run-Operational**: This stage is intended to be run on a schedule and contains the core logic for the model execution:
    *   `update_cfsv2_data`: Updates the CFSv2 forecast data for both `median` and `ensemble` methods.
    *   Runs the operational model with `pyonhm run-operational`.
    *   Runs the sub-seasonal forecasts for both `median` and `ensemble` methods.

This pipeline is still under development but provides a logical sequence for how the `pyonhm` CLI commands are intended to be used in an automated workflow.

## Notebooks for Data Pre-processing

The notebooks in the `notebooks/` directory are used to pre-process the raw model data (a "bandit pull") into the structured format required by `pyonhm`. This structured data is what is included in the `NHM_PRMS_CONUS_GF_1_1.zip` and `NHM_PRMS_UC_GF_1_1.zip` files.

The key notebook for the full CONUS dataset is `prep_conus_case.ipynb`. For development and testing, the `prep_uc_case.ipynb` notebook is used to create a smaller, more manageable test case for the Upper Colorado (UC) basin. This allows for rapid testing of the `pyonhm` commands and the Docker orchestration.

The general workflow demonstrated in these notebooks is:

1.  **Load and Clean Geometries**: Reads the NHM shapefiles (`model_nhru.shp`, `model_nsegment.shp`) using `geopandas`, corrects any invalid geometries, and re-projects them to `EPSG:4326`.
2.  **Extract Parameters**: Reads essential parameters like `nhm_id`, `hru_lat`, `hru_lon`, and `hru_elev` directly from the `myparam.param` file. These are saved as CSV files.
3.  **Extract Segment Data**: Calculates and saves the latitude and longitude of stream segment centroids (`seg_lat.csv`, `seg_lon.csv`).
4.  **Generate Gridmet Weights**: Uses `gdptools` to generate the spatial weights file (`uc_weights.csv` or similar) required to map GridMET climate data to the NHM HRUs.

These notebooks are not part of the operational `pyonhm` workflow but are a critical part of the initial data setup.

## License

This project is licensed under the CC0 1.0 Universal public domain dedication.
[View the license here](./LICENSE.md)

## Diclaimer

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.
