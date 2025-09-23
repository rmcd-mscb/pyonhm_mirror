# GitHub Copilot Instructions for pyONHM

This is the **Operational National Hydrologic Model (ONHM)** - a containerized Python CLI for running USGS watershed simulations across CONUS using Docker-orchestrated microservices.

## Architecture Overview

### Core Design Pattern: Docker Microservice Pipeline
- **Base Pattern**: Each processing stage = separate Docker service with shared `/nhm` volume mount
- **Entry Point**: `pyonhm` CLI (`pyproject.toml` script) → `docker_compose_manager.py` → Docker Compose orchestration
- **Data Flow**: Climate data → Model preprocessing → PRMS simulation → NetCDF output → Zarr conversion

### Key Services (see `docker-compose.yml`)
```
gridmetetl  → Climate data extraction (GridMET)
cfsv2etl    → Forecast data (CFSv2 ensembles) 
ncf2cbh     → NetCDF to PRMS climate format conversion
prms        → Core watershed model (Fortran binary)
out2ncf     → PRMS output to NetCDF conversion
ncf2zarr    → NetCDF to Zarr format for analysis
```

### Critical Environment Setup
- **Volume Mount**: `${NHM_BIND_PATH}` → `/nhm` (ALL services require this)
- **Environment Files**: `nhm_conus.env`, `nhm_uc.env` define project roots and data paths
- **Base Image**: `pyonhm/base/Dockerfile` provides conda environment + scientific libraries

## Development Workflows

### Environment Setup
```bash
mamba env create -f environment.yml  # Creates minimal conda env with poetry
mamba activate pyonhm
poetry install  # Installs full dependencies + CLI
```

### CLI Command Structure
- **Admin**: `pyonhm build-images`, `pyonhm load-data`
- **Operational**: `pyonhm run-operational` (daily GridMET runs)
- **Forecasting**: `pyonhm run-sub-seasonal`, `pyonhm run-seasonal`

### Cron Integration
- Production runs via `cron/run_operational.sh` 
- Conda environment activation required for cron jobs
- Logs to timestamped files in `$HOME/pyonhm_logs/`

## Project-Specific Patterns

### Docker Service Dependencies
- **Build Order**: `base` → all others (inheritance hierarchy)
- **Volume Strategy**: Bind mount (not Docker volumes) for direct file access. The `NHM_BIND_PATH` environment variable must be set on the host to map a local directory to the `/nhm` directory inside each container. This allows for easy inspection and manipulation of data from the host.
- **Service Communication**: File-based via shared `/nhm` directory

### Data Processing Conventions
- **NetCDF → Zarr**: Use `ncf2zarr.py` with ensemble dimension expansion
- **PRMS Integration**: Input via `.cbh` files, output as binary → converted to NetCDF
- **Environment Variables**: Service behavior controlled via `.env` files (see `nhm_*.env`)

### Logging & Rich Output
- **Standard**: `utils.setup_logging()` with Rich console formatting
- **Pattern**: RichHandler for TTY, StreamHandler for non-TTY (cron compatibility)
- **Configuration**: `logging.yaml` for structured logging setup

### File Organization
- **Notebooks**: Exploratory analysis in `notebooks/` (zarr conversion, case prep)
- **Docker Contexts**: Each `pyonhm/*/Dockerfile` builds specialized service
- **Entry Scripts**: `run_*` scripts in Docker contexts handle service logic

## Key Integration Points

### Data Dependencies
- **PRMS Model Data**: Downloaded from Zenodo (see `nhm_*.env` PRMS_SOURCE)
- **Climate Forcing**: GridMET (operational), CFSv2 (forecasts), NMME (seasonal)
- **Output Formats**: PRMS binary → NetCDF → Zarr (for analysis/visualization)

### External APIs
- **gdptools**: Climate data retrieval (`pyproject.toml` dependency)
- **Docker API**: Direct container management via `docker` Python library
- **Scientific Stack**: xarray, zarr for array processing

## Critical File Paths
- `pyonhm/docker_compose_manager.py`: Main CLI orchestration logic
- `pyonhm/base/Dockerfile`: Foundation image with scientific libraries
- `docker-compose.yml`: Service definitions and volume mounts
- `nhm_*.env`: Environment-specific configuration (CONUS vs UC test cases)
- `pyonhm/utils.py`: Logging, datetime utilities shared across services

## AI Agent Instructions

### Output Formatting
- **Raw Markdown Requests**: When user requests "raw markdown" or asks for markdown to copy/paste, provide the content in a plain text code block (```text or ```markdown) for easy copying.

### Development Guidelines
When working with this codebase:
1. Always consider the Docker service boundary for any data processing logic
2. Use the shared `/nhm` volume mount pattern for inter-service communication
3. Follow the environment variable pattern in `nhm_*.env` for configuration
4. Test changes with both CONUS and UC (Upper Colorado) test cases
5. Consider cron job compatibility when modifying CLI behavior