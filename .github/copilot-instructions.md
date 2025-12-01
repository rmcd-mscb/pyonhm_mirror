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
conda env create -f environment.yml  # Creates minimal conda env with poetry
conda activate pyonhm
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

---

## Code Generation Guidelines

### Python Code Standards

#### Logging Pattern
Always use the established logging pattern from `utils.py`:
```python
import logging
from pyonhm import utils

utils.setup_logging()  # Call once at module entry point
logger = logging.getLogger(__name__)

# Usage
logger.info("Processing started")
logger.error(f"Failed to process: {error}")
logger.exception("Unexpected error")  # Includes traceback
```

#### Type Hints
Use type hints consistently, following the existing pattern:
```python
from typing import Optional, Dict, List, Tuple
from pathlib import Path

def process_data(
    input_path: Path,
    env_vars: Optional[Dict[str, str]] = None,
) -> Optional[subprocess.CompletedProcess]:
    """Docstring with Args and Returns sections."""
```

#### Docstring Format
Follow Google-style docstrings as used in `docker_compose_manager.py`:
```python
def function_name(param1: str, param2: int) -> bool:
    """
    Brief description of function purpose.

    Args:
        param1 (str): Description of param1.
        param2 (int): Description of param2.

    Returns:
        bool: Description of return value.

    Raises:
        ValueError: When invalid input is provided.
    """
```

#### Path Handling
Always use `pathlib.Path` for file system operations:
```python
from pathlib import Path

output_path = Path(args.output_path)
output_file = output_path / f"{output_path.name}.zarr"

if not output_path.exists():
    output_path.mkdir(parents=True, exist_ok=True)
```

### CLI Command Development

#### Cyclopts Pattern
New CLI commands should follow the `docker_compose_manager.py` pattern:
```python
from cyclopts import App, Group, Parameter

app = App(default_parameter=Parameter(negative=()))

# Group commands logically
g_admin = Group.create_ordered(
    name="Admin Commands", 
    help="Administrative operations"
)

@app.command(group=g_admin)
def new_command(
    env_file: str = "nhm_conus.env",
    verbose: bool = False,
) -> None:
    """Command description shown in help."""
    manager = DockerComposeManager()
    # Implementation
```

### Docker Service Development

#### New Service Checklist
When creating a new Docker service:
1. **Dockerfile**: Inherit from `pyonhm/base` image
2. **Entry script**: Create executable `run_<service>` script
3. **docker-compose.yml**: Add service definition with `/nhm` volume mount
4. **Environment variables**: Document in both `nhm_conus.env` and `nhm_uc.env`
5. **Build order**: Update `build_images()` in `docker_compose_manager.py`

#### Dockerfile Template
```dockerfile
FROM pyonhm-base:latest

LABEL maintainer="your.email@usgs.gov"

# Copy service-specific scripts
COPY run_<service> /usr/local/bin/
RUN chmod +x /usr/local/bin/run_<service>

# Set working directory
WORKDIR /nhm

# Default command
CMD ["run_<service>"]
```

#### Entry Script Pattern
Entry scripts should handle environment variable validation:
```python
#!/usr/bin/env python3
import os
import sys

def check_env_variable(var_name: str) -> str:
    """Check if an environment variable is set."""
    value = os.getenv(var_name)
    if not value:
        print(f"Error: {var_name} environment variable is not set.")
        sys.exit(1)
    return value

def main():
    input_dir = check_env_variable("INPUT_DIR")
    output_dir = check_env_variable("OUTPUT_DIR")
    # ... processing logic
```

### Environment Variable Conventions

#### Naming Pattern
- **Prefix by context**: `OP_` (operational), `FCST_`/`FRCST_` (forecast), `GM_` (GridMET), `CFSV2_` (CFSv2)
- **Suffix by type**: `_DIR` (directory), `_FILE` (file path), `_PKG` (package name), `_SOURCE` (URL)

#### Documentation Requirement
When adding new environment variables:
1. Add to both `nhm_conus.env` and `nhm_uc.env` with comments
2. Document expected format and example values
3. Add validation in the consuming service

```dotenv
# Variables for new-service
# Description of what this controls
NEW_SERVICE_INPUT_DIR=/nhm/NHM_PRMS_CONUS_GF_1_1/input/
NEW_SERVICE_OUTPUT_DIR=/nhm/NHM_PRMS_CONUS_GF_1_1/output/
```

### Data Processing Patterns

#### xarray/NetCDF Pattern
Follow the pattern in `ncf2zarr.py` and `out2ncf.py`:
```python
import xarray as xr
import zarr

# Compression settings for Zarr output
compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)

# Open NetCDF files with chunking for memory efficiency
ds = xr.open_mfdataset(
    file_pattern, 
    combine='by_coords', 
    parallel=True, 
    chunks={}
)

# Encoding for variables
encoding = {var: {"compressor": compressor} for var in ds.data_vars}
```

#### Argument Parsing
Use `argparse` for standalone scripts within Docker services:
```python
import argparse
from pathlib import Path

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Brief description of script purpose."
    )
    parser.add_argument(
        '--output-path',
        type=Path,
        required=True,
        help="Directory for output files."
    )
    parser.add_argument(
        '--mode',
        required=True,
        choices=["op", "median", "ensemble"],
        help="Mode of operation."
    )
    return parser.parse_args()
```

### Error Handling

#### Subprocess Execution
Follow the pattern in `DockerComposeManager.run_compose_command()`:
```python
try:
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Command failed with return code {result.returncode}")
        logger.error(f"stdout: {result.stdout}")
        logger.error(f"stderr: {result.stderr}")
    return result
except Exception as e:
    logger.exception("Command execution failed")
    return None
```

#### Exit Codes
Use consistent exit codes for Docker service scripts:
- `0`: Success
- `1`: General error / missing environment variable
- `2`: Invalid input / file not found

### Rich Console Output

For CLI-facing code, use Rich for formatted output:
```python
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.pretty import pprint

console = Console()

# Tables for structured data
table = Table(title="Service Status")
table.add_column("Service", style="cyan")
table.add_column("Status", style="green")
console.print(table)

# Panels for emphasis
console.print(Panel("Operation Complete", style="bold green"))
```

### Cron Compatibility

When modifying CLI behavior, ensure cron compatibility:
- Logging falls back to `StreamHandler` when not TTY (handled by `setup_logging()`)
- Avoid interactive prompts
- Use absolute paths or rely on environment variables
- Handle missing display (`DISPLAY` not set) gracefully