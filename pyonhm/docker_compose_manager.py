import logging
import os
import shlex
import subprocess
import sys

from cyclopts import App, Group, Parameter
from pathlib import Path
from pyonhm import utils
from rich.pretty import pprint
from rich.pretty import Pretty
from rich.panel import Panel
from rich.table import Table
from rich.console import Console
from typing import Optional, Dict, List

utils.setup_logging()
logger = logging.getLogger(__name__)
logger.info("pyonhm application started")

app = App(
    default_parameter=Parameter(negative=()),
)
g_build_load = Group.create_ordered(
    name="Admin Commands", help="Build images and load supporting data into volume"
)
g_operational = Group.create_ordered(
    name="Operational Commands", help="NHM daily operational model methods"
)
g_sub_seasonal = Group.create_ordered(
    name="Sub-seasonal Forecast Commands",
    help="NHM sub-seasonal forecasts model methods",
)
g_seasonal = Group.create_ordered(
    name="Seasonal Forecast Commands", help="NHM seasonal forecasts model methods"
)

class DockerComposeManager:
    """
    A utility class to manage Docker Compose operations programmatically.

    Attributes:
        compose_file (str): Path to the Docker Compose YAML file.
        compose_cmd (List[str]): Base command for running Docker Compose operations.
    """

    def __init__(self, compose_file: str = "docker-compose.yml") -> None:
        """
        Initializes the DockerComposeManager with a specified compose file.

        Args:
            compose_file (str): Path to the Docker Compose YAML file. Defaults to 'docker-compose.yml'.
        """
        self.compose_file = compose_file
        self.compose_cmd = self.get_docker_compose_command()

    def get_docker_compose_command(self) -> List[str]:
        """
        Determines the appropriate Docker Compose command to use.

        Returns:
            List[str]: The base Docker Compose command with the compose file included.

        Raises:
            RuntimeError: If neither 'docker compose' nor 'docker-compose' is available.
        """
        result = subprocess.run(["docker", "compose", "version"], capture_output=True)
        if result.returncode == 0:
            return ["docker", "compose", "-f", self.compose_file]

        result = subprocess.run(["docker-compose", "version"], capture_output=True)
        if result.returncode == 0:
            return ["docker-compose", "-f", self.compose_file]

        logger.error("Neither 'docker compose' nor 'docker-compose' is available.")
        raise RuntimeError(
            "Neither 'docker compose' nor 'docker-compose' is available."
        )

    def run_compose_command(
        self, command: List[str], env_vars: Optional[Dict[str, str]] = None
    ) -> Optional[subprocess.CompletedProcess]:
        """
        Runs a Docker Compose command with optional environment variables.

        Args:
            command (List[str]): The Docker Compose command to execute.
            env_vars (Optional[Dict[str, str]]): Additional environment variables for the command. Defaults to None.

        Returns:
            Optional[subprocess.CompletedProcess]: The result of the command execution, or None if an error occurred.
        """
        cmd = self.compose_cmd + command
        logger.info(pprint(cmd))
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)
        env = {k: str(v) for k, v in env.items()}  # Ensure all values are strings

        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Command failed with return code {result.returncode}")
                logger.error("Output:")
                logger.error("\n" + result.stdout)
                logger.error("Error Output:")
                logger.error("\n" + result.stderr)
            return result
        except Exception as e:
            logger.exception("Command execution failed")
            return None

    def run_service(
        self,
        service_name: str,
        command_override: Optional[List[str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        working_dir: Optional[str] = None,
    ) -> Optional[subprocess.CompletedProcess]:
        """
        Runs a service with optional command overrides and environment variables.

        Args:
            service_name (str): Name of the service to run.
            command_override (Optional[List[str]]): Override for the default command. Defaults to None.
            env_vars (Optional[Dict[str, str]]): Additional environment variables for the command. Defaults to None.
            working_dir (Optional[str]): Working directory for the service. Defaults to None.

        Returns:
            Optional[subprocess.CompletedProcess]: The result of the command execution, or None if an error occurred.
        """
        command = ["run", "--rm"]
        if env_vars:
            for key, value in env_vars.items():
                command.extend(["-e", f"{key}={value}"])
        if working_dir:
            command.extend(["-w", working_dir])
        command.append(service_name)
        if command_override:
            command.extend(command_override)

        # logger.info("Running service with the following command:")
        # temp = pprint(command)
        # logger.info(temp)

        return self.run_compose_command(command, env_vars=env_vars)

    def up_service(
        self, service_name: str, env_vars: Optional[Dict[str, str]] = None
    ) -> Optional[subprocess.CompletedProcess]:
        """
        Brings up a specified service in detached mode.

        Args:
            service_name (str): Name of the service to bring up.
            env_vars (Optional[Dict[str, str]]): Additional environment variables for the command. Defaults to None.

        Returns:
            Optional[subprocess.CompletedProcess]: The result of the command execution, or None if an error occurred.
        """
        command = ["up", "-d", service_name]
        return self.run_compose_command(command, env_vars=env_vars)

    def down(self) -> Optional[subprocess.CompletedProcess]:
        """
        Brings down all services defined in the compose file.

        Returns:
            Optional[subprocess.CompletedProcess]: The result of the command execution, or None if an error occurred.
        """
        return self.run_compose_command(["down"])

    def stop_service(self, service_name: str) -> Optional[subprocess.CompletedProcess]:
        """
        Stops a specified service.

        Args:
            service_name (str): Name of the service to stop.

        Returns:
            Optional[subprocess.CompletedProcess]: The result of the command execution, or None if an error occurred.
        """
        return self.run_compose_command(["stop", service_name])

    def remove_service(
        self, service_name: str
    ) -> Optional[subprocess.CompletedProcess]:
        """
        Removes a specified service.

        Args:
            service_name (str): Name of the service to remove.

        Returns:
            Optional[subprocess.CompletedProcess]: The result of the command execution, or None if an error occurred.
        """
        return self.run_compose_command(["rm", "-f", service_name])

    def build_images(self, no_cache=False):
        """Builds all Docker images defined in the docker-compose.yml file.

        Args:
            no_cache (bool): If True, builds images without using the cache.
        """
        images_in_order = [
            "base",
            "gridmetetl",
            "ncf2cbh",
            "prms",
            "out2ncf",
            "cfsv2etl",
            "ncf2zarr"
        ]
        for service in images_in_order:
            command = ["build"]
            if no_cache:
                command.append("--no-cache")
            command.append(service)
            result = self.run_compose_command(command)
            if result and result.returncode != 0:
                logger.error(f"Failed to build image for service {service}.")
                return result
        logger.info("All Docker images built successfully.")
        return True

    def load_data(self, env_vars: Dict[str, str]) -> None:
        """
        Download necessary data using Docker containers.

        Args:
            env_vars (Dict[str, str]): Environment variables to use for downloading data.
        """
        logger.info("Downloading data...")
        self.download_model_data(env_vars=env_vars)
        self.download_model_test_data(env_vars=env_vars)


    def download_data_if_not_exists(
        self,
        env_vars: Dict[str, str],
        service_name: str,
        check_path: str,
        download_commands: List[str],
    ) -> None:
        """
        Check if data exists and download it if not.

        Args:
            env_vars (Dict[str, str]): Environment variables for the container.
            service_name (str): The name of the Docker Compose service to use.
            check_path (str): The path within the container to check for data.
            download_commands (List[str]): A list of shell commands to execute for downloading the data.
        """
        if not self.check_data_exists(
            service_name=service_name, check_path=check_path, env_vars=env_vars
        ):
            logger.info(f"Data at {check_path} not found. Proceeding with download.")
            self.download_data(
                service_name=service_name,
                working_dir="/nhm",
                download_commands=download_commands,
                env_vars=env_vars,
            )
        else:
            logger.info(f"Data at {check_path} already exists. Skipping download.")

    def check_data_exists(
        self,
        service_name: str,
        check_path: str,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Check if specific data exists in a Docker container.

        Args:
            service_name (str): The name of the service to run.
            check_path (str): The path within the container to check for data.
            env_vars (Optional[Dict[str, str]]): Environment variables to pass to the container.

        Returns:
            bool: True if the data exists at the specified path, False otherwise.
        """
        logger.info(f"Checking if data at {check_path} is downloaded...")
        command_override = ["sh", "-c", f"test -e {check_path} && echo 0 || echo 1"]
        result = self.run_service(
            service_name=service_name,
            command_override=command_override,
            env_vars=env_vars,
        )
        if result and result.returncode == 0:
            status_code = result.stdout.strip()
            return status_code == "0"
        else:
            logger.error("Failed to check data existence.")
            return False

    def download_model_test_data(self, env_vars: Dict[str, str]) -> None:
        """
        Download model test data if it is not already present.

        Args:
            env_vars (Dict[str, str]): Environment variables for the container.
        """
        logger.info("Checking if model test data exists...")

        check_path = env_vars["PROJECT_ROOT"]
        service_name = "base"

        # Validate required environment variables
        if "PRMS_TEST_SOURCE" not in env_vars or "PRMS_TEST_DATA_PKG" not in env_vars:
            logger.error(
                "Missing required environment variables: PRMS_TEST_SOURCE or PRMS_TEST_DATA_PKG"
            )
            return

        # Quote paths and variables to ensure safety
        check_path_quote = shlex.quote(check_path)
        prms_test_source = shlex.quote(env_vars["PRMS_TEST_SOURCE"])
        prms_test_data_pkg = shlex.quote(env_vars["PRMS_TEST_DATA_PKG"])

        # Define the download commands
        prms_test_download_commands = [
            f"wget --waitretry=3 --retry-connrefused --timeout=30 --tries=10 {prms_test_source}",
            f"unzip {prms_test_data_pkg}",
            f"chown -R nhm:nhm {check_path}",
            f"chmod -R 766 {check_path}",
        ]

        logger.debug(
            "PRMS model test download commands: %s", prms_test_download_commands
        )

        # Execute the download process if the data doesn't already exist
        self.download_data_if_not_exists(
            env_vars=env_vars,
            service_name=service_name,
            check_path=check_path_quote,
            download_commands=prms_test_download_commands,
        )

    def download_model_data(self, env_vars: Dict[str, str]) -> None:
        """
        Download model data if it is not already present.

        Args:
            env_vars (Dict[str, str]): Environment variables for the container.
        """
        logger.info("Checking if model data exists...")

        check_path = env_vars["PROJECT_ROOT"]
        service_name = "base"

        if "PRMS_SOURCE" not in env_vars or "PRMS_DATA_PKG" not in env_vars:
            logger.error(
                "Missing required environment variables: PRMS_SOURCE or PRMS_DATA_PKG"
            )
            return

        check_path_quote = shlex.quote(check_path)
        prms_source = shlex.quote(env_vars["PRMS_SOURCE"])
        prms_data_pkg = shlex.quote(env_vars["PRMS_DATA_PKG"])

        download_commands = [
            f"wget --waitretry=3 --retry-connrefused --timeout=30 --tries=10 {prms_source}",
            f"unzip {prms_data_pkg}",
            f"chown -R nhm:nhm {check_path}",
            f"chmod -R 766 {check_path}",
        ]

        self.download_data_if_not_exists(
            env_vars=env_vars,
            service_name=service_name,
            check_path=check_path,
            download_commands=download_commands,
        )

    def download_data(
        self,
        service_name: str,
        working_dir: str,
        download_commands: List[str],
        env_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Download data using a specified Docker Compose service.

        Args:
            service_name (str): The name of the service to run.
            working_dir (str): The working directory inside the container.
            download_commands (List[str]): A list of shell commands to execute for downloading the data.
            env_vars (Optional[Dict[str, str]]): Environment variables to pass to the container.
        """
        command_str = " && ".join(download_commands)
        command_override = ["sh", "-c", f"cd {working_dir} && {command_str}"]
        result = self.run_service(
            service_name=service_name,
            command_override=command_override,
            env_vars=env_vars,
        )
        if result and result.returncode == 0:
            logger.info(f"Data download completed in service '{service_name}'.")
        else:
            logger.error(f"Data download failed in service '{service_name}'.")
            if result:
                logger.error(f"Command output: {result.stdout}")
                logger.error(f"Command error: {result.stderr}")

    def operational_run(
        self,
        env_vars: dict,
        test: bool = False,
        num_days: int = 4,
        override: bool = False,
    ):
        """Execute operational tasks related to forecasting."""
        logger.info("Running operational tasks...")
        self.env_vars = env_vars  # Store env_vars if needed in other methods

        try:
            restart_date = self.get_latest_restart_date(env_vars=env_vars, mode="op")
            logger.info(f"Operational restart date: {restart_date}")
        except Exception as e:
            logger.error(f"Failed to retrieve the latest restart date: {e}")
            return

        if test:
            try:
                utils.env_update_dates_for_testing(
                    restart_date=restart_date, env_vars=env_vars, num_days=num_days
                )
                logger.info(
                    f"Environment dates updated for testing for {num_days} days."
                )
            except Exception as e:
                logger.error(f"Failed to update environment dates for testing: {e}")
                return
        else:
            try:
                status_list, date_list = utils.gridmet_updated()
                gm_status, end_date_str = utils.check_consistency(
                    status_list, date_list
                )
                if not gm_status and override:
                    logger.info(
                        "Override active: Using consistent date despite gm_status being False."
                    )
                    gm_status = True  # Force gm_status to True to proceed
                elif not gm_status:
                    logger.error("GridMet not yet updated - Try again later.")
                    return
                utils.env_update_dates(
                    restart_date=restart_date, end_date=end_date_str, env_vars=env_vars
                )
                logger.info(f"GridMet updated relative to yesterday: {gm_status}")
            except Exception as e:
                logger.error(f"Failed to update environment dates: {e}")
                return

        # Optionally print env_vars for debugging
        self.print_env_vars(env_vars)

        try:
            self.op_containers(env_vars, restart_date)
            logger.info("Operational containers executed successfully.")
        except Exception as e:
            logger.error(f"Failed to run operational containers: {e}")

    def get_latest_restart_date(self, env_vars: dict, mode: str):
        """Finds and returns the date of the latest restart file in a specified directory within a Docker container."""
        if mode not in ["op", "forecast"]:
            raise ValueError(f"Invalid mode '{mode}'. Mode must be 'op' or 'forecast'.")

        project_root = env_vars.get("PROJECT_ROOT")
        if not project_root:
            raise ValueError("PROJECT_ROOT not defined in environment variables.")

        working_dir = (
            f"{project_root}/daily/restart"
            if mode == "op"
            else f"{project_root}/forecast/restart"
        )

        # Command to list and get the latest restart file date
        command_override = [
            "bash",
            "-c",
            "ls -1 *.restart | sort | tail -1 | cut -f1 -d '.'",
        ]

        result = self.run_service(
            service_name="base",
            command_override=command_override,
            env_vars=env_vars,
            working_dir=working_dir,
        )

        if result and result.returncode == 0:
            restart_date = result.stdout.strip()
            if restart_date:
                return restart_date
            else:
                raise FileNotFoundError(
                    "No .restart files found in the specified directory."
                )
        else:
            raise RuntimeError("Failed to retrieve the latest restart date.")

    def op_containers(self, env_vars, restart_date=None):
        """Run operational containers for data processing and analysis."""
        logger.info("Starting operational containers...")

        try:
            # Run gridmetetl container
            self.run_service(service_name="gridmetetl", env_vars=env_vars)

            # Prepare environment variables for ncf2cbh
            ncf2cbh_vars = utils.get_ncf2cbh_opvars(env_vars=env_vars, mode="op")
            self.run_service(service_name="ncf2cbh", env_vars=ncf2cbh_vars)

            # Prepare environment variables for prms run
            prms_env = utils.get_prms_run_env(
                env_vars=env_vars, restart_date=restart_date
            )
            self.run_service(service_name="prms", env_vars=prms_env)

            # Prepare environment variables for out2ncf
            out2ncf_vars = utils.get_out2ncf_vars(env_vars=env_vars, mode="op")
            self.run_service(service_name="out2ncf", env_vars=out2ncf_vars)

            # Prepare environment variables for prms restart
            prms_restart_env = utils.get_prms_restart_env(env_vars=env_vars)
            self.run_service(service_name="prms", env_vars=prms_restart_env)

        except Exception as e:
            logger.error(f"An error occurred during container operations: {e}")
            sys.exit(1)

    def print_env_vars(self, env_vars: dict):
        """Print environment variables for debugging purposes."""
        logger.debug("Environment Variables:")
        for key, value in env_vars.items():
            logger.debug(f"{key}={value}")

    def print_forecast_env_vars(self, env_vars: dict):
        """
        Print selected environment variables.
        """
        print_keys = [
            "FRCST_START_DATE",
            "FRCST_END_DATE",
            "FRCST_START_TIME",
            "FRCST_END_TIME",
        ]
        for key, value in env_vars.items():
            if key in print_keys:
                logger.info(f"{key}: {value}")

    def update_cfsv2(self, env_vars: dict, method: str) -> None:
        """Update the CFSv2 environment by running a Docker container.

        This function retrieves the necessary environment variables for the CFSv2 processing based on the specified method
        and runs a Docker container to process the data. It logs any errors encountered during the retrieval of environment
        variables or the execution of the container.

        Args:
            env_vars (dict): A dictionary of environment variables required for the CFSv2 processing.
            method (str): The method to use for the CFSv2 update.

        Returns:
            None: This function does not return a value but logs the status of the update process.

        Raises:
            Exception: If there is an error while retrieving the CFSv2 environment variables or running the container.
        """
        logger.info("Running update_cfsv2 tasks...")

        try:
            cfsv2_env = utils.get_cfsv2_env(env_vars=env_vars, method=method)
        except Exception as e:
            logger.error(f"Failed to retrieve CFSv2 environment variables: {e}")
            return

        try:
            self.run_service(service_name="cfsv2etl", env_vars=cfsv2_env)

        except Exception as e:
            logger.error(f"An error occurred during container operations: {e}")
            sys.exit(1)

    def list_date_folders(self, env_vars: dict, path: Path):
        """
        Generates a list of date folders from the specified path by listing directories matching the date pattern.

        Args:
            path (Path): The path to search for date folders.

        Returns:
            list: A list of date folders extracted from the specified path.
        """

        # Bash command to list directories matching the date pattern
        command_override = [
            "bash",
            "-c",
            "find . -maxdepth 1 -type d -name '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' -printf '%f\n'",
        ]
        logger.debug(f"Command override: {command_override}")

        # Run the command using self.run_service
        result = self.run_service(
            service_name="base",
            command_override=command_override,
            env_vars=env_vars,
            working_dir=str(path),
        )
        if result and result.returncode == 0:
            output = result.stdout.strip()
            return [line.strip() for line in output.split("\n") if line.strip()]
        else:
            raise RuntimeError("Failed to list date folders.")

    def forecast_run(self, env_vars: dict, method: str = "median"):
        """Execute forecast tasks based on the specified method.

        This function runs forecast tasks for either a median or ensemble method, checking for the latest restart date
        and preparing the necessary environment. It manages the execution of Docker containers to process climate data
        and logs the progress and results.

        Args:
            env_vars (dict): A dictionary of environment variables required for the forecast process.
            method (str): The method to use for the forecast, either "median" or "ensemble". Defaults to "median".

        Returns:
            None: This function does not return a value but logs the status of the forecast tasks.

        Raises:
            ValueError: If the specified method is not "median" or "ensemble".
        """
        logger.info(f"Running tasks for {method} forecast...")
        if method not in ["median", "ensemble"]:
            raise ValueError(
                f"Invalid method '{method}'. Mode must be 'median' or 'ensemble'."
            )

        median_path = Path(env_vars.get("CFSV2_NCF_IDIR")) / "ensemble_median"
        ensemble_path = Path(env_vars.get("CFSV2_NCF_IDIR")) / "ensembles"
        logger.info("Running forecast tasks...")

        # Get the most recent operational run restart date.
        forecast_restart_date = self.get_latest_restart_date(
            env_vars=env_vars, mode="forecast"
        )
        logger.info(f"Forecast restart date is {forecast_restart_date}")

        utils.env_update_forecast_dates(
            restart_date=forecast_restart_date, env_vars=env_vars
        )
        self.print_forecast_env_vars(env_vars)

        # Get a list of dates representing the available processed climate drivers
        if method == "median":
            forecast_input_dates = self.list_date_folders(
                env_vars=env_vars, path=median_path
            )
        elif method == "ensemble":
            forecast_input_dates = self.list_date_folders(
                env_vars=env_vars, path=ensemble_path
            )

        print(forecast_input_dates)
        state, forecast_run_date = utils.is_next_day_present(
            forecast_input_dates, forecast_restart_date
        )
        logger.info(
            f"{method} forecast ready: {state}, forecast start date: {forecast_run_date}"
        )

        if not state:
            logger.error(
                "The restart date is not suitable to run the forecast. Please use 'run-operational' to update the restart date."
            )
            sys.exit(1)

        if method == "median":
            med_vars = utils.get_ncf2cbh_opvars(env_vars=env_vars, mode=method)
            try:
                self.run_service(service_name="ncf2cbh", env_vars=med_vars)
            except Exception as e:
                logger.error(f"An error occurred during container operations: {e}")
                sys.exit(1)

            prms_env = utils.get_forecast_median_prms_run_env(
                env_vars=env_vars, restart_date=forecast_restart_date
            )
            try:
                self.run_service(service_name="prms", env_vars=prms_env)
            except Exception as e:
                logger.error(f"An error occurred during container operations: {e}")
                sys.exit(1)

            out2ncf_vars = utils.get_out2ncf_vars(env_vars=env_vars, mode="median")
            try:
                self.run_service(service_name="out2ncf", env_vars=out2ncf_vars)
            except Exception as e:
                logger.error(f"An error occurred during container operations: {e}")
                sys.exit(1)

        elif method == "ensemble":
            for idx in range(48):  #  Loop through 48 ensembles
                logger.info(f"Running ensemble number: {idx}")
                ens_vars = utils.get_ncf2cbh_opvars(
                    env_vars=env_vars, mode=method, ensemble=idx
                )
                try:
                    self.run_service(service_name="ncf2cbh", env_vars=ens_vars)
                except Exception as e:
                    logger.error(f"An error occurred during container operations: {e}")
                    sys.exit(1)

                prms_env = utils.get_forecast_ensemble_prms_run_env(
                    env_vars=env_vars, restart_date=forecast_restart_date, n=idx
                )
                try:
                    self.run_service(service_name="prms", env_vars=prms_env)
                except Exception as e:
                    logger.error(f"An error occurred during container operations: {e}")
                    sys.exit(1)

                out2ncf_vars = utils.get_out2ncf_vars(
                    env_vars=env_vars, mode="ensemble", ensemble=idx
                )
                try:
                    self.run_service(service_name="out2ncf", env_vars=out2ncf_vars)
                except Exception as e:
                    logger.error(f"An error occurred during container operations: {e}")
                    sys.exit(1)

            ncf2zarr_vars = utils.get_ncf2zarr_vars(env_vars=env_vars, mode=method)
            try:
                self.run_service(service_name="ncf2zarr", env_vars=ncf2zarr_vars)
            except Exception as e:
                logger.error(f"An error occurred during container operations: {e}")
                sys.exit(1)

    def fetch_output(self, env_vars):
        """
        Fetch output files from a running Docker container (using the `base` service) and manage the container lifecycle.

        Args:
            env_vars (dict): A dictionary containing environment variables, including paths for output and forecast directories.
        """
        output_dir = env_vars.get("OUTPUT_DIR")
        frcst_dir = env_vars.get("FRCST_OUTPUT_DIR")
        project_root = env_vars.get("PROJECT_ROOT")

        if not output_dir or not frcst_dir or not project_root:
            logger.error(
                "Missing required environment variables: OUTPUT_DIR, FRCST_OUTPUT_DIR, PROJECT_ROOT."
            )
            return

        logger.info(f'Output files will show up in the "{output_dir}" directory.')

        # Bring up the `base` service in detached mode
        result = self.up_service("base")
        if result and result.returncode != 0:
            logger.error("Failed to bring up 'base' service.")
            return

        # Get the container ID for the `base` service
        ps_result = self.run_compose_command(["ps", "-q", "base"])
        if ps_result.returncode != 0 or not ps_result.stdout.strip():
            logger.error("Could not retrieve container ID for 'base' service.")
            self.down()
            return

        container_id = ps_result.stdout.strip()

        paths_to_copy = [
            (f"{project_root}/daily/output", output_dir),
            (f"{project_root}/daily/input", output_dir),
            (f"{project_root}/daily/restart", output_dir),
            (f"{project_root}/forecast/input", frcst_dir),
            (f"{project_root}/forecast/output", frcst_dir),
            (f"{project_root}/forecast/restart", frcst_dir),
        ]

        try:
            for src_path, dest_dir in paths_to_copy:
                # Ensure the host destination directory exists
                os.makedirs(dest_dir, exist_ok=True)

                # Copy from container to host using docker cp
                cp_cmd = ["docker", "cp", f"{container_id}:{src_path}", dest_dir]
                logger.info(f"Copying {src_path} to {dest_dir}")
                subprocess.run(cp_cmd, check=True)

                # If the path is not a restart directory, remove files inside the container
                if "restart" not in src_path:
                    # Use docker-compose exec via the run_compose_command
                    rm_cmd = ["exec", "base", "sh", "-c", f"rm -rf {src_path}/*"]
                    rm_result = self.run_compose_command(rm_cmd)
                    if rm_result.returncode != 0:
                        logger.error(
                            f"Failed to remove files inside container for {src_path}"
                        )
                else:
                    logger.info(f"Skipping removal for path '{src_path}'.")

            logger.info("Directories copied and cleaned up successfully.")

        except subprocess.CalledProcessError as e:
            logger.error(f"Error copying directories: {e}")
        finally:
            # Stop and remove the container
            down_result = self.down()
            if down_result and down_result.returncode == 0:
                logger.info("Container(s) removed successfully.")
            else:
                logger.warning("Failed to remove container(s) properly.")
    
    def convert_to_zarr(self, env_vars: dict, method: str):
        logger.info(f"Running tasks for {method} convert output to zarr...")
        if method not in ["median", "ensemble"]:
            raise ValueError(
                f"Invalid method '{method}'. Mode must be 'median' or 'ensemble'."
            )

        median_path = Path(env_vars.get("CFSV2_NCF_IDIR")) / "ensemble_median"
        ensemble_path = Path(env_vars.get("CFSV2_NCF_IDIR")) / "ensembles"
        logger.info("Running convert to zarr tasks...")

        if method == "ensemble":
            ncf2zarr_vars = utils.get_ncf2zarr_vars(env_vars=env_vars, mode=method)
            try:
                self.run_service(service_name="ncf2zarr", env_vars=ncf2zarr_vars)
            except Exception as e:
                logger.error(f"An error occurred during container operations: {e}")
                sys.exit(1)
        elif method == "median":
            logger.error(f"Converting forecasted median ouput to zarr not yet implemented")
            sys.exit(1)

@app.command(group=g_build_load)
def build_images(*, no_cache: bool = False):
    """
    Builds all Docker images using the DockerComposeManager.

    Args:
        no_cache: If True, builds the images without using cache. Defaults to False.

    Returns:
        None
    """
    compose_manager = DockerComposeManager()
    logger.info("Building all Docker images defined in docker-compose.yml...")
    success = compose_manager.build_images(no_cache=no_cache)
    if success:
        logger.info("All Docker images built successfully.")
    else:
        logger.error("Failed to build Docker images.")


@app.command(group=g_build_load)
def load_data(*, env_file: str):
    """
    Loads data using the DockerComposeManager.

    Args:
        env_file: The path to the environment file.

    Returns:
        None
    """
    compose_manager = DockerComposeManager()
    dict_env_vars = utils.load_env_file(env_file)
    compose_manager.load_data(env_vars=dict_env_vars)


@app.command(group=g_operational)
def run_operational(
    *, env_file: str, test: bool = False, num_days: int = 4, override: bool = False
) -> None:
    """Runs the operational simulation using the DockerComposeManager.

    Args:
        env_file: The path to the environment file.
        test: If True, runs the simulation in test mode. Defaults to False.
        num_days: If test is True, then the number of days to run the simulation. Defaults to 4.
        override: If True, override gm_status == False when dates are consistent. Defaults to False.

    Returns:
        None
    """
    compose_manager = DockerComposeManager()

    try:
        dict_env_vars = utils.load_env_file(env_file)
        logger.info(f"Environment variables loaded from '{env_file}'.")
    except Exception as e:
        logger.error(f"Failed to load environment file '{env_file}': {e}")
        return

    # Log to inform the user about how num_days is used
    if test:
        logger.info(f"Running in test mode for {num_days} days.")
    else:
        logger.info("Running in normal mode. --num_days will be ignored.")

    try:
        compose_manager.operational_run(
            env_vars=dict_env_vars, test=test, num_days=num_days, override=override
        )
    except Exception as e:
        logger.error(f"An error occurred while running the operational simulation: {e}")


@app.command(group=g_sub_seasonal)
def run_update_cfsv2_data(*, env_file: str, method: str):
    """
    Runs the update of CFSv2 data using the specified method , either 'ensemble' or 'median'.

    Args:
        env_file (str): Path to the environment file.
        method (str): The method to use for updating data, either 'ensemble' or 'median'.

    Returns:
        None
    """
    compose_manager = DockerComposeManager()

    try:
        dict_env_vars = utils.load_env_file(env_file)
        logger.info(f"Environment variables loaded from '{env_file}'.")
    except Exception as e:
        logger.error(f"Failed to load environment file '{env_file}': {e}")
        return

    if method not in ["ensemble", "median"]:
        print(
            f"Error: '{method}' is not a valid method. Please use 'ensemble' or 'median'."
        )
        sys.exit(1)  # Exit with error code 1 to indicate failure
    try:
        compose_manager.update_cfsv2(env_vars=dict_env_vars, method=method)
    except Exception as e:
        logger.error(f"An error occurred while running the operational simulation: {e}")


@app.command(group=g_seasonal)
def run_seasonal(*, env_file: str, num_days: int = 4, test: bool = False):
    """
    Runs the seasonal operational simulation using the DockerManager.

    Args:
        env_file: The path to the environment file.
        num_days: The number of days to run the simulation for. Defaults to 4.
        test: If True, runs the simulation in test mode. Defaults to False.

    Returns:
        None
    """
    compose_manager = DockerComposeManager()

    try:

        dict_env_vars = utils.load_env_file(env_file)
        logger.info(f"Environment variables loaded from '{env_file}'.")
    except Exception as e:
        logger.error(f"Failed to load environment file '{env_file}': {e}")
        return

    print("TODO")


@app.command(group=g_sub_seasonal)
def run_sub_seasonal(*, env_file: str, method: str) -> None:
    """
    Runs the sub-seasonal operational simulation using the DockerManager.

    Args:
        env_file (str): The path to the environment file.
        method (str): One of ["median"]["ensemble"]

    Returns:
        None
    """
    compose_manager = DockerComposeManager()

    try:

        dict_env_vars = utils.load_env_file(env_file)
        logger.info(f"Environment variables loaded from '{env_file}'.")
    except Exception as e:
        logger.error(f"Failed to load environment file '{env_file}': {e}")
        return

    compose_manager.forecast_run(env_vars=dict_env_vars, method=method)

@app.command(group=g_sub_seasonal)
def conv_output_to_zarr(*, env_file: str, method: str) -> None:
    """
    Runs the sub-seasonal operational simulation using the DockerManager.

    Args:
        env_file (str): The path to the environment file.
        method (str): One of ["median"]["ensemble"]

    Returns:
        None
    """
    compose_manager = DockerComposeManager()

    try:

        dict_env_vars = utils.load_env_file(env_file)
        logger.info(f"Environment variables loaded from '{env_file}'.")
    except Exception as e:
        logger.error(f"Failed to load environment file '{env_file}': {e}")
        return

    compose_manager.convert_to_zarr(env_vars=dict_env_vars, method=method)

@app.command(group=g_operational)
def fetch_op_results(*, env_file: str):
    """
    Fetches operational results using the DockerManager.

    Args:
        env_file: The path to the environment file.

    Returns:
        None
    """
    compose_manager = DockerComposeManager()

    try:
        dict_env_vars = utils.load_env_file(env_file)
        logger.info(f"Environment variables loaded from '{env_file}'.")
    except Exception as e:
        logger.error(f"Failed to load environment file '{env_file}': {e}")
        return
    compose_manager.fetch_output(env_vars=dict_env_vars)


def main():
    try:
        app()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
