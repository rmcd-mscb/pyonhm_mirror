from cyclopts import App, Group, Parameter
from pyonhm import utils
import subprocess
import logging

import os
import shlex
import sys

utils.setup_logging()
logger = logging.getLogger(__name__)
logger.info("pyonhm application started")

app = App(
    default_parameter=Parameter(negative=()),
    )
g_build_load = Group.create_ordered(name="Admin Commands", help="Build images and load supporting data into volume")
g_operational = Group.create_ordered(name="Operational Commands", help="NHM daily operational model methods")
g_sub_seasonal = Group.create_ordered(name="Sub-seasonal Forecast Commands", help="NHM sub-seasonal forecasts model methods")
g_seasonal = Group.create_ordered(name="Seasonal Forecast Commands", help="NHM seasonal forecasts model methods")


class DockerComposeManager:
    def __init__(self, compose_file='docker-compose.yml'):
        self.compose_file = compose_file
        self.compose_cmd = self.get_docker_compose_command()

    def get_docker_compose_command(self):
        # Check if 'docker compose' is available
        result = subprocess.run(['docker', 'compose', 'version'], capture_output=True)
        if result.returncode == 0:
            return ['docker', 'compose', '-f', self.compose_file]
        # Fallback to 'docker-compose'
        result = subprocess.run(['docker-compose', 'version'], capture_output=True)
        if result.returncode == 0:
            return ['docker-compose', '-f', self.compose_file]
        logger.error("Neither 'docker compose' nor 'docker-compose' is available.")
        raise RuntimeError("Neither 'docker compose' nor 'docker-compose' is available.")

    def run_compose_command(self, command, env_vars=None):
        cmd = self.compose_cmd + command
        logger.info(f"Running command: {' '.join(cmd)}")
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)
        # Ensure all environment variable values are strings
        env = {k: str(v) for k, v in env.items()}
        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Command failed with return code {result.returncode}")
                logger.error(f"Output: {result.stdout}")
                logger.error(f"Error Output: {result.stderr}")
            return result
        except Exception as e:
            logger.exception(f"Command execution failed")
            return None

    def run_service(self, service_name, command_override=None, env_vars=None, working_dir=None):
        command = ['run', '--rm']
        if env_vars:
            for key, value in env_vars.items():
                command.extend(['-e', f'{key}={value}'])
        if working_dir:
            command.extend(['-w', working_dir])
        command.append(service_name)
        if command_override:
            command.extend(command_override)
        result = self.run_compose_command(command, env_vars=env_vars)
        return result


    def up_service(self, service_name, env_vars=None):
        command = ['up', '-d', service_name]
        return self.run_compose_command(command, env_vars=env_vars)

    def down(self):
        return self.run_compose_command(['down'])

    def stop_service(self, service_name):
        return self.run_compose_command(['stop', service_name])

    def remove_service(self, service_name):
        return self.run_compose_command(['rm', '-f', service_name])

    def build_images(self, no_cache=False):
        """Builds all Docker images defined in the docker-compose.yml file.

        Args:
            no_cache (bool): If True, builds images without using the cache.
        """
        images_in_order = ['base', 'gridmetetl', 'ncf2cbh', 'prms', 'out2ncf', 'cfsv2etl']
        for service in images_in_order:
            command = ['build']
            if no_cache:
                command.append('--no-cache')
            command.append(service)
            result = self.run_compose_command(command)
            if result and result.returncode != 0:
                logger.error(f"Failed to build image for service {service}.")
                return result
        logger.info("All Docker images built successfully.")
        return None


    def load_data(self, env_vars: dict):
        """
        Download necessary data using Docker containers.
        """
        logger.info("Downloading data...")
        # self.download_fabric_data(env_vars=env_vars)
        self.download_model_data(env_vars=env_vars)
        self.download_model_test_data(env_vars=env_vars)

    def download_data_if_not_exists(self, env_vars, service_name, check_path, download_commands):
        if not self.check_data_exists(
            service_name=service_name,
            check_path=check_path,
            env_vars=env_vars
        ):
            logger.info(f"Data at {check_path} not found. Proceeding with download.")
            # Pass the commands to the container to execute them
            self.download_data(
                service_name=service_name,
                working_dir="/nhm",
                download_commands=download_commands,
                env_vars=env_vars
            )
        else:
            logger.info(f"Data at {check_path} already exists. Skipping download.")

    def check_data_exists(self, service_name, check_path, env_vars=None):
        """Check if specific data exists in a Docker container.

        Args:
            service_name (str): The name of the service to run.
            check_path (str): The path within the container to check for data.
            env_vars (dict): Environment variables to pass to the container.

        Returns:
            bool: True if the data exists at the specified path, False otherwise.
        """
        logger.info(f"Checking if data at {check_path} is downloaded...")
        command_override = ['sh', '-c', f'test -e {check_path} && echo 0 || echo 1']
        result = self.run_service(
            service_name=service_name,
            command_override=command_override,
            env_vars=env_vars
        )
        if result and result.returncode == 0:
            status_code = result.stdout.strip()
            return status_code == '0'
        else:
            logger.error("Failed to check data existence.")
            return False
        
    def download_model_test_data(self, env_vars):
        """Download model test data if it is not already present."""
        logger.info("Checking if model test data exists...")

        check_path = "/nhm/NHM_PRMS_UC_GF_1_1"
        service_name = "base"

        # Validate required environment variables
        if 'PRMS_TEST_SOURCE' not in env_vars or 'PRMS_TEST_DATA_PKG' not in env_vars:
            logger.error("Missing required environment variables: PRMS_TEST_SOURCE or PRMS_TEST_DATA_PKG")
            return

        # Sanitize and validate inputs
        prms_test_source = shlex.quote(env_vars['PRMS_TEST_SOURCE'])
        prms_test_data_pkg = shlex.quote(env_vars['PRMS_TEST_DATA_PKG'])

        # Construct commands as a list of arguments
        prms_test_download_commands = [
            f"wget --waitretry=3 --retry-connrefused --timeout=30 --tries=10 {prms_test_source}",
            f"unzip {prms_test_data_pkg}",
            "chown -R nhm:nhm /nhm/NHM_PRMS_UC_GF_1_1",
            "chmod -R 766 /nhm/NHM_PRMS_UC_GF_1_1"
        ]

        # Log the commands for debugging purposes
        logger.debug("PRMS model test download commands: %s", prms_test_download_commands)

        # Pass the commands to the container to execute them
        self.download_data_if_not_exists(
            env_vars=env_vars,
            service_name=service_name,
            check_path=check_path,
            download_commands=prms_test_download_commands
        )

    def download_data(self, service_name, working_dir, download_commands, env_vars=None):
        """Download data using a specified Docker Compose service.

        Args:
            service_name (str): The name of the service to run.
            working_dir (str): The working directory inside the container.
            download_commands (list): A list of shell commands to execute for downloading the data.
            env_vars (dict): Environment variables to pass to the container.
        """
        command_str = ' && '.join(download_commands)
        # Prepare the override command
        command_override = ['sh', '-c', f'cd {working_dir} && {command_str}']
        result = self.run_service(
            service_name=service_name,
            command_override=command_override,
            env_vars=env_vars
        )
        if result and result.returncode == 0:
            logger.info(f"Data download completed in service '{service_name}'.")
        else:
            logger.error(f"Data download failed in service '{service_name}'.")
            if result:
                logger.error(f"Command output: {result.stdout}")
                logger.error(f"Command error: {result.stderr}")

    def download_model_data(self, env_vars):
        logger.info("Checking if model data exists...")

        check_path = "/nhm/NHM_PRMS_CONUS_GF_1_1"
        service_name = "base"

        if 'PRMS_SOURCE' not in env_vars or 'PRMS_DATA_PKG' not in env_vars:
            logger.error("Missing required environment variables: PRMS_SOURCE or PRMS_DATA_PKG")
            return

        prms_source = shlex.quote(env_vars['PRMS_SOURCE'])
        prms_data_pkg = shlex.quote(env_vars['PRMS_DATA_PKG'])

        download_commands = [
            f"wget --waitretry=3 --retry-connrefused --timeout=30 --tries=10 {prms_source}",
            f"unzip {prms_data_pkg}",
            "chown -R nhm:nhm /nhm/NHM_PRMS_CONUS_GF_1_1",
            "chmod -R 766 /nhm/NHM_PRMS_CONUS_GF_1_1"
        ]

        self.download_data_if_not_exists(
        env_vars=env_vars,
        service_name=service_name,
        check_path=check_path,
        download_commands=download_commands
    )

    def operational_run(self, env_vars: dict, test: bool = False, num_days: int = 4, override: bool = False):
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
                logger.info(f"Environment dates updated for testing for {num_days} days.")
            except Exception as e:
                logger.error(f"Failed to update environment dates for testing: {e}")
                return
        else:
            try:
                status_list, date_list = utils.gridmet_updated()
                gm_status, end_date_str = utils.check_consistency(status_list, date_list)
                if not gm_status and override:
                    logger.info("Override active: Using consistent date despite gm_status being False.")
                    gm_status = True  # Force gm_status to True to proceed
                elif not gm_status:
                    logger.error("GridMet not yet updated - Try again later.")
                    return
                utils.env_update_dates(restart_date=restart_date, end_date=end_date_str, env_vars=env_vars)
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

        working_dir = f"{project_root}/daily/restart" if mode == "op" else f"{project_root}/forecast/restart"

        # Command to list and get the latest restart file date
        command_override = ['bash', '-c', 'ls -1 *.restart | sort | tail -1 | cut -f1 -d \'.\'']

        result = self.run_service(
            service_name='base',
            command_override=command_override,
            env_vars=env_vars,
            working_dir=working_dir
        )

        if result and result.returncode == 0:
            restart_date = result.stdout.strip()
            if restart_date:
                return restart_date
            else:
                raise FileNotFoundError("No .restart files found in the specified directory.")
        else:
            raise RuntimeError("Failed to retrieve the latest restart date.")


    def op_containers(self, env_vars, restart_date=None):
        """Run operational containers for data processing and analysis."""
        logger.info("Starting operational containers...")

        try:
            # Run gridmetetl container
            self.run_service(
                service_name='gridmetetl',
                env_vars=env_vars
            )

            # Prepare environment variables for ncf2cbh
            ncf2cbh_vars = utils.get_ncf2cbh_opvars(env_vars=env_vars, mode="op")
            self.run_service(
                service_name='ncf2cbh',
                env_vars=ncf2cbh_vars
            )

            # Prepare environment variables for prms run
            prms_env = utils.get_prms_run_env(env_vars=env_vars, restart_date=restart_date)
            self.run_service(
                service_name='prms',
                env_vars=prms_env
            )

            # Prepare environment variables for out2ncf
            out2ncf_vars = utils.get_out2ncf_vars(env_vars=env_vars, mode="op")
            self.run_service(
                service_name='out2ncf',
                env_vars=out2ncf_vars
            )

            # Prepare environment variables for prms restart
            prms_restart_env = utils.get_prms_restart_env(env_vars=env_vars)
            self.run_service(
                service_name='prms',
                env_vars=prms_restart_env
            )

        except Exception as e:
            logger.error(f"An error occurred during container operations: {e}")
            sys.exit(1)

    def print_env_vars(self, env_vars):
        """Print environment variables for debugging purposes."""
        logger.debug("Environment Variables:")
        for key, value in env_vars.items():
            logger.debug(f"{key}={value}")

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
            self.run_service(
                service_name='cfsv2etl',
                env_vars=cfsv2_env
            )

        except Exception as e:
            logger.error(f"An error occurred during container operations: {e}")
            sys.exit(1)

@app.command(group=g_build_load)
def build_images(*, no_cache: bool=False):
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
    *,
    env_file: str,
    test: bool = False,
    num_days: int = 4,
    override: bool = False
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
            env_vars=dict_env_vars,
            test=test,
            num_days=num_days,
            override=override
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
        print(f"Error: '{method}' is not a valid method. Please use 'ensemble' or 'median'.")
        sys.exit(1)  # Exit with error code 1 to indicate failure
    try:
        compose_manager.update_cfsv2(env_vars=dict_env_vars, method=method)
    except Exception as e:
        logger.error(f"An error occurred while running the operational simulation: {e}")

def main():
    try:
        app()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()