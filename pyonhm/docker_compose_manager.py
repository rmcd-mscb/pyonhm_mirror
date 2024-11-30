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

    def run_service(self, service_name, command_override=None, env_vars=None):
        command = ['run', '--rm']
        if env_vars:
            env_options = [item for key, value in env_vars.items() for item in ['-e', f'{key}={value}']]
            command.extend(env_options)
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

def main():
    try:
        app()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()