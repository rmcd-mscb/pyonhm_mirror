from cyclopts import App, Group, Parameter
import argparse
from io import BytesIO
import docker
import sys
import subprocess
from pyonhm import utils
from docker.errors import ContainerError, ImageNotFound, APIError
from datetime import datetime, timedelta
from pathlib import Path
from typing_extensions import Annotated

app = App(default_parameter=Parameter(negative=()))
g_build_load = Group.create_ordered(name="Admin Commands", help="Build images and load supporting data into volume")
g_operational = Group.create_ordered(name="Operational Commands", help="NHM daily operational model methods")
g_sub_seasonal = Group.create_ordered(name="Sub-seasonal Forecast Commands", help="NHM sub-seasonal forecasts model methods")
g_seasonal = Group.create_ordered(name="Seasonal Forecast Commands", help="NHM seasonal forecasts model methods")


class DockerManager:
    def __init__(self):
        try:
            self.client = docker.from_env()
            self.volume_binding = {"nhm_nhm": {"bind": "/nhm", "mode": "rw"}}
        except docker.errors.DockerException as e:
            print(f"Failed to initialize Docker client: {e}")
            # Handle the failure as appropriate for your application
            # For example, you might want to raise the exception to halt execution
            # or set self.client to None and check before use in other methods.
            self.client = None
        except Exception as e:
            # Catch-all for any other exception, which might not be related to Docker directly
            print(f"An unexpected error occurred: {e}")
            self.client = None

    def build_image(self, context_path, tag, no_cache=False) -> bool:
        """
        Build docker image from context_path and tag. This is useful for debugging

        Args:
            context_path: path to docker image to build
            tag: tag to use for docker image ( ex : docker - v1 )
            no_cache: Don't use the cache when set to True
        """
        # Check if self.client is initialized
        if not self.client:
            print("Docker client is not initialized. Cannot build image.")
            return

        print(f"Building Docker image: {tag} from {context_path}", flush=True)
        try:
            response = self.client.images.build(
                path=context_path, tag=tag, rm=True, nocache=no_cache
            )
            for line in response[1]:
                if "stream" in line:
                    print(line["stream"], end="", flush=True)
            return True
        except Exception as e:
            print(f"Failed to build Docker image: {e}")
            return False

    def container_exists_and_running(self, container_name):
        """
        Check if a container exists and is running.

        This is useful for tests that need to know if a container exists and is running.

        Args:
                container_name: The name of the container

        Returns:
                A tuple of ( exists running )
        """
        try:
            container = self.client.containers.get(container_name)
            return True, container.status == "running"
        except docker.errors.NotFound:
            return False, False

    def manage_container(self, container_name, action):
        try:
            container = self.client.containers.get(container_name)
            if action == "restart" and container.status == "exited":
                print(f"Restarting container '{container_name}'.")
                container.start()
                return container
            elif action == "stop_remove":
                print(f"Stopping and removing container '{container_name}'.")
                container.stop()
                container.remove()
        except docker.errors.NotFound:
            print(f"Container '{container_name}' not found.")

    def check_data_exists(self, image, container_name, volume, check_path):
        print(f"Checking if data at {check_path} is downloaded...")
        exists, _running = self.container_exists_and_running(container_name)
        if exists:
            # Handle the exited container. You can either restart it or remove and recreate it.
            self.manage_container(container_name=container_name, action="stop_remove")
        command = f"sh -c 'test -e {check_path} && echo 0 || echo 1'"
        container = self.client.containers.run(
            image=image,
            name=container_name,
            command=command,
            volumes=self.volume_binding,
            environment=["TERM=dumb"],
            remove=False,
            detach=True,
        )
        _result = container.wait()
        status_code = container.logs().decode("utf-8").strip()
        return status_code == "0"  # Returns True if data exists

    def download_data(
        self, image, container_name, working_dir, download_path, download_commands
    ):
        print(f"Data needs to be downloaded at {download_path}")
        exists, _running = self.container_exists_and_running(container_name)
        if exists:
            # Handle the exited container. You can either restart it or remove and recreate it.
            self.manage_container(container_name=container_name, action="stop_remove")

        container = self.client.containers.run(
            image=image,
            name=container_name,
            command=f"sh -c '{download_commands}'",
            volumes=self.volume_binding,
            working_dir=working_dir,
            environment=["TERM=dumb"],
            remove=False,
            detach=True,
        )
        for log in container.logs(stream=True):
            print(log.decode("utf-8").strip(), flush=True)

    def download_fabric_data(self, env_vars):
        """
        Download HRU data if not already downloaded.

        Args:
                env_vars: Dictionary of environment variables to use when
        """
        if not self.check_data_exists(
            image="nhmusgs/base",
            container_name="base",
            volume="nhm_nhm",
            check_path="/nhm/gridmetetl/nhm_hru_data_gfv11",
        ):
            hru_download_commands = f"""
                wget --waitretry=3 --retry-connrefused {env_vars['HRU_SOURCE']} ;
                unzip -o {env_vars['HRU_DATA_PKG']} -d /nhm/gridmetetl ;
                chown -R nhm /nhm/gridmetetl ;
                chmod -R 766 /nhm/gridmetetl
            """
            self.download_data(
                image="nhmusgs/base",
                container_name="base",
                working_dir="/nhm",
                download_path="/nhm/gridmetetl/nhm_hru_data_gfv11",
                download_commands=hru_download_commands,
            )

    def download_model_data(self, env_vars):
        if not self.check_data_exists(
            image="nhmusgs/base",
            container_name="base",
            volume="nhm_nhm",
            check_path="/nhm/NHM_PRMS_CONUS_GF_1_1",
        ):
            prms_download_commands = f"""
                wget --waitretry=3 --retry-connrefused {env_vars['PRMS_SOURCE']} ;
                unzip {env_vars['PRMS_DATA_PKG']} ;
                chown -R nhm:nhm /nhm/NHM_PRMS_CONUS_GF_1_1 ;
                chmod -R 766 /nhm/NHM_PRMS_CONUS_GF_1_1
            """
            self.download_data(
                image="nhmusgs/base",
                container_name="base",
                working_dir="/nhm",
                download_path="/nhm/NHM_PRMS_CONUS_GF_1_1",
                download_commands=prms_download_commands,
            )

    def download_model_test_data(self, env_vars):
        if not self.check_data_exists(
            image="nhmusgs/base",
            container_name="base",
            volume="nhm_nhm",
            check_path="/nhm/NHM_PRMS_UC_GF_1_1",
        ):
            prms_download_commands = f"""
                wget --waitretry=3 --retry-connrefused {env_vars['PRMS_TEST_SOURCE']} ;
                unzip {env_vars['PRMS_TEST_DATA_PKG']} ;
                chown -R nhm:nhm /nhm/NHM_PRMS_UC_GF_1_1 ;
                chmod -R 766 /nhm/NHM_PRMS_UC_GF_1_1
            """
            self.download_data(
                image="nhmusgs/base",
                container_name="base",
                working_dir="/nhm",
                download_path="/nhm/NHM_PRMS_UC_GF_1_1",
                download_commands=prms_download_commands,
            )

    def get_latest_restart_date(self, env_vars: dict, mode: str):
        """
        Finds and returns the date of the latest restart file in a specified directory within a Docker container.

        This function runs a Docker container to execute a shell command that lists all `.restart` files in a
        specific directory, sorts them, and extracts the date from the filename of the most recent file. It assumes
        that the filenames are structured such that the date can be isolated by removing the file extension.

        Parameters:
        - self: The instance of the class containing this method.
        - env_vars (dict): A dictionary of environment variables where "PROJECT_ROOT" specifies the root directory
        of the project within the container's file system.
        - mode (str): Either "op" or "forecast".

        Returns:
        - str: The date of the latest restart file, extracted from its filename.

        Note:
        This function requires that the Docker client is initialized in the class and that the necessary volume
        bindings are set up in `self.volume_binding` to allow access to the project's files from within the container.
        """
        if mode not in ["op", "forecast"]:
           raise ValueError(f"Invalid mode '{mode}'. Mode must be 'op' or 'forecast'.")
        command = "bash -c 'ls -1 *.restart | sort | tail -1 | cut -f1 -d .'"
        project_root = env_vars.get("PROJECT_ROOT")
        if mode == "op":
            container = self.client.containers.run(
                image="nhmusgs/base",
                command=command,
                volumes=self.volume_binding,
                working_dir=f"{project_root}/daily/restart",
                environment={"TERM": "dumb"},
                detach=True,
                tty=True,
            )
        elif mode == "forecast":
             container = self.client.containers.run(
                image="nhmusgs/base",
                command=command,
                volumes=self.volume_binding,
                working_dir=f"{project_root}/forecast/restart",
                environment={"TERM": "dumb"},
                detach=True,
                tty=True,
            )

        restart_date = container.logs().decode("utf-8").strip()
        container.remove()  # Clean up the container
        return restart_date

    def run_container(self, image, container_name, env_vars):
        exists, _running = self.container_exists_and_running(container_name)
        if exists:
            # Handle the exited container. You can either restart it or remove and recreate it.
            self.manage_container(container_name=container_name, action="stop_remove")
        try:
            print(f"Running container '{container_name}' from image '{image}'...")
            container = self.client.containers.run(
                image=image,
                name=container_name,
                environment=env_vars,
                volumes=self.volume_binding,
                detach=True,
            )
            for log in container.logs(stream=True):
                print(log.decode("utf-8").strip())
            container.reload()  # Reload the container's state
            if container.status == "exited":
                exit_code = container.attrs['State']['ExitCode']
                if exit_code != 0:
                    print(f"Container '{container_name}' exited with error code {exit_code}.")
                    return False
            return True
        except (ContainerError, ImageNotFound, APIError) as e:
            print(f"An error occurred: {e}")
            return False
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return False

    def run_container_old(self, image, container_name, env_vars):
        exists, _running = self.container_exists_and_running(container_name)
        if exists:
            # Handle the exited container. You can either restart it or remove and recreate it.
            self.manage_container(container_name=container_name, action="stop_remove")
        print(f"Running container '{container_name}' from image '{image}'...")
        container = self.client.containers.run(
            image=image,
            name=container_name,
            environment=env_vars,
            volumes=self.volume_binding,
            detach=True,
        )
        for log in container.logs(stream=True):
            print(log.decode("utf-8").strip())

    def build_images(self, no_cache: bool = False):
        """
        Build Docker images for various components of the application.

        This method orchestrates the building of Docker images for the application's components,
        including base, gridmetetl, ncf2cbh, prms, and out2ncf. It allows for the option to build
        these images without using the cache to ensure that the latest versions of all dependencies
        are used.

        Parameters
        ----------
        no_cache : bool, optional
            A boolean flag indicating whether the Docker build process should ignore the cache.
        """
        print("Building Docker images...")
        components = [
            ("./pyonhm/base", "nhmusgs/base"),
            ("./pyonhm/gridmetetl", "nhmusgs/gridmetetl:0.30"),
            ("./pyonhm/ncf2cbh", "nhmusgs/ncf2cbh"),
            ("./pyonhm/prms", "nhmusgs/prms:5.2.1"),
            ("./pyonhm/out2ncf", "nhmusgs/out2ncf"),
            ("./pyonhm/cfsv2etl", "nhmusgs/cfsv2etl")
        ]

        for context_path, tag in components:
            success = self.build_image(context_path, tag, no_cache=no_cache)
            if not success:
                print(f"Stopping build process due to failure in building {tag}.")
                return  # Stop execution if a build fails


    def load_data(self, env_vars:dict):
        """
        Download necessary data using Docker containers.
        """
        print("Downloading data...")
        self.download_fabric_data(env_vars=env_vars)
        self.download_model_data(env_vars=env_vars)
        self.download_model_test_data(env_vars=env_vars)

    def print_env_vars(self, env_vars):
        """
        Print selected environment variables.
        """
        print_keys = [
            "RESTART_DATE",
            "START_DATE",
            "END_DATE",
            "SAVE_RESTART_DATE",
        ]
        for key, value in env_vars.items():
            if key in print_keys:
                print(f"{key}: {value}")

    def print_forecast_env_vars(self, env_vars: dict):
        """
        Print selected environment variables.
        """
        print_keys = [
            "FRCST_START_DATE",
            "FRCST_END_DATE",
            "FRCST_START_TIME",
            "FRCST_END_TIME"
        ]
        for key, value in env_vars.items():
            if key in print_keys:
                print(f"{key}: {value}")
    def list_date_folders(self, path: Path):
        """
        Generates a list of date folders from the specified path by listing directories matching the date pattern.

        Args:
            path (Path): The path to search for date folders.

        Returns:
            list: A list of date folders extracted from the specified path.
        """

        # Bash command to list directories matching the date pattern
        command = f"bash -c 'find {path} -maxdepth 1 -type d | grep -E \"/[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$\"'"
        container = self.client.containers.run(
            image="nhmusgs/base",
            command=command,
            volumes=self.volume_binding,
            # environment={"TERM": "dumb"},
            detach=True,
            tty=True,
        )
        output = container.logs().decode("utf-8").strip()
        container.remove()  # Clean up the container

        # return [line.split('/')[-1] for line in output.split('\n')]
        return [line.strip().split('/')[-1] for line in output.split('\n')]

    def forecast_run(
            self,
            env_vars: dict,
            method: str = "median"
    ):
        print("Running tasks for {method} forecast...")
        if method not in ["median", "ensemble"]:
            raise ValueError(f"Invalid method '{method}'. Mode must be 'median' or 'ensemble'.")
        median_path = Path(env_vars.get("CFSV2_NCF_IDIR")) / "ensemble_median"
        ensemble_path = Path(env_vars.get("CFSV2_NCF_IDIR")) / "ensembles/"
        print("Running forecast tasks...")
        # Get the most recent operational run restart date.  During an operational run, a restart file representing
        # The last day of operational simulation is placed in forecast/restart/ directory.
        forecast_restart_date = self.get_latest_restart_date(env_vars=env_vars, mode="forecast")
        print(f"Forecast restart date is {forecast_restart_date}")
        utils.env_update_forecast_dates(restart_date=forecast_restart_date, env_vars=env_vars)
        self.print_forecast_env_vars(env_vars)

        # Get a list of dates representing the available processed climate drivers
        if method == "median":
            forecast_input_dates = self.list_date_folders(median_path)
        elif method == "ensemble":
            forecast_input_dates = self.list_date_folders(ensemble_path)
        
        # Given the list of available forecast climate data is there data that represents the calculated 
        # forecast_start_date calculated above?
        state, forecast_run_date = utils.is_next_day_present(forecast_input_dates, forecast_restart_date)
        print(f"{method} forecast ready: {state}, forecast start date: {forecast_run_date}")

        if method == 'median':
            med_vars = utils.get_ncf2cbh_opvars(env_vars=env_vars, mode=method, ensemble=0)
            success = self.run_container(
                image="nhmusgs/ncf2cbh", container_name="ncf2cbh", env_vars=med_vars
            )
            if not success:
                print("Failed to run container 'ncf2cbh'. for median ensemble Exiting...")
                sys.exit(1)

            prms_env = utils.get_prms_run_env(env_vars=env_vars, restart_date=restart_date)
            success = self.run_container(
                image="nhmusgs/prms:5.2.1", container_name="prms", env_vars=prms_env
            )
            if not success:
                print("Failed to run container 'prms'. Exiting...")
                sys.exit(1)
    def operational_run(
        self,
        env_vars: dict,
        test: bool = False,
        num_days: int = 4,
    ):
        """
        Executes the operational run tasks for a given environment.

        This function performs a series of operational tasks, including determining the latest restart date,
        updating environment variables for testing (if specified), printing current environment variables,
        checking for gridMET updates, and initiating operational containers based on the updated environment
        variables and restart date.

        Parameters
        ----------
        env_vars : dict
            A dictionary containing environment variables as key-value pairs. These variables are essential
            for configuring the operational tasks, including paths, configurations, and operational parameters.
        test : bool, optional
            A flag indicating whether the run is a test run. If True, the environment variables are updated
            for testing purposes, affecting the operational period defined by `num_days`. Defaults to False.
        num_days : int, optional
            The number of days to consider for the operational run when in test mode. This parameter defines
            the temporal scope of the test, adjusting the relevant dates in `env_vars` accordingly. Only
            applicable if `test` is True.
        """
        print("Running operational tasks...")
        restart_date = self.get_latest_restart_date(env_vars=env_vars, mode="op")
        print(restart_date)

        if test:
            utils.env_update_dates_for_testing(
                restart_date=restart_date, env_vars=env_vars, num_days=num_days
            )
        else:
            status_list, date_list = utils.gridmet_updated()
            gm_status, end_date_str = utils.check_consistency(status_list, date_list)
            utils.env_update_dates(restart_date=restart_date, end_date=end_date_str, env_vars=env_vars)
            print(f"Gridmet updated relative to yesterday: {gm_status}")

        self.print_env_vars(env_vars)
        self.op_containers(env_vars, restart_date)

    def update_operational_restart(
            self,
            env_vars: dict,
    ):
        """
        Updates the operational restart for the Docker manager.

        Args:
            env_vars (dict): A dictionary containing environment variables.

        Returns:
            None
        """
        print("Running restart update...")
        restart_date = self.get_latest_restart_date(env_vars=env_vars, mode="op")
        print(f"The most recent restart date is {restart_date}")
        utils.env_update_dates_for_restart_update(restart_date=restart_date, env_vars=env_vars)
        print_keys = [
            "START_DATE",
            "END_DATE",
            "RESTART_DATE",
            "SAVE_RESTART_DATE",
        ]
        for key, value in env_vars.items():
            if key in print_keys:
                print(f"{key}: {value}")
        self.update_restart_containers(env_vars=env_vars, restart_date=restart_date)

    import sys

    def op_containers(self, env_vars, restart_date=None):
        """
        Run containers for data processing and analysis. Exits if a container fails to run.
        """
        success = self.run_container(
            image="nhmusgs/gridmetetl:0.30",
            container_name="gridmetetl",
            env_vars=env_vars,
        )
        if not success:
            print("Failed to run container 'gridmetetl'. Exiting...")
            sys.exit(1)  # Exit the program with an error code

        ncf2cbh_vars = utils.get_ncf2cbh_opvars(env_vars=env_vars, mode="op")
        success = self.run_container(
            image="nhmusgs/ncf2cbh", container_name="ncf2cbh", env_vars=ncf2cbh_vars
        )
        if not success:
            print("Failed to run container 'ncf2cbh'. Exiting...")
            sys.exit(1)

        prms_env = utils.get_prms_run_env(env_vars=env_vars, restart_date=restart_date)
        success = self.run_container(
            image="nhmusgs/prms:5.2.1", container_name="prms", env_vars=prms_env
        )
        if not success:
            print("Failed to run container 'prms'. Exiting...")
            sys.exit(1)

        success = self.run_container(
            image="nhmusgs/out2ncf",
            container_name="out2ncf",
            env_vars={"OUT_NCF_DIR": env_vars.get("OP_DIR")},
        )
        if not success:
            print("Failed to run container 'out2ncf'. Exiting...")
            sys.exit(1)

        prms_restart_env = utils.get_prms_restart_env(env_vars=env_vars)
        success = self.run_container(
            image="nhmusgs/prms:5.2.1", container_name="prms", env_vars=prms_restart_env
        )
        if not success:
            print("Failed to run container 'prms' with restart environment. Exiting...")
            sys.exit(1)

    def update_restart_containers(self, env_vars, restart_date=None):
        """Update restart file to current day.

        Convenience method that runs containers to forward the most recent restart file current with what would be required
        to run the operational model today.
        """
        self.run_container(
            image="nhmusgs/gridmetetl:0.30",
            container_name="gridmetetl",
            env_vars=env_vars,
        )
        self.run_container(
            image="nhmusgs/ncf2cbh", container_name="ncf2cbh", env_vars=env_vars
        )

        prms_restart_env = utils.get_prms_restart_env(env_vars=env_vars)
        self.run_container(
            image="nhmusgs/prms:5.2.1", container_name="prms", env_vars=prms_restart_env
        )

    def fetch_output(self, env_vars):
        client = docker.from_env()
        output_dir = env_vars.get("OUTPUT_DIR")
        frcst_dir = env_vars.get("FRCST_DIR")
        project_root = env_vars.get("PROJECT_ROOT")
        print(f'Output files will show up in the "{output_dir}" directory.')

        try:
            # Attempt to remove existing container if it exists
            existing_container = client.containers.get("volume_mounter")
            existing_container.remove(force=True)
            print("Existing container 'volume_mounter' found and removed.")
        except docker.errors.NotFound:
            print("No existing container 'volume_mounter' to remove.")

        # Build a minimal Docker image (if not already exists)
        try:
            dockerfile = "FROM alpine\nCMD\n"
            client.images.build(
                fileobj=BytesIO(dockerfile.encode("utf-8")),
                tag="nhmusgs/volume-mounter",
            )
        except docker.errors.BuildError as build_error:
            print(f"Error building Docker image: {build_error}")
            return

        # Create a container with a volume attached
        container = client.containers.create(
            name="volume_mounter",
            image="nhmusgs/volume-mounter",
            volumes={"nhm_nhm": {"bind": "/nhm", "mode": "rw"}},
        )

        # Define the paths to copy
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
                # Ensure the destination directory exists
                subprocess.run(["mkdir", "-p", dest_dir], check=True)
                # Copy each directory from the container to the host
                subprocess.run(
                    ["docker", "cp", f"volume_mounter:{src_path}", dest_dir], check=True
                )
            print("Directories copied successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error copying directories: {e}")
        finally:
            # Cleanup
            try:
                container.remove()
                print("Container 'volume_mounter' removed successfully.")
            except docker.errors.NotFound:
                print("Container already removed or not found.")
    def update_cfsv2(self, env_vars: dict, method: str):
        cfsv2_env = utils.get_cfsv2_env(env_vars=env_vars, method=method)
        self.run_container(
            image="nhmusgs/cfsv2etl",
            container_name="cfsv2_env",
            env_vars=cfsv2_env,
        )
@app.command(group=g_operational)
def run_operational(env_file: str, num_days: int=4, test:bool=False):
    """
    Runs the operational simulation using the DockerManager.

    Args:
        env_file: The path to the environment file.
        num_days: The number of days to run the simulation for. Defaults to 4.
        test: If True, runs the simulation in test mode. Defaults to False.

    Returns:
        None
    """
    docker_manager = DockerManager()
    dict_env_vars = utils.load_env_file(env_file)
    if docker_manager.client is not None:
        print("Docker client initialized successfully.")
    else:
        print("Failed to initialize Docker client.")
    
    docker_manager.operational_run(env_vars=dict_env_vars, test=test, num_days=num_days)

@app.command(group=g_sub_seasonal)
def run_sub_seasonal(env_file: str, method: str):
    """
    Runs the sub-seasonal operational simulation using the DockerManager.

    Args:
        env_file (str): The path to the environment file.
        method (str): One of ["median"]["ensemble"]  

    Returns:
        None
    """
    docker_manager = DockerManager()
    dict_env_vars = utils.load_env_file(env_file)
    if docker_manager.client is not None:
        print("Docker client initialized successfully.")
    else:
        print("Failed to initialize Docker client.")
    docker_manager.forecast_run(env_vars=dict_env_vars, method=method)
    print("TODO")

@app.command(group=g_sub_seasonal)
def run_update_cfsv2_data(env_file: str, method: str):
    """
    Runs the update of CFSv2 data using the specified method , either 'ensemble' or 'median'.

    Args:
        env_file (str): Path to the environment file.
        method (str): The method to use for updating data, either 'ensemble' or 'median'.

    Returns:
        None
    """
    docker_manager = DockerManager()
    dict_env_vars = utils.load_env_file(env_file)
    if docker_manager.client is not None:
        print("Docker client initialized successfully.")
    else:
        print("Failed to initialize Docker client.")
    if method not in ["ensemble", "median"]:
        print(f"Error: '{method}' is not a valid method. Please use 'ensemble' or 'median'.")
        sys.exit(1)  # Exit with error code 1 to indicate failure
    
    docker_manager.update_cfsv2(env_vars=dict_env_vars, method=method)

@app.command(group=g_seasonal)
def run_seasonal(env_file: str, num_days: int=4, test:bool=False):
    """
    Runs the seasonal operational simulation using the DockerManager.

    Args:
        env_file: The path to the environment file.
        num_days: The number of days to run the simulation for. Defaults to 4.
        test: If True, runs the simulation in test mode. Defaults to False.

    Returns:
        None
    """
    docker_manager = DockerManager()
    dict_env_vars = utils.load_env_file(env_file)
    if docker_manager.client is not None:
        print("Docker client initialized successfully.")
    else:
        print("Failed to initialize Docker client.")
    
    
    print("TODO")

@app.command(group=g_build_load)
def build_images(no_cache: bool=False):
    """
    Builds Docker images using the DockerManager.

    Args:
        no_cache: If True, builds the images without using cache. Defaults to False.

    Returns:
        None
    """
    docker_manager = DockerManager()
    if docker_manager.client is not None:
        print("Docker client initialized successfully.")
    else:
        print("Failed to initialize Docker client.")
    
    docker_manager.build_images(no_cache=no_cache)

@app.command(group=g_build_load)
def update_operational_restart(env_file: str):
    """
    Updates the operational restart using the provided environment file.

    Args:
        env_file (str): Path to the environment file.

    Returns:
        None
    """
    docker_manager=DockerManager()
    dict_env_vars = utils.load_env_file(env_file)
    if docker_manager.client is not None:
        print("Docker client initialized successfully.")
    else:
        print("Failed to initialize Docker client.")
    docker_manager.update_operational_restart(env_vars=dict_env_vars)

@app.command(group=g_build_load)
def load_data(env_file: str):
    """
    Loads data using the DockerManager.

    Args:
        env_file: The path to the environment file.

    Returns:
        None
    """
    docker_manager = DockerManager()
    dict_env_vars = utils.load_env_file(env_file)
    if docker_manager.client is not None:
        print("Docker client initialized successfully.")
    else:
        print("Failed to initialize Docker client.")
    docker_manager.load_data(env_vars=dict_env_vars)

@app.command(group=g_operational)
def fetch_op_results(env_file: str):
    """
    Fetches operational results using the DockerManager.

    Args:
        env_file: The path to the environment file.

    Returns:
        None
    """
    docker_manager = DockerManager()
    dict_env_vars = utils.load_env_file(env_file)
    if docker_manager.client is not None:
        print("Docker client initialized successfully.")
    else:
        print("Failed to initialize Docker client.")
    docker_manager.fetch_output(env_vars=dict_env_vars)

def main():
    try:
        app()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()