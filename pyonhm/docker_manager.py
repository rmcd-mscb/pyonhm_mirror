from cyclopts import App, Group, Parameter
import argparse
from io import BytesIO
import docker
import sys
import subprocess
from . import utils
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

    def build_image(self, context_path, tag, no_cache=False):
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
        except Exception as e:
            print(f"Failed to build Docker image: {e}")

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
                chown -R nhm /nhm/NHM_PRMS_CONUS_GF_1_1 ;
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
                chown -R nhm /nhm/NHM_PRMS_UC_GF_1_1 ;
                chmod -R 766 /nhm/NHM_PRMS_UC_GF_1_1
            """
            self.download_data(
                image="nhmusgs/base",
                container_name="base",
                working_dir="/nhm",
                download_path="/nhm/NHM_PRMS_UC_GF_1_1",
                download_commands=prms_download_commands,
            )

    def get_latest_restart_date(self, env_vars):
        """
        Finds and returns the date of the latest restart file in a specified directory within a Docker container.

        This function runs a Docker container to execute a shell command that lists all `.restart` files in a
        specific directory, sorts them, and extracts the date from the filename of the most recent file. It assumes
        that the filenames are structured such that the date can be isolated by removing the file extension.

        Parameters:
        - self: The instance of the class containing this method.
        - env_vars (dict): A dictionary of environment variables where "PROJECT_ROOT" specifies the root directory
        of the project within the container's file system.

        Returns:
        - str: The date of the latest restart file, extracted from its filename.

        Note:
        This function requires that the Docker client is initialized in the class and that the necessary volume
        bindings are set up in `self.volume_binding` to allow access to the project's files from within the container.
        """
        command = "bash -c 'ls -1 *.restart | sort | tail -1 | cut -f1 -d .'"
        project_root = env_vars.get("PROJECT_ROOT")
        container = self.client.containers.run(
            image="nhmusgs/base",
            command=command,
            volumes=self.volume_binding,
            working_dir=f"{project_root}/daily/restart",
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

    def build_images(self, no_cache:bool = False):
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
            When set to True, the build process does not use the cache, leading to a fresh
            download of all layers in the Dockerfile. This can be useful for ensuring that the
            latest versions of dependencies are included in the built image. The default is False,
            which allows the build process to use the cache for efficiency.
        """
        print("Building Docker images...")
        self.build_image("./pyonhm/base", "nhmusgs/base", no_cache=no_cache)
        self.build_image("./pyonhm/gridmetetl", "nhmusgs/gridmetetl:0.30", no_cache=no_cache)
        self.build_image("./pyonhm/ncf2cbh", "nhmusgs/ncf2cbh", no_cache=no_cache)
        self.build_image("./pyonhm/prms", "nhmusgs/prms:5.2.1", no_cache=no_cache)
        self.build_image("./pyonhm/out2ncf", "nhmusgs/out2ncf", no_cache=no_cache)

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
            "START_DATE",
            "END_DATE",
            "RESTART_DATE",
            "SAVE_RESTART_DATE",
            "FRCST_END_DATE",
            "F_END_TIME",
        ]
        for key, value in env_vars.items():
            if key in print_keys:
                print(f"{key}: {value}")

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
        env_vars : str
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
        restart_date = self.get_latest_restart_date(env_vars=env_vars)
        print(restart_date)

        if test:
            utils.env_update_dates_for_testing(
                restart_date=restart_date, env_vars=env_vars, num_days=num_days
            )
        else:
            utils.env_update_dates(restart_date=restart_date, env_vars=env_vars)

        self.print_env_vars(env_vars)

        gm_update = utils.gridmet_updated()
        print(f"Gridmet updated: {gm_update}")

        self.op_containers(env_vars, restart_date)

    def op_containers(self, env_vars, restart_date=None):
        """
        Run containers for data processing and analysis.
        """
        self.run_container(
            image="nhmusgs/gridmetetl:0.30",
            container_name="gridmetetl",
            env_vars=env_vars,
        )
        self.run_container(
            image="nhmusgs/ncf2cbh", container_name="ncf2cbh", env_vars=env_vars
        )

        prms_env = utils.get_prms_run_env(env_vars=env_vars, restart_date=restart_date)
        self.run_container(
            image="nhmusgs/prms:5.2.1", container_name="prms", env_vars=prms_env
        )

        self.run_container(
            image="nhmusgs/out2ncf",
            container_name="out2ncf",
            env_vars={"OUT_NCF_DIR": env_vars.get("OP_DIR")},
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
def run_sub_seasonal(env_file: str, num_days: int=4, test:bool=False):
    """
    Runs the sub-seasonal operational simulation using the DockerManager.

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
    app()

if __name__ == "__main__":
    main()