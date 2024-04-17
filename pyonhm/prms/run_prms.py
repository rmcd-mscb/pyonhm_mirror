#!/usr/bin/env python3

import os
import sys
import subprocess

def check_env_variable(var_name):
    """Check if an environment variable is set and print its status."""
    value = os.getenv(var_name)
    if not value:
        print(f"Error: {var_name} environment variable is not set.")
        sys.exit(1)
    else:
        print(f"{var_name} is set to '{value}'.")
    return value

def change_directory(target_dir):
    """Change the working directory to the target directory."""
    try:
        os.chdir(target_dir)
        print(f"Changed directory to {target_dir}.")
    except Exception as e:
        print(f"Failed to change directory to {target_dir}: {e}")
        sys.exit(1)

def main():
    op_dir = check_env_variable("OP_DIR")
    frcst_dir = check_env_variable("FRCST_DIR")
    prms_run_type = check_env_variable("PRMS_RUN_TYPE")
    nhm_source_dir = check_env_variable("NHM_SOURCE_DIR")
    prms_control_file = check_env_variable("PRMS_CONTROL_FILE")

    # Determine the directory to change to based on PRMS_RUN_TYPE
    if prms_run_type == '0':
        change_directory(op_dir)
        # Construct the command to run the PRMS model
        command = [
            os.path.join(nhm_source_dir, "bin", "prms"),
            "-set", "start_time", os.getenv("PRMS_START_TIME"),
            "-set", "end_time", os.getenv("PRMS_END_TIME"),
            "-set", "init_vars_from_file", os.getenv("PRMS_INIT_VARS_FROM_FILE"),
            "-set", "var_init_file", os.getenv("PRMS_VAR_INIT_FILE"),
            "-set", "save_vars_to_file", os.getenv("PRMS_SAVE_VARS_TO_FILE"),
            "-set", "var_save_file", os.getenv("PRMS_VAR_SAVE_FILE"),
            "-C", prms_control_file
        ]
    elif prms_run_type == '1':
        change_directory(frcst_dir)
        command = [
            os.path.join(nhm_source_dir, "bin", "prms"),
            "-set", "start_time", os.getenv("PRMS_START_TIME"),
            "-set", "end_time", os.getenv("PRMS_END_TIME"),
            "-set", "init_vars_from_file", os.getenv("PRMS_INIT_VARS_FROM_FILE"),
            "-set", "var_init_file", os.getenv("PRMS_VAR_INIT_FILE"),
            "-set", "save_vars_to_file", os.getenv("PRMS_SAVE_VARS_TO_FILE"),
            "-set", "humidity_day", f"{os.getenv('PRMS_INPUT_DIR')}/humidity.cbh",
            "-set", "prcp_day", f"{os.getenv('PRMS_INPUT_DIR')}/prcp.cbh",
            "-set", "tmax_day", f"{os.getenv('PRMS_INPUT_DIR')}/tmax.cbh",
            "-set", "tmin_day", f"{os.getenv('PRMS_INPUT_DIR')}/tmin.cbh",
            "-set", "nhruOutBaseFileName", f"{os.getenv('PRMS_OUTPUT_DIR')}/"
            "-set", "nsegmentOutBaseFileName", f"{os.getenv('PRMS_OUTPUT_DIR')}/"
            "-C", prms_control_file
        ]

    # Execute the PRMS command
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to execute PRMS: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
