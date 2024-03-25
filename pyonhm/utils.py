import pprint
import urllib3
import xmltodict
from datetime import datetime, timedelta
from pprint import pprint
import pytz

def adjust_date_str(date_str, days):
    """
    Adjusts a date by a certain number of days.

    Parameters:
    - date_str: The date in string format (%Y-%m-%d).
    - days: The number of days to adjust the date by. Can be negative.

    Returns:
    - The adjusted date as a string in %Y-%m-%d format.
    """
    date = datetime.strptime(date_str, '%Y-%m-%d')
    adjusted_date = date + timedelta(days=days)
    return adjusted_date.strftime('%Y-%m-%d')

def get_yesterday_mst():
    # Define MST timezone
    mst = pytz.timezone('MST7MDT')
    
    # Get current time in UTC, then convert to MST
    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    now_mst = now_utc.astimezone(mst)
    
    # Subtract one day to get 'yesterday' and return only the date part
    yesterday_mst = (now_mst - timedelta(days=1)).date()
    return yesterday_mst

# Function to get yesterday's date
def get_yesterday():
    return (datetime.now() - timedelta(days=1)).date()

# Function to add or subtract days from a given date
def adjust_date(date, days):
    return (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=days)).date()

def env_update_dates_for_restart_update(restart_date, env_vars):
    env_vars["RESTART_DATE"] = restart_date
    yesterday = get_yesterday_mst().strftime('%Y-%m-%d')
    # Directly setting START_DATE to restart_date + 1 day
    start_date = adjust_date_str(restart_date, 1)
    env_vars['START_DATE'] = start_date
    env_vars['SAVE_RESTART_DATE'] = adjust_date(yesterday, -59).strftime('%Y-%m-%d')

    # Setting END_DATE to SAVE_RESTART_DATE
    env_vars['END_DATE'] = env_vars['SAVE_RESTART_DATE']

    # setting for updating new restart time
    env_vars['SAVE_RESTART_TIME'] = datetime.strptime(env_vars['END_DATE'], '%Y-%m-%d').strftime('%Y,%m,%d,00,00,00')

    # Save restart date in env vars
    env_vars['NEW_RESTART_DATE'] = restart_date

def env_update_dates_for_testing(restart_date, env_vars, num_days):
    yesterday = get_yesterday_mst().strftime('%Y-%m-%d')

    # Directly setting START_DATE to restart_date + 1 day
    start_date = adjust_date_str(restart_date, 1)
    env_vars['START_DATE'] = start_date

    # Setting END_DATE to START_DATE + num_days
    end_date = adjust_date_str(start_date, num_days)
    env_vars['END_DATE'] = end_date

    # Setting SAVE_RESTART_DATE if it's not set
    env_vars['SAVE_RESTART_DATE'] = end_date

    # Setting FRCST_END_DATE if it's not set
    if 'FRCST_END_DATE' not in env_vars or not env_vars['FRCST_END_DATE']:
        env_vars['FRCST_END_DATE'] = adjust_date(yesterday, 29).strftime('%Y-%m-%d')

    # Formatting FRCST_END_DATE for F_END_TIME
    env_vars['F_END_TIME'] = datetime.strptime(env_vars['FRCST_END_DATE'], '%Y-%m-%d').strftime('%Y,%m,%d,00,00,00')

    # setting for updating new restart time
    env_vars['SAVE_RESTART_TIME'] = datetime.strptime(env_vars['END_DATE'], '%Y-%m-%d').strftime('%Y,%m,%d,00,00,00')

    # Save restart date in env vars
    env_vars['NEW_RESTART_DATE'] = restart_date

def env_update_dates(restart_date, env_vars):
    yesterday = get_yesterday_mst().strftime('%Y-%m-%d')

    # Setting END_DATE if it's not set
    if 'END_DATE' not in env_vars or not env_vars['END_DATE']:
        env_vars['END_DATE'] = yesterday

    # Setting START_DATE if it's not set
    if 'START_DATE' not in env_vars or not env_vars['START_DATE']:
        restart_date = env_vars.get('RESTART_DATE', yesterday)  # Use yesterday if RESTART_DATE is not set
        env_vars['START_DATE'] = adjust_date(restart_date, 1).strftime('%Y-%m-%d')

    # Setting SAVE_RESTART_DATE if it's not set
    if 'SAVE_RESTART_DATE' not in env_vars or not env_vars['SAVE_RESTART_DATE']:
        env_vars['SAVE_RESTART_DATE'] = adjust_date(yesterday, -59).strftime('%Y-%m-%d')

    # Setting FRCST_END_DATE if it's not set
    if 'FRCST_END_DATE' not in env_vars or not env_vars['FRCST_END_DATE']:
        env_vars['FRCST_END_DATE'] = adjust_date(yesterday, 29).strftime('%Y-%m-%d')

    # Formatting FRCST_END_DATE for F_END_TIME
    env_vars['F_END_TIME'] = datetime.strptime(env_vars['FRCST_END_DATE'], '%Y-%m-%d').strftime('%Y,%m,%d,00,00,00')

    # setting for updating new restart time
    if 'SAVE_RESTART_DATE' not in env_vars or not env_vars['SAVE_RESTART_DATE']:
        env_vars['SAVE_RESTART_TIME'] = adjust_date(yesterday, -59).strftime('%Y,%m,%d,00,00,00')
    else:
        env_vars['SAVE_RESTART_TIME'] = datetime.strptime(env_vars['END_DATE'], '%Y-%m-%d').strftime('%Y,%m,%d,00,00,00')

    # Save restart date in env vars
    env_vars['NEW_RESTART_DATE'] = restart_date

def get_prms_run_env(env_vars, restart_date):
    # Convert START_DATE string to datetime object
    start_date = datetime.strptime(env_vars.get("START_DATE"), '%Y-%m-%d')
    # Format START_DATE as needed
    start_time = start_date.strftime('%Y,%m,%d,00,00,00')
    env_vars["START_TIME"] = start_time

    # Convert END_DATE string to datetime object
    end_date = datetime.strptime(env_vars.get("END_DATE"), '%Y-%m-%d')
    # Format END_DATE as needed
    end_time = end_date.strftime('%Y,%m,%d,00,00,00')
    end_date_string = env_vars.get("END_DATE")
    project_root = env_vars.get("PROJECT_ROOT")
    op_dir = env_vars.get("OP_DIR")
    frcst_dir = env_vars.get("FRCST_DIR")

    prms_env = {
        "OP_DIR": op_dir,
        "FRCST_DIR": op_dir,
        "PRMS_START_TIME": start_time,
        "PRMS_END_TIME": end_time,
        "PRMS_INIT_VARS_FROM_FILE": "1",
        "PRMS_RESTART_DATE": restart_date,
        "PRMS_VAR_INIT_FILE": f"{project_root}/daily/restart/{restart_date}.restart",
        "PRMS_SAVE_VARS_TO_FILE": "1",
        "PRMS_VAR_SAVE_FILE": f"{project_root}/forecast/restart/{end_date_string}.restart",
        "PRMS_CONTROL_FILE": env_vars.get("OP_PRMS_CONTROL_FILE"),
        "PRMS_RUN_TYPE": 0
    }
    print("PRMS RUN ENV: \n")
    pprint(prms_env)
    return prms_env

def get_prms_restart_env(env_vars):
    # Convert START_DATE string to datetime object
    start_date = datetime.strptime(env_vars.get("START_DATE"), '%Y-%m-%d')
    # Format START_DATE as needed
    start_time = start_date.strftime('%Y,%m,%d,00,00,00')
    env_vars["START_TIME"] = start_time
    project_root = env_vars.get("PROJECT_ROOT")
    op_dir = env_vars.get("OP_DIR")
    frcst_dir = env_vars.get("FRCST_DIR")
    prms_restart_env = {
        "OP_DIR": op_dir,
        "FRCST_DIR": op_dir,
        "PRMS_START_TIME": env_vars.get("START_TIME"),
        "PRMS_END_TIME": env_vars.get("SAVE_RESTART_TIME"),
        "PRMS_INIT_VARS_FROM_FILE": 1,
        "PRMS_VAR_INIT_FILE": f"{project_root}/daily/restart/{env_vars.get('NEW_RESTART_DATE')}.restart",
        "PRMS_SAVE_VARS_TO_FILE": 1,
        "PRMS_VAR_SAVE_FILE": f"{project_root}/daily/restart/{env_vars.get('SAVE_RESTART_DATE')}.restart",
        "PRMS_CONTROL_FILE": env_vars.get("OP_PRMS_CONTROL_FILE"),
        "PRMS_RUN_TYPE": 0
    }
    print("PRMS RESTART RUN ENV: \n")
    pprint(prms_restart_env)
    return prms_restart_env

def load_env_file(filename):
    env_dict = {}
    with open(filename) as file:
        for line in file:
            if line.startswith('#') or not line.strip():
                continue
            key, value = line.strip().split('=', 1)
            env_dict[key] = value
    return env_dict

def _getxml(url):
    http = urllib3.PoolManager()
    try:
        response = http.request('GET', url)
        data = xmltodict.parse(response.data)
        return data
    except Exception as e:  # Better error handling
        print(f"Error: {e}")
        return None
    
def gridmet_updated() -> bool:
    serverURL = 'http://thredds.northwestknowledge.net:8080/thredds/ncss/grid'

    data_packets = ['agg_met_tmmn_1979_CurrentYear_CONUS.nc', 'agg_met_pr_1979_CurrentYear_CONUS.nc',
                    'agg_met_rmin_1979_CurrentYear_CONUS.nc', 'agg_met_rmax_1979_CurrentYear_CONUS.nc',
                    'agg_met_vs_1979_CurrentYear_CONUS.nc']
    urlsuffix = 'dataset.xml'

    # Timezone-aware datetime objects
    tz = pytz.timezone('America/Denver')  # Replace with your timezone
    nowutc = datetime.now(pytz.utc)
    now = nowutc.astimezone(tz)
    yesterday = (now - timedelta(days=1)).date()

    for data in data_packets:
        masterURL = f"{serverURL}/{data}/{urlsuffix}"
        xml_data = _getxml(masterURL)
        if xml_data:
            datadef = xml_data['gridDataset']['TimeSpan']['end']
            gm_date = datetime.strptime(datadef[:10], '%Y-%m-%d').date()
            if gm_date != yesterday:
                print(f'Gridmet data {data} is not available:\nprocess exiting')
                return False
            else:
                print(f'Gridmet data {data} is available')
                return True
        else:
            print(f"Failed to fetch or parse data for {data}")
            return False