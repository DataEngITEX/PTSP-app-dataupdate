import pandas as pd
import sqlite3
import requests
import base64
import os
import psutil
import time
from datetime import datetime
import json

# Adding the configuration file to boost credential security
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = '\\'.join([ROOT_DIR, 'credentials.json'])

# read json file
with open(config_path) as config_file:
    config = json.load(config_file)
    config_dir = config['directories']
    config_sp = config['sharepoint']
    config_ftp = config['ftp']
    config_git = config['github']
    config_ldd = config['LEGACY_DB']

# Defining file paths for downloads
raw_url = config_dir['RAW_DB']
sha_url = config_dir['SHA_DB']
rca_loc = config_dir['PROCESSED_RCA_LOC']
inputrca_loc = config_dir['RAW_RCA_LOC']

def download_databases(url,durl):
    response = requests.get(url)
    try:
        if response.status_code == 200:
            # Define a local file path to save the downloaded database
            global local_db_path
            local_db_path = config_dir['LOCAL_DB']
            
            # Save the content of the response to the local file
            with open(local_db_path, 'wb') as f:
                f.write(response.content)           
            print('Database Downloaded')
        else:
            raise Exception(f"Failed to download the database, response code: error{response.status_code}")
    except Exception as e:
        print(f"Failed to download the database, response code: error{e}")

    # download legacy data
    response = requests.get(durl)
    try:
        if response.status_code == 200:
            # Define a local file path to save the downloaded database
            global legacy_db_path
            legacy_db_path = config_ldd['LEGACY_DB']
            
            # Save the content of the response to the local file
            with open(legacy_db_path, 'wb') as f:
                f.write(response.content)
            print('Legacy database Downloaded')
            
        else:
            raise Exception(f"Failed to download the database, response code: error{response.status_code}")
    except Exception as de:
        print(f"Failed to download the database, response code: error{de}")

    return local_db_path, legacy_db_path


def create_current_dataframes():
    # Connect to your SQLite database
    l_conn = sqlite3.connect(legacy_db_path)
    query1 = f"SELECT * FROM RCA_table"
    legacy_df = pd.read_sql_query(query1, l_conn)
    # Close the connection
    l_conn.close()

    return legacy_df

def create_legacy_dataframes():
    # Connect to your SQLite database
    d_conn = sqlite3.connect(local_db_path)
    query2 = f"SELECT * FROM RCA_table"
    current_df = pd.read_sql_query(query2, d_conn)
    # Close the connection
    d_conn.close()

    return current_df

def update_legacy():

    leg_df = create_legacy_dataframes()
    cur_df = create_current_dataframes()

    for tid in cur_df['Terminal_ID']:
        if tid in leg_df['Terminal_ID']:
            leg_df['LAST_TRANSACTION_DATE'] = datetime.date.today()