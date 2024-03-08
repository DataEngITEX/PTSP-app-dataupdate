import pandas as pd
import sqlite3
import requests
import base64
import os
import psutil
import time
from datetime import date
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

# Defining file paths for downloads
raw_url = config_dir['RAW_DB']
leg_url = config_dir['LEG_DB']
sha_url = config_dir['SHA_DB']
rca_loc = config_dir['PROCESSED_RCA_LOC']
inputrca_loc = config_dir['RAW_RCA_LOC']
#legacy_loc = config_dir['LEGACY_DB']

def download_current_database(url):
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
            return local_db_path
        else:
            raise Exception(f"Failed to download the database, response code: error{response.status_code}")
    except Exception as e:
        print(f"Failed to download the database, response code: error{e}")

def download_legacy_database(url):
    # download legacy data
    response = requests.get(url)
    try:
        if response.status_code == 200:
            # Define a local file path to save the downloaded database
            global legacy_db_path
            legacy_db_path = config_dir['LEGACY_DB']
            
            # Save the content of the response to the local file
            with open(legacy_db_path, 'wb') as f:
                f.write(response.content)
            print('Legacy database Downloaded')
            return legacy_db_path
        else:
            raise Exception(f"Failed to download the database, response code: error{response.status_code}")
    except Exception as de:
        print(f"Failed to download the database, response code: error{de}")



def create_current_dataframes():

    local_db_path = download_current_database(raw_url)
    # Connect to your SQLite database
    c_conn = sqlite3.connect(local_db_path)
    query1 = f"SELECT * FROM RCA_table"
    current_df = pd.read_sql_query(query1, c_conn)
    # Close the connection
    c_conn.close()

    return current_df

def create_legacy_dataframes():
    legacy_db_path = download_legacy_database(leg_url)
    # Connect to your SQLite database
    l_conn = sqlite3.connect(legacy_db_path)
    query2 = f"SELECT * FROM RCA_table"
    legacy_df = pd.read_sql_query(query2, l_conn)
    # Close the connection
    l_conn.close()

    return legacy_df

def update_legacy():
    leg_df = create_legacy_dataframes()
    cur_df = create_current_dataframes()

    today_date = date.today() 
    for tid in cur_df['Terminal_ID']:
        if tid in leg_df['Terminal_ID'].values:
            # Update the last transaction date for existing Terminal IDs
            leg_df.loc[leg_df['Terminal_ID'] == tid, 'LAST_TRANSACTION_DATE'] = today_date
        else:
            # Append Terminal IDs not present in leg_df
            leg_df = leg_df.append({'Terminal_ID': tid, 'LAST_TRANSACTION_DATE': today_date}, ignore_index=True)
    
    conn = sqlite3.connect(legacy_db_path)
    # Replace the old database with the new file
    try:
        leg_df.to_sql('RCA_table', conn, if_exists='replace', index=False)
        print("Legacy database updated")
    except Exception as e:
        print(f"An error occurred updating the database: {e}")
