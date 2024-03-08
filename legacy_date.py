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
leg_sha = config_dir['LEG_SHA']
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
    print('Current df created')
    return current_df

def create_legacy_dataframes():
    legacy_db_path = download_legacy_database(leg_url)
    # Connect to your SQLite database
    l_conn = sqlite3.connect(legacy_db_path)
    query2 = f"SELECT * FROM RCA_table"
    legacy_df = pd.read_sql_query(query2, l_conn)
    # Close the connection
    l_conn.close()
    print('legacy df created')
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
            # Create a new DataFrame with the new row
            new_row = pd.DataFrame({'Terminal_ID': [tid], 'LAST_TRANSACTION_DATE': [today_date]})
            # Concatenate the new DataFrame with the existing one
            leg_df = pd.concat([leg_df, new_row], ignore_index=True)
    
    conn = sqlite3.connect(legacy_db_path)
    # Replace the old database with the new file
    try:
        print('Updating RCA TABLE')
        leg_df.to_sql('RCA_table', conn, if_exists='replace', index=False)
        print("Legacy database updated")
    except Exception as e:
        raise e 

def load_to_github():

    # Connect to the githubAPI with the access tokens and usernames
    username = config_git['USERNAME']
    repository = config_git['REPOSITORY']
    file_path = config_git['LEG_PATH']
    access_token = config_git['TOKEN']
    
    # Enter the location of the new db file
    new_db_filepath = legacy_db_path
    
    # Read the binary data from the new SQLite database file
    with open(new_db_filepath, 'rb') as file:
        new_content = file.read()

    # Encode the binary content as Base64
    content_base64 = base64.b64encode(new_content).decode('utf-8')

    # Create the URL for the API endpoint
    url = f'https://api.github.com/repos/{username}/{repository}/contents/{file_path}'

    # Create the request headers with the authorization token
    headers = {
        'Authorization': f'token {access_token}'
    }

    def get_sha():
        response = requests.get(leg_sha, headers=headers)

        if response.status_code == 200:
            try:
                # Try to parse JSON data
                file_info = response.json()
                sha = file_info.get("sha")
                print('sha obtained')
                return sha

            except Exception as e:
                    print(f"Error parsing JSON response: {e}")
        else:
            print(f"Failed to retrieve file info: {response.status_code} - {response.text}")

        
    sha = get_sha()

    # Create the request payload with the new content as a base64-encoded string
    data = {
        'message': 'Update database file',
        'content': content_base64,
        'sha': sha
    }

    # Send a PUT request to update the file
    response = requests.put(url, headers=headers, json=data)

    if response.status_code == 200:
        print('Database file updated successfully.')
    else:
        print('Failed to update database file:', response.text, response.status_code)


update_legacy()
load_to_github()