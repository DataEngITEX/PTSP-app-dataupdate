import os
import pandas as pd
import sqlite3
import datetime
import json
import requests

db_url = 'https://github.com/daniel-DE-ITEX/PTSP-app-dataupdate/blob/master/data/testDB.db'
excel_file_loc = "C:/Users/daniel.opanubi/Downloads/ITEX RCA (30-09-23).xlsx"

# Define a function to download the database file and return the local file path
def download_database(url):
    response = requests.get(url)
    if response.status_code == 200:
        # Define a local file path to save the downloaded database
        global local_db_path
        local_db_path = 'testDB.db'
        
        # Save the content of the response to the local file
        with open(local_db_path, 'wb') as f:
            f.write(response.content)
        
        return local_db_path
    else:
        raise Exception("Failed to download the database.")
    
# Define a function to connect and update the database file in local
def connect_and_update_database():
    local_db_path = download_database(db_url)
    df = pd.read_excel(excel_file_loc)
    df.to_sql('RCA_table', conn, if_exists='replace', index=False)

    
    conn = sqlite3.connect(local_db_path)
    cursor = conn.cursor()
    query1 = """

    """
    query2 = """
    
    """
    cursor.execute(query1)
    cursor.execute(query2)

def load_to_github():

    # Connect to the githubAPI with the access tokens and usernames
    username = "daniel_de_ITEX"
    repository = "PTSP-app-dataupdate"
    file_path = "data/newDB.db"

    access_token = "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"
    # Enter the location of the new db file
    new_db_filepath = local_db_path
    
    # Read the binary data from the new SQLite database file
    with open(new_db_filepath, 'rb') as file:
        new_content = file.read()

    # Create the URL for the API endpoint
    url = f'https://api.github.com/repos/{username}/{repository}/contents/{file_path}'

    # Create the request headers with the authorization token
    headers = {
        'Authorization': f'token {access_token}'
    }

    # Create the request payload with the new content as a base64-encoded string
    data = {
        'message': 'Update database file',
        'content': new_content.decode('latin1'),
        'sha': "d30b7bf164ffb1012b5d3fba2ee7a7344f2abbf6"
    }

    # Send a PUT request to update the file
    response = requests.put(url, headers=headers, json=data)

    if response.status_code == 200:
        print('Database file updated successfully.')
    else:
        print('Failed to update database file:', response.text)