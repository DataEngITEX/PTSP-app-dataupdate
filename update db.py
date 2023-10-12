import pandas as pd
import sqlite3
import requests
import base64
import os


raw_url = 'https://github.com/daniel-DE-ITEX/PTSP-app-dataupdate/raw/master/data/testDB.db'
sha_url = 'https://api.github.com/repos/daniel-DE-ITEX/PTSP-app-dataupdate/contents/data/testDB.db'
rca_loc = 'C:/Users/daniel.opanubi/OneDrive - ITEX Integrated Services/Desktop/Projects/PTSP-app-dataupdate/rca_file/'

# Define a function to download the database file and return the local file path
def download_database(url):
    response = requests.get(url)
    if response.status_code == 200:
        # Define a local file path to save the downloaded database
        global local_db_path
        local_db_path = 'C:/Users/daniel.opanubi/OneDrive - ITEX Integrated Services/Desktop/Projects/PTSP-app-dataupdate/download.db'
        
        # Save the content of the response to the local file
        with open(local_db_path, 'wb') as f:
            f.write(response.content)
        
        print('DB Downloaded')
        return local_db_path
    else:
        raise Exception("Failed to download the database.")
    
# Define a function to connect and update the database file in local
def connect_and_update_database():

    loc_db_path = download_database(raw_url)
    conn = sqlite3.connect(loc_db_path)

    for xfile in os.listdir(rca_loc):
        if len(os.listdir(rca_loc)) > 0:
            excel_file_loc = str(rca_loc) + str(xfile)

            df = pd.read_excel(excel_file_loc)
            df = df.astype(str)
            df['LAST_TRANSACTION_DATE'] = df['LAST_TRANSACTION_DATE'].apply(lambda x: x if pd.to_datetime(x, errors='coerce') is not pd.NaT else 'Not available')
            df['LAST_TRANSACTION_DATE'] = pd.to_datetime(df['LAST_TRANSACTION_DATE'], errors='coerce')
            df['LAST_TRANSACTION_DATE'] = df['LAST_TRANSACTION_DATE'].dt.date

    cursor = conn.cursor()
    query1 = """
            CREATE TABLE RCA_table1 (
                Terminal_ID TEXT, 
                Merchant_Name TEXT, 
                STATUS TEXT,
                CONNECTED TEXT, 
                LAST_TRANSACTION_DATE TEXT
            );
            """
    query2 = "DROP TABLE RCA_table;"
    
    query3 = "ALTER TABLE RCA_table1 RENAME TO RCA_table;"
    cursor.execute(query1)
    cursor.execute(query2)
    cursor.execute(query3)


    # Replace the old database with the new file
    try:
        df.to_sql('RCA_table', conn, if_exists='replace', index=False)
        print("Table modified")
    except Exception as e:
        print(f"An error occurred: {e}")
        
def load_to_github():

    print('Loading to github')
    # Connect to the githubAPI with the access tokens and usernames
    username = "daniel-DE-ITEX"
    repository = "PTSP-app-dataupdate"
    file_path = "data/testDB.db"

    access_token = "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"
    
    # Enter the location of the new db file
    new_db_filepath = local_db_path
    
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
        response = requests.get(sha_url, headers=headers)

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

def clean_data():

    try:
        os.remove(local_db_path)
        print(f"File '{local_db_path}' deleted successfully.")
    except FileNotFoundError:
        print(f"File '{local_db_path}' not found.")
    except PermissionError:
        print(f"Permission denied. Unable to delete file '{local_db_path}'.")
    except Exception as e:
        print(f"An error occurred while deleting the file: {e}")

def main():
    download_database(raw_url)
    connect_and_update_database()
    load_to_github()
    clean_data()

if __name__ == '__main__':
    main()