import pandas as pd
import sqlite3
import requests
import base64
import os
import psutil
import time
from pymongo import MongoClient
import urllib.parse
import json
from datetime import datetime, timedelta, date
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.user_credential import UserCredential


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
    config_mongo = config['mongodb']

# Defining file paths for downloads
raw_url = config_dir['RAW_DB']
leg_url = config_dir['LEG_DB']
sha_url = config_dir['SHA_DB']
leg_sha = config_dir['LEG_SHA']
rca_loc = config_dir['PROCESSED_RCA_LOC']
inputrca_loc = config_dir['RAW_RCA_LOC']
# SharePoint Details
sharepoint_site_url = config_sp['SITE']
sharepoint_username = config_sp['USERNAME']
sharepoint_password = config_sp['PASSWORD']


def retrieve_rca_from_sharepoint():
    try:
        # Create a SharePoint context
        ctx_auth = UserCredential(sharepoint_username, sharepoint_password)
        ctx = ClientContext(sharepoint_site_url).with_credentials(ctx_auth)

        # Specify the SharePoint library and folder
        library_name = "Shared Documents"
        folder_relative_url = "/sites/NIBSS-ITEXrepo/Shared Documents/RCA_input"

        # Get the folder by its server relative URL
        folder = ctx.web.get_folder_by_server_relative_url(folder_relative_url)
        ctx.load(folder)
        ctx.execute_query()

        # Check if the folder is empty
        files = folder.files
        ctx.load(files)
        ctx.execute_query()
        
        if not files or len(files) == 0:
            print("The 'RCA_input' folder is empty. No files to download.")
            return

        # Create a local directory if it doesn't exist
        if not os.path.exists(inputrca_loc):
            os.makedirs(inputrca_loc)

        # Iterate through files and download them
        for file in files:
            file_name = file.properties['Name']
            local_file_path = os.path.join(inputrca_loc, file_name)

            with open(local_file_path, "wb") as local_file:
                file.download(local_file)
                ctx.execute_query()
                print(f"Downloaded: {file_name}")

        print('Raw RCA downloaded successfully.')

    except Exception as e:
        print(f"An error occurred: {e}")


def get_recent_date():
    # Connect to vas transactions in Mongodb to collect last date
    host = config_mongo["HOST"]
    port = config_mongo["PORT"]
    user_name = config_mongo["USERNAME"]
    pass_word = config_mongo["PASSWORD"]
    db_name = config_mongo["DATABASE"]
    client = MongoClient(f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}')
    db = client['eftEngine']
    today = datetime.now(datetime.UTC)
    start = today - timedelta(days=30)
    
    pipeline = [
        {
            "$match": {
                "updatedAt": {"$gte": start, "$lt": today}
            }
        },
        {
            "$group": {
                "_id": "$terminalId",
                "latest_date": {"$max": "$updatedAt"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "terminalId": "$_id",
                "latest_date": 1
            }
        }
    ]

    # Sort the result by terminal if needed
    # pipeline.append({"$sort": {"terminal": 1}})
    print('Processing dates from VAS')
    result = list(db.journals_24_01_03.aggregate(pipeline))

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(result)
    print('Dates obtained from VAS')

    return df


def transform_file():
    if len(os.listdir(inputrca_loc)) == 0:
        print('No Available Raw RCA File')
        return
    elif len(os.listdir(inputrca_loc)) > 1:
        print(f'Cannot process multiple files in {inputrca_loc}')
        return
    else: 
        for rawfile in os.listdir(inputrca_loc):
            raw_rca_path = inputrca_loc + rawfile
            rca_df = pd.read_excel(raw_rca_path, sheet_name=None) # Read in the RCA file and convert to pandas dataframe
            
            print('Raw RCA file loaded')
            try:
                # Break up the excel tabs into different dataframes
                reg_df = rca_df['REGISTERED TERMINALS']
                connected_df = rca_df['CONNECTED TERMINALS']

                # Deleting irrelevant columns from reg_df
                del reg_df['Merchant_ID']
                del reg_df['Bank']
                del reg_df['MCC']
                del reg_df['ptsp_code']
                del reg_df['PTSP']
                del reg_df['Merchant_Account_No']
                del reg_df['AccountNo']
                del reg_df['Registered_Date']
                del reg_df['ConnectDate']
                del reg_df['Contact']
                del reg_df['Address']
                del reg_df['Phone']
                del reg_df['State']

                print('Transforming dataframe')
                
                # Update the 'CONNECTED' column based on conditions
                reg_df['CONNECTED'] = reg_df['Terminal_ID'].apply(
                    lambda tid: 'YES' if tid in connected_df['Terminal_ID'].values else 'NO'
                )

                # Get the latest_date DataFrame using get_recent_date function
                latest_date_df = get_recent_date()
                
                # Update the 'STATUS' column based on if the terminal id is in latest_date_df
                reg_df['STATUS'] = reg_df['Terminal_ID'].apply(
                    lambda stat: 'ACTIVE' if stat in latest_date_df['terminalId'].values else 'INACTIVE'
                )

                # Merge the latest_date data into reg_df based on 'Terminal_ID' and 'terminal'
                reg_df = reg_df.merge(latest_date_df, left_on='Terminal_ID', right_on='terminalId', how='left')

                # Rename 'LastSeenDate' to 'LAST_TRANSACTION_DATE'
                reg_df.rename(columns={'LastSeenDate': 'LAST_TRANSACTION_DATE'}, inplace=True)

                # Replace 'LAST_TRANSACTION_DATE' with the value from 'latest_date' where it's not null
                reg_df['LAST_TRANSACTION_DATE'] = reg_df['latest_date'].combine_first(reg_df['LAST_TRANSACTION_DATE'])


                # Drop the 'latest_date' column as it's no longer needed
                reg_df.drop('latest_date', axis=1, inplace=True)

            except Exception as dataframeException:
                print(f'An error occurred in processing dataframe: {dataframeException}')

            try:
                reg_df.to_excel((str(rca_loc) + 'processed_rca.xlsx'), index=False, engine='xlsxwriter')
                print(f'Processed RCA file loaded to {rca_loc}')
            except Exception as ex:
                print(f'An error occurred in building the processed excel file: {ex}')

         
# Define a function to download the database file and return the local file path
def download_database(url):
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
            raise Exception(f"Failed to connect to the database, response code: error{response.status_code}")
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


def create_current_dataframe():

    local_db_path = download_database(raw_url)

    # Connect to your SQLite database
    c_conn = sqlite3.connect(local_db_path)
    query1 = f"SELECT * FROM RCA_table"
    current_df = pd.read_sql_query(query1, c_conn)
    # Close the connection
    c_conn.close()
    print('Current df created')
    return current_df


def create_legacy_dataframe():

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
    leg_df = create_legacy_dataframe()
    cur_df = create_current_dataframe()
    print('Updating legacy date database')

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
    conn.close()


# Define a function to connect and update the database file in local
def connect_and_update_database():

    if len(os.listdir(rca_loc)) == 0:
        print('No Available Processed RCA File')
    elif len(os.listdir(rca_loc)) > 1:
        print(f'Cannot process multiple files in {rca_loc}')
    else:
        #loc_db_path = download_database(raw_url)
        conn = sqlite3.connect(local_db_path)

        for xfile in os.listdir(rca_loc):
                excel_file_loc = str(rca_loc) + str(xfile)

                df = pd.read_excel(excel_file_loc)
                df = df.astype(str)
                df['LAST_TRANSACTION_DATE'] = df['LAST_TRANSACTION_DATE'].apply(lambda x: x if pd.to_datetime(x, errors='coerce') is not pd.NaT else 'Not available')
                df['LAST_TRANSACTION_DATE'] = pd.to_datetime(df['LAST_TRANSACTION_DATE'], errors='coerce')
                df['LAST_TRANSACTION_DATE'] = df['LAST_TRANSACTION_DATE'].dt.date

        print('RCA file ready for db upload')

        cursor = conn.cursor()
        query1 = """
                CREATE TABLE RCA_table1 (
                    Terminal_ID TEXT, 
                    Merchant_Name TEXT, 
                    Terminal_Owner TEXT,
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
            print("Database updated")
        except Exception as e:
            print(f"An error occurred updating the database: {e}")

        conn.close()
        
def load_to_github():

    # Connect to the githubAPI with the access tokens and usernames
    username = config_git['USERNAME']
    repository = config_git['REPOSITORY']
    file_path = config_git['PATH']
    access_token = config_git['TOKEN']
    
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


def load_legacy_to_github():

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


def move_raw_rca_to_archive():
    ctx_auth = UserCredential(sharepoint_username, sharepoint_password)
    ctx = ClientContext(sharepoint_site_url).with_credentials(ctx_auth)

    # Load the file to archive
    try:
        if len(os.listdir(inputrca_loc)) > 0:
            for file in os.listdir(inputrca_loc):
                # Connect to SharePoint and upload files
                local_file_path = inputrca_loc + file
                
                sharepoint_library_name = '/sites/NIBSS-ITEXrepo/Shared Documents/RCA_archives'

                # Construct the file path in SharePoint
                target_folder = ctx.web.get_folder_by_server_relative_url(sharepoint_library_name)
                target_file = target_folder.upload_file(os.path.basename(local_file_path), open(local_file_path, 'rb'))
                ctx.execute_query()

                print(f"{file} uploaded successfully to RCA_archives.")

        else:
            print("Missing local raw RCA file")
            return
        
    except Exception as e:
        print(f"An error occured:{e}")

    folder_list_url = '/sites/NIBSS-ITEXrepo/Shared Documents/RCA_input'

    # Clean the file 
    try:
        # Get the folder by its relative URL
        list_source = ctx.web.get_folder_by_server_relative_url(folder_list_url)
        files = list_source.files
        ctx.load(files)
        ctx.execute_query()

        # Check if the folder is empty
        if len(files) == 0:
            print("The 'RCA_input' folder is empty. No files to move.")
            return
        else:
            files_to_delete = []

            # Iterate through files and add them to the delete list
            for file in files:
                files_to_delete.append(file)

            # Delete the files outside the loop
            for file in files_to_delete:
                file.delete_object()

            # Execute the query after all delete operations
            ctx.execute_query()
            print('Raw RCA deleted successfully.')

    except Exception as e:
        print(f"An error occurred deleting raw file: {e}")


def clean_data():

    time.sleep(30)

    # Delete the downloaded db file
    try:
        # Attempt to terminate any processes holding a handle to the file
        for proc in psutil.process_iter(['pid', 'name', 'open_files']):
            open_files = proc.info.get('open_files')
            if open_files:
                for item in open_files:
                    if local_db_path == item.path:
                        print(f"Terminating process {proc.info['pid']} ({proc.info['name']}) holding the file '{local_db_path}'.")
                        psutil.Process(proc.info['pid']).terminate()
                        time.sleep(1)  # Allow some time for the process to terminate

        os.remove(local_db_path)
        print(f"File '{local_db_path}' deleted successfully.")
    except FileNotFoundError:
        print(f"File '{local_db_path}' not found.")
    except PermissionError as pe:
        print(f"Permission denied. Unable to delete file local_db file: {pe}")
    except Exception as e:
        print(f"An error occurred while deleting the file: {e}")

    try:
        os.remove(legacy_db_path)
        print(f"File '{legacy_db_path}' deleted successfully.")
    except FileNotFoundError:
        print(f"File '{legacy_db_path}' not found.")
    except PermissionError as pe:
        print(f"Permission denied. Unable to delete file local_db file: {pe}")
    except Exception as e:
        print(f"An error occurred while deleting the file: {e}")

    # Delete the raw rca file
    for i in os.listdir(inputrca_loc):
        del_file = str(inputrca_loc) + str(i)
        try:
            os.remove(del_file)
            print(f"Raw RCA file deleted successfully.")
        except FileNotFoundError:
            print(f"File '{del_file}' not found.")
        except PermissionError:
            print(f"Permission denied. Unable to delete file '{del_file}'.")
        except Exception as e:
            print(f"An error occurred while deleting the file: {e}")

    # Finally delete the processed RCA file
    for i in os.listdir(rca_loc):
        del_file = str(rca_loc) + str(i)
        try:
            os.remove(del_file)
            print(f"Processed RCA file deleted successfully.")
        except FileNotFoundError:
            print(f"File '{del_file}' not found.")
        except PermissionError:
            print(f"Permission denied. Unable to delete file '{del_file}'.")
        except Exception as e:
            print(f"An error occurred while deleting the file: {e}")

def main():
    retrieve_rca_from_sharepoint()
    transform_file()
    update_legacy()
    connect_and_update_database()
    load_to_github()
    load_legacy_to_github()
    move_raw_rca_to_archive()
    clean_data()

if __name__ == '__main__':
    main()