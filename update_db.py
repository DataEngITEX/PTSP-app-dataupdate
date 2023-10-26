import pandas as pd
import sqlite3
import requests
import base64
import os
from pymongo import MongoClient
import urllib.parse
import json
from datetime import datetime, timedelta
from ftplib import FTP, error_perm


# Adding the configuration file to boost credential security
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = '\\'.join([ROOT_DIR, 'credentials.json'])

# read json file
with open(config_path) as config_file:
    config = json.load(config_file)
    config_dir = config['directories']
    config_ftp = config['ftp']
    config_git = config['github']
    config_mongo = config['mongodb']


raw_url = config_dir['RAW_DB']
sha_url = config_dir['SHA_DB']
rca_loc = config_dir['PROCESSED_RCA_LOC']
inputrca_loc = config_dir['RAW_RCA_LOC']


def retrieve_rca_file():
    try:
        # Connect to the FTP server
        ftp_host = config_ftp['FTP_HOST']
        ftp_user = config_ftp['FTP_USER']
        ftp_pass = config_ftp['FTP_PASSWORD']
        ftp = FTP(ftp_host)

        # Log in
        ftp.login(ftp_user, ftp_pass)

        # Change to the directory containing the file you want to download
        source_directory = '/users/Report_EIU/ITEX/RCA_files_input'
        destination_directory = '/users/Report_EIU/ITEX/RCA_files_archive'

        ftp.cwd(source_directory)
        file_list = ftp.nlst()

        if len(file_list) == 0:
            raise Exception('Folder empty, try later')
        else:
            for rcafile in file_list:
                try:
                    with open(rcafile, 'wb') as file:
                        ftp.retrbinary('RETR ' + source_directory + '/' + rcafile, file.write)
                    print(f'{rcafile} copied')

                    with open(rcafile, 'rb') as file:
                        ftp.storbinary('STOR ' + destination_directory + '/' + rcafile, file)
                    print(f'{rcafile} uploaded to archive')

                    # Download the file
                    with open(inputrca_loc, 'wb') as file:
                        ftp.retrbinary('RETR ' + rcafile, file.write)
                        print(f'{rcafile} downloaded')

                    ftp.delete(rcafile)
                    print(f'{rcafile} deleted from the source directory')

                except Exception as e:
                    print(f"An error occurred while processing {rcafile}: {e}")

    except error_perm as e:
        print(f"FTP error: {e}")

    except Exception as e:
        print(f"Unable to connect to FTP: {e}")

    finally:
        # Close the FTP connection
        ftp.quit()


def get_recent_date():
    # Connect to vas transactions in Mongodb to collect last date
    host = config_mongo["HOST"]
    port = config_mongo["PORT"]
    user_name = config_mongo["USERNAME"]
    pass_word = config_mongo["PASSWORD"]
    db_name = config_mongo["DATABASE"]
    client = MongoClient(f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}')
    db = client['eftEngine']
    today = datetime.utcnow()
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
    result = list(db.journals_23_10_12.aggregate(pipeline))

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(result)
    print('Dates obtained from VAS')

    return df

def transform_file():
    try:
        rca_df = pd.read_excel(inputrca_loc, sheet_name=None) # Read in the RCA file and convert to pandas dataframe
        print('Raw RCA file loaded')
        try:
            # Break up the excel tabs into different dataframes
            reg_df = rca_df['REGISTERED TERMINALS']
            connected_df = rca_df['CONNECTED TERMINALS']
            active_df = rca_df['ACTIVE TERMINALS']

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
            
            # Update the 'STATUS' column based on conditions
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

    except FileNotFoundError:
        raise Exception('Raw RCA file Unavailable')

    
        
# Define a function to download the database file and return the local file path
def download_database(url):
    response = requests.get(url)
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
    
# Define a function to connect and update the database file in local
def connect_and_update_database():

    if len(os.listdir(rca_loc)) == 0:
        print('No Available Processed RCA File')
    elif len(os.listdir(rca_loc)) > 1:
        print(f'Cannot process multiple files in {rca_loc}')
    else:
        loc_db_path = download_database(raw_url)
        conn = sqlite3.connect(loc_db_path)

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

def clean_data():

    # Delete the downloaded db file
    try:
        os.remove(local_db_path)
        print(f"File '{local_db_path}' deleted successfully.")
    except FileNotFoundError:
        print(f"File '{local_db_path}' not found.")
    except PermissionError:
        print(f"Permission denied. Unable to delete file '{local_db_path}'.")
    except Exception as e:
        print(f"An error occurred while deleting the file: {e}")

    # Delete the raw rca file
    try:
        os.remove(inputrca_loc)
        print(f"Raw RCA file deleted successfully.")
    except FileNotFoundError:
        print(f"File '{inputrca_loc}' not found.")
    except PermissionError:
        print(f"Permission denied. Unable to delete file '{inputrca_loc}'.")
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
    retrieve_rca_file()
    transform_file()
    connect_and_update_database()
    load_to_github()
    clean_data()

if __name__ == '__main__':
    main()