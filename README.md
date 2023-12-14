# PTSP-app-dataupdate
## Introduction
This repository houses a robust end-to-end automated pipeline designed to facilitate the retrieval, processing, and updating of electronic fund transaction-related data for Root Cause Analysis (RCA). The Python-based pipeline integrates seamlessly with diverse data sources, executes intricate data transformations, and ultimately updates a SQLite database, subsequently pushing the updated data to a designated GitHub repository. Each distinct facet of this multifaceted process is meticulously encapsulated within modular functions to optimize code clarity and enhance maintainability.

## Prerequisites
Before executing the pipeline, ensure the following prerequisites are met:

1. Python Environment:

A Python interpreter (version 3.6 or later) must be installed on the system.
2. Python Libraries:

Install the necessary Python libraries by running:
bash

'pip install pandas sqlite3 requests base64 pymongo office365'
3. SharePoint Credentials:

Valid SharePoint credentials are required to access and download files from the SharePoint site.
4. GitHub Access Token:

Generate a GitHub personal access token with the required repository permissions.
5. MongoDB Connection:

Ensure the pipeline has the necessary credentials to connect to the MongoDB server.

## Main Pipeline Code
1. Retrieve RCA Data from SharePoint
Dependencies:

office365, requests, os
Description:

Connects to the specified SharePoint site to download raw RCA files from the 'RCA_input' folder.
Organizes the downloaded files locally within the defined directory structure.
2. Transform RCA Data
Dependencies:

pandas, os, datetime
Description:

Processes the raw RCA file, executing complex data transformations and data cleaning operations.
Generates a processed RCA file, incorporating pertinent updates and refined information.
3. Connect and Update Database
Dependencies:

pandas, sqlite3, os
Description:

Downloads a database file from a specified URL.
Establishes a connection to a SQLite database and executes an update operation with the processed RCA data.
4. Load to GitHub
Dependencies:

requests, base64
Description:

Connects to a designated GitHub repository using provided access tokens and repository details.
Retrieves the SHA of the existing database file on GitHub.
Updates the GitHub repository with the new processed data, ensuring version control.
5. Move Raw RCA to Archive
Dependencies:

office365, os
Description:

Uploads the raw RCA files to a specified SharePoint folder ('RCA_archives') for archival purposes.
Deletes the original raw RCA files from the source folder ('RCA_input') on SharePoint.
6. Clean Data
Dependencies:

os
Description:

Deletes temporary and unnecessary files, including the downloaded database file, raw RCA files, and processed RCA files.
Enhances data hygiene and resource optimization.
Configuration
The pipeline relies on a meticulously crafted configuration file (credentials.json) to store sensitive information and customizable parameters. It is imperative to populate this file accurately with the requisite credentials and configurations before initiating the script execution.

## Usage
Execute the pipeline by invoking the main() function using a Python interpreter. Ensure that the defined dependencies are installed before executing the script.

bash
python main_pipeline.py
### Author
Daniel Opanubi
