import json
import os
import subprocess
import datetime
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

from dotenv import load_dotenv

load_dotenv()

import psycopg2

from urllib.parse import urlparse, parse_qs

def verify_db_url(url):
    parsed = urlparse(url)
    # Check for hostname, path, scheme etc.
    if not all([parsed.scheme, parsed.hostname, parsed.path]):
        raise ValueError("The DB_URL is missing essential components.")
    return parsed

def extract_password(url):
    # This first checks for user info credentials before query parameters.
    parsed = urlparse(url)
    if parsed.username and parsed.password:
        return parsed.password
    # Alternatively, check query parameters for a password key
    query_params = parse_qs(parsed.query)
    if 'password' in query_params and query_params['password']:
        return query_params['password'][0]
    raise ValueError("No password found in the DB_URL.")

# Test

# import psycopg2
# from urllib.parse import urlparse
#
# def test_db_connection(db_url):
#     # Parse the URL to extract connection components
#     parsed = urlparse(db_url)
#     conn_params = {
#         "dbname": parsed.path[1:],  # Skip the leading /
#         "user": parsed.username,
#         "password": parsed.password,
#         "host": parsed.hostname,
#         "port": parsed.port or 5432  # default PostgreSQL port
#     }
#     try:
#         # Try to establish a connection
#         conn = psycopg2.connect(**conn_params)
#         conn.close()
#         print("Connection successful!")
#     except Exception as e:
#         print(f"Connection failed: {e}")
#
# db_url = os.environ.get('DB_URL')
# test_db_connection(db_url)



# Database credentials
DB_URL = os.environ.get('DB_URL')
DB_USER = os.environ.get('DB_USER')
DB_NAME = os.environ.get('DB_NAME')

# Google Drive credentials
SERVICE_ACCOUNT_KEY = os.environ['GOOGLE_SERVICE_ACCOUNT_KEY']
FOLDER_ID = os.environ['GOOGLE_DRIVE_FOLDER_ID']
BACKUP_DIR = "/tmp/backups"

# Maximum backup's
MAX_BACKUPS = 7


def extract_db_credentials(db_url):
    """
    Extracts DB credentials from the provided URL.

    Expected formats:
        postgres://username:password@hostname:port/dbname
    OR
        postgres://username@hostname:port/dbname?password=yourpassword
    """
    parsed = urlparse(db_url)
    creds = {
        "dbname": parsed.path.lstrip('/'),
        "user": parsed.username if parsed.username else DB_USER,
        "host": parsed.hostname,
        "port": parsed.port or 5432
    }
    # Try to get the password from the URL credentials first:
    if parsed.password:
        creds["password"] = parsed.password
    else:
        # If not found, try to extract from query parameters:
        query_params = parse_qs(parsed.query)
        if 'password' in query_params and query_params['password']:
            creds["password"] = query_params['password'][0]

    if "password" not in creds:
        raise ValueError("No password found in the DB_URL.")
    return creds


def test_db_connection(creds):
    """
    Tests the DB connection using psycopg2.
    """
    try:
        conn = psycopg2.connect(**creds)
        conn.close()
        print("Database connection successful!")
    except Exception as e:
        raise ValueError(f"Database connection failed: {e}")


# Extract credentials and test connection
db_creds = extract_db_credentials(DB_URL)
test_db_connection(db_creds)

# Set PGPASSWORD so that pg_dump can use it
os.environ["PGPASSWORD"] = db_creds["password"]

# Append the PostgreSQL bin directory to PATH if needed
# (Uncomment and update the path if pg_dump is not in your system PATH)
# os.environ["PATH"] += os.pathsep + r"C:\Program Files\PostgreSQL\14\bin"

# Get the current date and time for the backup file name
date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# Create the backup directory if it doesn't exist
os.makedirs(BACKUP_DIR, exist_ok=True)

# Define the backup file name
backup_file = f"{BACKUP_DIR}/backup-{date}.sql"

# Use the full path to pg_dump (update the path accordingly) if needed:
pg_dump_path = r"C:\Program Files\PostgreSQL\14\bin\pg_dump.exe"

# Perform the backup using pg_dump
subprocess.run([
    pg_dump_path,
    "-h", db_creds["host"],
    "-U", db_creds["user"],
    "-d", db_creds["dbname"],
    "-F", "c",
    "-f", backup_file
], check=True)

# Compress the backup file
compressed_backup_file = f"{backup_file}.gz"
subprocess.run(["gzip", backup_file], check=True)

# Parse the JSON key for Google Drive service account credentials
service_account_info = json.loads(SERVICE_ACCOUNT_KEY)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# Upload the compressed backup to Google Drive
file_metadata = {
    "name": os.path.basename(compressed_backup_file),
    "parents": [FOLDER_ID]
}
media = MediaFileUpload(compressed_backup_file, resumable=True)
uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

print(f"Backup uploaded to Google Drive: {uploaded_file.get('id')}")

# Manage backups: remove older backups if there are more than MAX_BACKUPS
backup_files = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".sql.gz")]
backup_files.sort(key=lambda f: os.path.getmtime(os.path.join(BACKUP_DIR, f)))
while len(backup_files) > MAX_BACKUPS:
    oldest_file = backup_files.pop(0)
    os.remove(os.path.join(BACKUP_DIR, oldest_file))

print(f"Backup management complete. {len(backup_files)} backups retained.")









#
# # Get the current date and time for the backup file name
# date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
#
# # Create the backup directory if it doesn't exist
# os.makedirs(BACKUP_DIR, exist_ok=True)
#
# # Extract the password from the database URL
# # password = DB_URL.split("password=")[1].split("&")[0]
# password = os.environ.get('DB_PASSWORD')
#
# # Perform the backup
# backup_file = f"{BACKUP_DIR}/backup-{date}.sql"
# subprocess.run([
#     "pg_dump",
#     # "-h", DB_URL.split("host=")[1].split("&")[0],
#     "-h", DB_URL,
#     "-U", DB_USER,
#     "-d", DB_NAME,
#     "-F", "c",
#     "-f", backup_file
# ], check=True)
#
# # Optionally, compress the backup
# compressed_backup_file = f"{BACKUP_DIR}/backup-{date}.sql.gz"
# subprocess.run(["gzip", backup_file], check=True)
#
# # Parse the json key
# service_account_info = json.loads(SERVICE_ACCOUNT_KEY)
#
# # Authenticate with Google Drive API
# credentials = service_account.Credentials.from_service_account_info(
#     service_account_info,
#     scopes=["https://www.googleapis.com/auth/drive"]
# )
# drive_service = build("drive", "v3", credentials=credentials)
#
# # Upload the backup to Google Drive
# file_metadata = {
#     "name": os.path.basename(compressed_backup_file),
#     "parents": [FOLDER_ID]
# }
# media = MediaFileUpload(compressed_backup_file, resumable=True)
# file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
#
# print(f"Backup uploaded to Google Drive: {file.get('id')}")
#
# # # Optionally, remove backups older than 7 days
# # seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
# # for file in os.listdir(BACKUP_DIR):
# #     file_path = os.path.join(BACKUP_DIR, file)
# #     if os.path.isfile(file_path) and file.endswith(".sql.gz"):
# #         file_date = datetime.datetime.strptime(file.split(".")[0].split("-")[-1], "%Y%m%d%H%M%S")
# #         if file_date < seven_days_ago:
# #             os.remove(file_path)
#
# # Optionally, manage the number of backups
# backup_files = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".sql.gz")]
# backup_files.sort(key=lambda x: os.path.getmtime(os.path.join(BACKUP_DIR, x)))
#
# while len(backup_files) > MAX_BACKUPS:
#     oldest_file = backup_files.pop(0)
#     os.remove(os.path.join(BACKUP_DIR, oldest_file))
#
# print(f"Backup management complete. {len(backup_files)} backups retained.")