import json
import os
import subprocess
import datetime
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

# from dotenv import load_dotenv
#
# load_dotenv()

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

# Get the current date and time for the backup file name
date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# Create the backup directory if it doesn't exist
os.makedirs(BACKUP_DIR, exist_ok=True)

# Extract the password from the database URL
password = DB_URL.split("password=")[1].split("&")[0]

# Perform the backup
backup_file = f"{BACKUP_DIR}/backup-{date}.sql"
subprocess.run([
    "pg_dump",
    "-h", DB_URL.split("host=")[1].split("&")[0],
    "-U", DB_USER,
    "-d", DB_NAME,
    "-F", "c",
    "-f", backup_file
], check=True)

# Optionally, compress the backup
compressed_backup_file = f"{BACKUP_DIR}/backup-{date}.sql.gz"
subprocess.run(["gzip", backup_file], check=True)

# Parse the json key
service_account_info = json.loads(SERVICE_ACCOUNT_KEY)

# Authenticate with Google Drive API
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# Upload the backup to Google Drive
file_metadata = {
    "name": os.path.basename(compressed_backup_file),
    "parents": [FOLDER_ID]
}
media = MediaFileUpload(compressed_backup_file, resumable=True)
file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

print(f"Backup uploaded to Google Drive: {file.get('id')}")

# # Optionally, remove backups older than 7 days
# seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
# for file in os.listdir(BACKUP_DIR):
#     file_path = os.path.join(BACKUP_DIR, file)
#     if os.path.isfile(file_path) and file.endswith(".sql.gz"):
#         file_date = datetime.datetime.strptime(file.split(".")[0].split("-")[-1], "%Y%m%d%H%M%S")
#         if file_date < seven_days_ago:
#             os.remove(file_path)

# Optionally, manage the number of backups
backup_files = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".sql.gz")]
backup_files.sort(key=lambda x: os.path.getmtime(os.path.join(BACKUP_DIR, x)))

while len(backup_files) > MAX_BACKUPS:
    oldest_file = backup_files.pop(0)
    os.remove(os.path.join(BACKUP_DIR, oldest_file))

print(f"Backup management complete. {len(backup_files)} backups retained.")