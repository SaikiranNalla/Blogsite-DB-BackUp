import json
import os
import subprocess
import datetime
import gzip
import shutil
import psycopg2
from urllib.parse import urlparse, parse_qs
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

# Environment Variables from .env or platform secrets
DB_URL = os.environ.get('DB_URL')
DB_USER = os.environ.get('DB_USER')  # Optional fallback
DB_NAME = os.environ.get('DB_NAME')
SERVICE_ACCOUNT_KEY = os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY')
FOLDER_ID = os.environ.get('GOOGLE_DRIVE_FOLDER_ID')
# Use a common backup directory. On Linux, /tmp/backups works well; on Windows, we'll use C:\temp\backups.
if os.name == 'nt':
    BACKUP_DIR = os.environ.get('BACKUP_DIR', r"C:\temp\backups")
else:
    BACKUP_DIR = os.environ.get('BACKUP_DIR', "/tmp/backups")
MAX_BACKUPS = 7

def extract_db_credentials(db_url):
    """
    Extracts DB credentials from the DB_URL.
    Expected format:
      postgres://username:password@hostname:port/dbname
      Or: postgres://username@hostname:port/dbname?password=yourpassword
    Returns a dictionary with keys: dbname, user, password, host, port.
    """
    parsed = urlparse(db_url)
    creds = {
        "dbname": parsed.path.lstrip('/'),
        "user": parsed.username if parsed.username else DB_USER,
        "host": parsed.hostname,
        "port": parsed.port or 5432
    }
    if parsed.password:
        creds["password"] = parsed.password
    else:
        query_params = parse_qs(parsed.query)
        if 'password' in query_params and query_params['password']:
            creds["password"] = query_params['password'][0]
    if "password" not in creds:
        raise ValueError("No password found in the DB_URL. Please ensure it includes the password, or set it separately.")
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

# Extract and test DB credentials
db_creds = extract_db_credentials(DB_URL)
test_db_connection(db_creds)

# Set the PGPASSWORD environment variable so that pg_dump can use it
os.environ["PGPASSWORD"] = db_creds["password"]

# Determine the pg_dump executable:
# - On Windows, you might need the full path (or provide it via an environment variable)
# if os.name == 'nt':
#     # Optionally allow PG_DUMP_PATH environment variable to override default path
#     pg_dump_executable = os.environ.get('PG_DUMP_PATH', r"C:\Program Files\PostgreSQL\13\bin\pg_dump.exe")
# else:
#     pg_dump_executable = "pg_dump"  # On Linux, pg_dump is usually in the PATH.

if os.name == 'nt':
    pg_dump_executable = r"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe"
else:
    pg_dump_executable = "pg_dump"


# Create the backup directory if it doesn't exist
os.makedirs(BACKUP_DIR, exist_ok=True)

# Define backup file names based on current datetime
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
backup_file = os.path.join(BACKUP_DIR, f"backup-{timestamp}.sql")
compressed_backup_file = backup_file + ".gz"

# Run pg_dump to create a backup
subprocess.run([
    pg_dump_executable,
    "-h", db_creds["host"],
    "-U", db_creds["user"],
    "-d", db_creds["dbname"],
    "-F", "c",  # Custom format
    "-f", backup_file
], check=True)

# Compress the backup file using Python's gzip module (cross-platform)
with open(backup_file, 'rb') as f_in, gzip.open(compressed_backup_file, 'wb') as f_out:
    shutil.copyfileobj(f_in, f_out)
os.remove(backup_file)

# Authenticate with Google Drive API using the service account key
service_account_info = json.loads(SERVICE_ACCOUNT_KEY)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# Upload the backup file to Google Drive
file_metadata = {
    "name": os.path.basename(compressed_backup_file),
    "parents": [FOLDER_ID]
}
media = MediaFileUpload(compressed_backup_file, resumable=True)
uploaded_file = drive_service.files().create(
    body=file_metadata, media_body=media, fields="id"
).execute()

print(f"Backup uploaded to Google Drive: {uploaded_file.get('id')}")

# Remove old backups exceeding MAX_BACKUPS
backup_files = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".sql.gz")]
backup_files.sort(key=lambda f: os.path.getmtime(os.path.join(BACKUP_DIR, f)))
while len(backup_files) > MAX_BACKUPS:
    oldest_file = backup_files.pop(0)
    os.remove(os.path.join(BACKUP_DIR, oldest_file))

print(f"Backup management complete. {len(backup_files)} backups retained.")
