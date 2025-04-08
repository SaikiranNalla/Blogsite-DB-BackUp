import json
import os
import subprocess
import datetime
import psycopg2
from urllib.parse import urlparse, parse_qs
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

# Get credentials and settings from environment variables (GitHub Actions secrets are automatically loaded)
DB_URL = os.environ.get('DB_URL')
# Optional fallback variables:
DB_USER = os.environ.get('DB_USER')  # If not provided in the URL
DB_NAME = os.environ.get('DB_NAME')

SERVICE_ACCOUNT_KEY = os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY')
FOLDER_ID = os.environ.get('GOOGLE_DRIVE_FOLDER_ID')
BACKUP_DIR = "/tmp/backups"
MAX_BACKUPS = 7


def extract_db_credentials(db_url):
    """
    Extracts DB credentials from a URL.

    The URL is expected to be in one of these forms:
      - postgres://username:password@hostname:port/dbname
      - postgres://username@hostname:port/dbname?password=yourpassword

    Returns a dict with keys: dbname, user, password, host, port.
    """
    parsed = urlparse(db_url)
    creds = {
        "dbname": parsed.path.lstrip('/'),
        "user": parsed.username if parsed.username else DB_USER,
        "host": parsed.hostname,
        "port": parsed.port or 5432
    }
    # Check for password in the URL's user info first
    if parsed.password:
        creds["password"] = parsed.password
    else:
        # Try to extract password from query parameters
        query_params = parse_qs(parsed.query)
        if 'password' in query_params and query_params['password']:
            creds["password"] = query_params['password'][0]

    if "password" not in creds:
        raise ValueError(
            "No password found in the DB_URL. Make sure your DB_URL includes the password information, or provide a DB_PASSWORD separately.")
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

# For GitHub Actions, if pg_dump is installed on your runner,
# you might need to add its directory to the PATH.
# Uncomment and update the next line if needed.
# os.environ["PATH"] += os.pathsep + "/usr/pgsql-14/bin"  # example for Linux runner

# Get current datetime to create a unique backup file name
date = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
os.makedirs(BACKUP_DIR, exist_ok=True)
backup_file = f"{BACKUP_DIR}/backup-{date}.sql"

# Run pg_dump.
# Using "pg_dump" by itself assumes it's in the PATH.
# If not, specify the full path to pg_dump (example for Linux or adjust accordingly).
pg_dump_executable = "pg_dump"  # or specify full path like r"/usr/pgsql-14/bin/pg_dump"

subprocess.run([
    pg_dump_executable,
    "-h", db_creds["host"],
    "-U", db_creds["user"],
    "-d", db_creds["dbname"],
    "-F", "c",
    "-f", backup_file
], check=True)

# Compress the backup file
compressed_backup_file = f"{backup_file}.gz"
subprocess.run(["gzip", backup_file], check=True)

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

# Remove backups older than MAX_BACKUPS
backup_files = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".sql.gz")]
backup_files.sort(key=lambda f: os.path.getmtime(os.path.join(BACKUP_DIR, f)))
while len(backup_files) > MAX_BACKUPS:
    oldest_file = backup_files.pop(0)
    os.remove(os.path.join(BACKUP_DIR, oldest_file))

print(f"Backup management complete. {len(backup_files)} backups retained.")
