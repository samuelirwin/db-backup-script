import os
import subprocess
import datetime
import boto3
from tqdm import tqdm

# Configuration
DATABASES = ['']  # List of databases to back up
BACKUP_DIR = 'dumps'  # Directory where backups will be stored
ENABLE_S3_UPLOAD = False  # Set to False to disable S3 upload
S3_BUCKET = 'your-s3-bucket-name'  # S3 bucket name
AWS_REGION = 'your-region'  # S3 bucket AWS region
CNF_CREDS = '/Users/samuelirwin/backup/scripts/.my.cnf'

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
backup_path = os.path.join(BACKUP_DIR, f"backup_{timestamp}")
os.makedirs(backup_path, exist_ok=True)


def get_tables(db_name):
    """Fetch the list of tables for the given database."""
    try:
        command = [
            "mysql",
            f"--defaults-extra-file={CNF_CREDS}",
            "-e",
            f"SHOW TABLES IN {db_name};"
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, text=True, check=True)
        tables = result.stdout.strip().split("\n")[1:]  # Skip the header
        return tables
    except subprocess.CalledProcessError as e:
        print(f"Error fetching tables for database {db_name}: {e}")
        return []


def backup_table(db_name, table_name):
    """Backup a single table into an SQL file."""
    table_backup_path = os.path.join(backup_path, db_name)
    os.makedirs(table_backup_path, exist_ok=True)
    backup_file = os.path.join(table_backup_path, f"{table_name}.sql")
    try:
        command = [
            "mysqldump",
            f"--defaults-extra-file={CNF_CREDS}",
            db_name,
            table_name,
            f"--result-file={backup_file}"
        ]
        subprocess.run(command, check=True)
        return backup_file
    except subprocess.CalledProcessError as e:
        print(f"Error backing up table {db_name}.{table_name}: {e}")
        return None


def upload_to_s3(file_path, bucket, s3_key):
    """Upload a file to S3."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    try:
        s3.upload_file(file_path, bucket, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"Error uploading {file_path} to S3: {e}")


# Backup each database
with tqdm(total=len(DATABASES), desc="Backing up databases", unit="database") as db_pbar:
    for db in DATABASES:
        print(f"Processing database: {db}")
        tables = get_tables(db)

        for table in tables:
            backup_file = backup_table(db, table)
            if backup_file and ENABLE_S3_UPLOAD:
                # Upload to S3 if enabled
                s3_key = f"backups/{os.path.basename(backup_path)}/{db}/{os.path.basename(backup_file)}"
                upload_to_s3(backup_file, S3_BUCKET, s3_key)

        db_pbar.update(1)

print(f"All backups saved to {backup_path}")
if ENABLE_S3_UPLOAD:
    print("Backups have also been uploaded to S3.")
