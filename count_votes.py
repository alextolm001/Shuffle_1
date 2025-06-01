from pyspark.sql import SparkSession
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from datetime import datetime
import io
import json
import sys
import os

# ========== CONFIG ==========
SERVICE_ACCOUNT_FILE = "service_account.json"

INPUT_FOLDER_ID = "1CeJKRS9Dv_dXyUT2rSdaKe9OR2ObK5KF"      # Generated_votes
OUTPUT_FOLDER_ID = "1iJVmIgBLt2_WVA5AfOQzPalceUif2-Sd"     # Reduced_votes
LOGS_FOLDER_ID = "1aTNKqDVg8wxrTScrhyoKcHYcY_cQEJqU"        # System_logs

INPUT_FILE_NAME = "generated_votes_fr.txt"
OUTPUT_FILE_NAME = "reduced_votes.json"
LOG_FILE_NAME = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# ========== Logging helper ==========
log_lines = []

def log(msg):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{timestamp} {msg}"
    log_lines.append(line)
    print(line, flush=True)

def save_log_to_file():
    with open(LOG_FILE_NAME, "w") as log_file:
        log_file.write("\n".join(log_lines))

def upload_file_to_drive(service, filename, folder_id, mimetype="application/json"):
    file_metadata = {
        "name": filename,
        "parents": [folder_id]
    }
    media = MediaFileUpload(filename, mimetype=mimetype)
    uploaded = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()
    return uploaded.get("id")

# ========== Google Drive AUTH ==========
try:
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    drive_service = build("drive", "v3", credentials=credentials)
    log("🔐 Succesvol geauthenticeerd met Google Drive.")
except Exception as e:
    log(f"❌ Authenticatie mislukt: {e}")
    sys.exit(1)

# ========== Download van Drive ==========
try:
    log(f"🔍 Zoeken naar '{INPUT_FILE_NAME}' in de Generated_votes map...")
    query = f"name='{INPUT_FILE_NAME}' and '{INPUT_FOLDER_ID}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])

    if not items:
        raise FileNotFoundError(f"❌ Bestand '{INPUT_FILE_NAME}' niet gevonden in Drive.")

    file_id = items[0]["id"]
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(INPUT_FILE_NAME, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    log(f"✅ '{INPUT_FILE_NAME}' succesvol gedownload.")
except Exception as e:
    log(f"❌ Download mislukt: {e}")
    save_log_to_file()
    upload_file_to_drive(drive_service, LOG_FILE_NAME, LOGS_FOLDER_ID, mimetype="text/plain")
    sys.exit(1)

# ========== Spark-verwerking ==========
try:
    log("⚙️ Spark-telling wordt uitgevoerd...")
    spark = SparkSession.builder \
        .appName("SongVoteCount") \
        .master("local[*]") \
        .getOrCreate()

    rdd = spark.sparkContext.textFile(INPUT_FILE_NAME)

    mapped = rdd.map(lambda line: line.strip().split("\t")) \
                .filter(lambda fields: len(fields) >= 3 and fields[2].isdigit()) \
                .map(lambda fields: ((fields[0], fields[2]), 1))

    reduced = mapped.reduceByKey(lambda a, b: a + b)

    grouped = reduced.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                     .groupByKey() \
                     .mapValues(list)

    result = grouped.map(lambda x: {
        "country": x[0],
        "votes": [{"song_number": song, "count": count} for song, count in x[1]]
    }).collect()

    with open(OUTPUT_FILE_NAME, "w") as f:
        json.dump(result, f, indent=4)
    log(f"✅ '{OUTPUT_FILE_NAME}' lokaal opgeslagen.")

    spark.stop()
except Exception as e:
    log(f"❌ Spark-verwerking mislukt: {e}")
    save_log_to_file()
    upload_file_to_drive(drive_service, LOG_FILE_NAME, LOGS_FOLDER_ID, mimetype="text/plain")
    sys.exit(1)

# ========== Upload output naar Drive ==========
try:
    log(f"📤 Uploaden van '{OUTPUT_FILE_NAME}' naar Reduced_votes map...")
    file_id = upload_file_to_drive(drive_service, OUTPUT_FILE_NAME, OUTPUT_FOLDER_ID)
    log(f"✅ Bestand geüpload naar Drive (ID: {file_id})")
except Exception as e:
    log(f"❌ Upload mislukt: {e}")

# ========== Upload logbestand ==========
try:
    save_log_to_file()
    log("📄 Uploaden van logbestand naar System_logs map...")
    log_id = upload_file_to_drive(drive_service, LOG_FILE_NAME, LOGS_FOLDER_ID, mimetype="text/plain")
    log(f"✅ Logbestand geüpload naar Drive (ID: {log_id})")
except Exception as e:
    log(f"❌ Log-upload mislukt: {e}")

log("🏁 Verwerking voltooid.")

