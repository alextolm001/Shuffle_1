# GENERATE SOME VOTE DATA AND UPLOAD TO GOOGLE DRIVE VIA SERVICE ACCOUNT

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# =============================
# 1. GENEREER STEMDATA VOOR FRANKRIJK
# =============================

np.random.seed(42)
num_records = 1000

data = {
    "COUNTRY CODE": ["FR"] * num_records,
    "MOBILE NUMBER": [],
    "SONG NUMBER": np.random.randint(1, 26, num_records),
    "TIMESTAMP": []
}

current_time = datetime.now()

for _ in range(num_records):
    # Franse mobiele nummers beginnen vaak met +33 6 of +33 7
    first_digit = random.choice(["6", "7"])
    remaining_digits = "".join([str(random.randint(0, 9)) for _ in range(8)])
    formatted_number = f"+33 {first_digit} " + " ".join([remaining_digits[i:i+2] for i in range(0, 8, 2)])
    data["MOBILE NUMBER"].append(formatted_number)

for _ in range(num_records):
    random_seconds = random.randint(0, 3600)
    timestamp = current_time - timedelta(seconds=random_seconds)
    data["TIMESTAMP"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))

df = pd.DataFrame(data)
output_file = "generated_votes_fr.txt"
df.to_csv(output_file, sep="\t", index=False)

print(df.head(5))
print(f"\n{output_file} is aangemaakt.")

# =============================
# 2. UPLOAD NAAR GOOGLE DRIVE
# =============================

# Configuratie
SERVICE_ACCOUNT_FILE = "service_account.json"  # JSON bestand van Google
FOLDER_ID = "1CeJKRS9Dv_dXyUT2rSdaKe9OR2ObK5KF"  # Map-ID van je gedeelde Drive-map

# Authenticatie met service account
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive"]
)

service = build("drive", "v3", credentials=credentials)

# Upload bestand
file_metadata = {
    "name": output_file,
    "parents": [FOLDER_ID]
}
media = MediaFileUpload(output_file, mimetype="text/plain")

try:
    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()
    print(f"✅ Bestand succesvol geüpload naar Google Drive met ID: {uploaded_file.get('id')}")
except Exception as e:
    print(f"❌ Upload mislukt: {e}")
