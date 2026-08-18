import urllib.request
import os

# ==========================================
# CONFIGURATION VARIABLES (Top-Level)
# ==========================================
CATALOG_NAME = "claims_lakehouse"
LANDING_SCHEMA = "raw_landing"
VOLUME_NAME = "synthea_ingress"
SOURCE_URL = "https://mitre.box.com/shared/static/3bo45m48ocpzp8fc0tp005vax7l93xji.gz"
TARGET_FILE_NAME = "synthea_export.json.gz"

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/{VOLUME_NAME}"
FULL_FILE_PATH = f"{VOLUME_PATH}/{TARGET_FILE_NAME}"

# ==========================================
# INFRASTRUCTURE SETUP
# ==========================================
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.{VOLUME_NAME}")

# ==========================================
# DOWNLOAD & STAGE DATA
# ==========================================
print(f"Downloading dataset to {FULL_FILE_PATH}...")
urllib.request.urlretrieve(SOURCE_URL, FULL_FILE_PATH)
print("Staging complete. File ready for automated Bronze ingestion.")