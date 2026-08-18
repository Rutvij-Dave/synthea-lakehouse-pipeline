import urllib.request
import zipfile
import os

# ==========================================
# CONFIGURATION VARIABLES
# ==========================================
CATALOG_NAME = "claims_lakehouse"
LANDING_SCHEMA = "raw_landing"
VOLUME_NAME = "synthea_ingress"

# Using the 36MB FHIR R4 (JSON) dataset directly from GitHub to bypass network blocks
SOURCE_URL = "https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_fhir_latest.zip"
ZIP_FILE_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/{VOLUME_NAME}/fhir_sample.zip"
EXTRACT_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/{VOLUME_NAME}/"

# ==========================================
# INFRASTRUCTURE SETUP
# ==========================================
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.{VOLUME_NAME}")

# ==========================================
# DOWNLOAD & EXTRACT DATA
# ==========================================
print("1. Downloading 36 MB FHIR R4 Sample Dataset...")
urllib.request.urlretrieve(SOURCE_URL, ZIP_FILE_PATH)

print("2. Extracting JSON files into the landing zone...")
with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
    zip_ref.extractall(EXTRACT_PATH)

# Clean up the zip file to save storage space
os.remove(ZIP_FILE_PATH)

print(f"Success! The JSON files are staged in {EXTRACT_PATH} and ready for Auto Loader.")