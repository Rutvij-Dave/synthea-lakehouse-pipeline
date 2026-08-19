import os
import shutil
import zipfile
import requests

# ============================================================
# CONFIGURATION
# ============================================================

BASE_VOLUME = "/Volumes/claims_lakehouse/raw/synthea_ingress"

DOWNLOAD_DIR = f"{BASE_VOLUME}/downloads"
EXTRACT_DIR = f"{BASE_VOLUME}/extracted"

# Temporary local directory on the Databricks driver
LOCAL_TMP = "/local_disk0/synthea_tmp"

# ============================================================
# SYNTHEA DOWNLOAD URLS
# ============================================================

DATASETS = {
    "fhir_r4": {
        "url": "https://mitre.box.com/shared/static/ylzmiichhvtw1igr4ck6q32i5b333nqs.zip",
        "zip_name": "synthea_sample_data_fhir_r4.zip",
    },
    "fhir_stu3": {
        "url": "https://mitre.box.com/shared/static/sxskvjlssubht58y23a1uua27sltitfc.zip",
        "zip_name": "synthea_sample_data_fhir_stu3.zip",
    },
    "fhir_dstu2": {
        "url": "https://mitre.box.com/shared/static/8klkudtknxvhvrmdmud3lo319ultvjze.zip",
        "zip_name": "synthea_sample_data_fhir_dstu2.zip",
    },
    "ccda": {
        "url": "https://mitre.box.com/shared/static/scrhj9jry8xarko6pyjyvt8w378wcybv.zip",
        "zip_name": "synthea_sample_data_ccda.zip",
    },
    "csv": {
        "url": "https://mitre.box.com/shared/static/aw9po06ypfb9hrau4jamtvtz0e5ziucz.zip",
        "zip_name": "synthea_sample_data_csv.zip",
    },
}


# ============================================================
# CREATE DIRECTORIES
# ============================================================

os.makedirs(LOCAL_TMP, exist_ok=True)

dbutils.fs.mkdirs(DOWNLOAD_DIR)
dbutils.fs.mkdirs(EXTRACT_DIR)


# ============================================================
# DOWNLOAD FUNCTION
# ============================================================

def download_file(url: str, local_path: str) -> None:

    print(f"Downloading:")
    print(url)

    response = requests.get(
        url,
        stream=True,
        allow_redirects=True,
        timeout=300,
    )

    response.raise_for_status()

    total_bytes = 0

    with open(local_path, "wb") as file:

        for chunk in response.iter_content(chunk_size=1024 * 1024):

            if chunk:

                file.write(chunk)
                total_bytes += len(chunk)

    print(
        f"Downloaded {total_bytes / (1024 * 1024):.2f} MB"
    )


# ============================================================
# PROCESS EACH DATASET
# ============================================================

for dataset_name, config in DATASETS.items():

    print("=" * 80)
    print(f"PROCESSING DATASET: {dataset_name}")
    print("=" * 80)

    url = config["url"]
    zip_name = config["zip_name"]

    local_zip = f"{LOCAL_TMP}/{zip_name}"

    volume_zip = f"{DOWNLOAD_DIR}/{zip_name}"

    local_extract = f"{LOCAL_TMP}/{dataset_name}"

    volume_extract = f"{EXTRACT_DIR}/{dataset_name}"

    # --------------------------------------------------------
    # 1. Download ZIP to local Databricks driver
    # --------------------------------------------------------

    download_file(
        url=url,
        local_path=local_zip,
    )

    # --------------------------------------------------------
    # 2. Copy ZIP into Unity Catalog Volume
    # --------------------------------------------------------

    dbutils.fs.cp(
        f"file:{local_zip}",
        volume_zip,
        recurse=False,
    )

    print(f"ZIP saved to: {volume_zip}")

    # --------------------------------------------------------
    # 3. Extract locally
    # --------------------------------------------------------

    if os.path.exists(local_extract):
        shutil.rmtree(local_extract)

    os.makedirs(local_extract, exist_ok=True)

    print(f"Extracting {zip_name} ...")

    with zipfile.ZipFile(local_zip, "r") as zip_ref:

        zip_ref.extractall(local_extract)

    # --------------------------------------------------------
    # 4. Copy extracted files to Unity Catalog Volume
    # --------------------------------------------------------

    dbutils.fs.rm(
        volume_extract,
        recurse=True,
    )

    dbutils.fs.mkdirs(volume_extract)

    dbutils.fs.cp(
        f"file:{local_extract}",
        volume_extract,
        recurse=True,
    )

    print(
        f"Extracted files available at: {volume_extract}"
    )

    # --------------------------------------------------------
    # 5. Remove local temporary files
    # --------------------------------------------------------

    os.remove(local_zip)

    shutil.rmtree(local_extract)

    print(f"Completed: {dataset_name}")


print("=" * 80)
print("ALL SYNTHEA DATASETS DOWNLOADED AND EXTRACTED")
print("=" * 80)