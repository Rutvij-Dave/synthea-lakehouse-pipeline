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

LOCAL_TMP = "/tmp/synthea_landing"


# ============================================================
# CURRENT SYNTHEA SAMPLE DATA
# ============================================================

DATASETS = {
    "fhir_r4": {
        "url": "https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_fhir_r4_sep2019.zip",
        "zip_name": "synthea_sample_data_fhir_r4.zip",
    },
    "fhir_stu3": {
        "url": "https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_fhir_stu3_sep2019.zip",
        "zip_name": "synthea_sample_data_fhir_stu3.zip",
    },
    "fhir_dstu2": {
        "url": "https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_fhir_dstu2_sep2019.zip",
        "zip_name": "synthea_sample_data_fhir_dstu2.zip",
    },
    "ccda": {
        "url": "https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_ccda_sep2019.zip",
        "zip_name": "synthea_sample_data_ccda.zip",
    },
    "csv": {
        "url": "https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_csv_apr2020.zip",
        "zip_name": "synthea_sample_data_csv.zip",
    },
}


# ============================================================
# HELPERS
# ============================================================

def download_file(url: str, local_path: str) -> None:

    print("=" * 80)
    print("DOWNLOADING")
    print(url)
    print("=" * 80)

    response = requests.get(
        url,
        stream=True,
        allow_redirects=True,
        timeout=300,
    )

    response.raise_for_status()

    total_bytes = 0

    with open(local_path, "wb") as f:

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):

            if chunk:

                f.write(chunk)
                total_bytes += len(chunk)

    print(
        f"Downloaded: "
        f"{total_bytes / (1024 * 1024):.2f} MB"
    )


def copy_tree_to_volume(
    local_dir: str,
    volume_dir: str
) -> None:

    os.makedirs(volume_dir, exist_ok=True)

    for item in os.listdir(local_dir):

        source = os.path.join(local_dir, item)
        target = os.path.join(volume_dir, item)

        if os.path.isdir(source):

            if os.path.exists(target):
                shutil.rmtree(target)

            shutil.copytree(
                source,
                target
            )

        else:

            shutil.copy2(
                source,
                target
            )


# ============================================================
# CREATE VOLUME DIRECTORIES
# ============================================================

os.makedirs(
    DOWNLOAD_DIR,
    exist_ok=True
)

os.makedirs(
    EXTRACT_DIR,
    exist_ok=True
)

os.makedirs(
    LOCAL_TMP,
    exist_ok=True
)


# ============================================================
# PROCESS DATASETS
# ============================================================

for dataset_name, config in DATASETS.items():

    print()
    print("#" * 80)
    print(
        f"PROCESSING DATASET: "
        f"{dataset_name}"
    )
    print("#" * 80)

    url = config["url"]
    zip_name = config["zip_name"]

    local_zip = (
        f"{LOCAL_TMP}/{zip_name}"
    )

    local_extract = (
        f"{LOCAL_TMP}/{dataset_name}"
    )

    volume_download = (
        f"{DOWNLOAD_DIR}/{zip_name}"
    )

    volume_extract = (
        f"{EXTRACT_DIR}/{dataset_name}"
    )

    # --------------------------------------------------------
    # 1. Download locally
    # --------------------------------------------------------

    print("Step 1: Downloading ZIP...")

    download_file(
        url=url,
        local_path=local_zip
    )

    # --------------------------------------------------------
    # 2. Copy ZIP to Volume
    # --------------------------------------------------------

    print("Step 2: Copying ZIP to Volume...")

    shutil.copy2(
        local_zip,
        volume_download
    )

    print(
        f"ZIP saved to:\n"
        f"{volume_download}"
    )

    # --------------------------------------------------------
    # 3. Extract locally
    # --------------------------------------------------------

    print(
        "Step 3: Extracting locally..."
    )

    if os.path.exists(local_extract):

        shutil.rmtree(
            local_extract
        )

    os.makedirs(
        local_extract,
        exist_ok=True
    )

    with zipfile.ZipFile(
        local_zip,
        "r"
    ) as zip_ref:

        zip_ref.extractall(
            local_extract
        )

    # --------------------------------------------------------
    # 4. Copy extraction to Volume
    # --------------------------------------------------------

    print(
        "Step 4: Copying extracted data "
        "to Unity Catalog Volume..."
    )

    if os.path.exists(volume_extract):

        shutil.rmtree(
            volume_extract
        )

    os.makedirs(
        volume_extract,
        exist_ok=True
    )

    copy_tree_to_volume(
        local_extract,
        volume_extract
    )

    print(
        f"Extracted data saved to:\n"
        f"{volume_extract}"
    )

    # --------------------------------------------------------
    # 5. Cleanup local temporary files
    # --------------------------------------------------------

    os.remove(local_zip)

    shutil.rmtree(
        local_extract
    )

    print(
        f"Completed: {dataset_name}"
    )


print()
print("=" * 80)
print(
    "ALL SYNTHEA DATASETS DOWNLOADED "
    "AND EXTRACTED SUCCESSFULLY"
)
print("=" * 80)