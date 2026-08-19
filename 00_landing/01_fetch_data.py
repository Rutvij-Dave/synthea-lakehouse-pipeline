import os
import zipfile
import requests

# ============================================================
# CONFIGURATION
# ============================================================

BASE_VOLUME = "/Volumes/claims_lakehouse/raw/synthea_ingress"

DOWNLOAD_DIR = f"{BASE_VOLUME}/downloads"
EXTRACT_DIR = f"{BASE_VOLUME}/extracted"


# ============================================================
# SYNTHEA 1K SAMPLE DATASETS
# ============================================================

DATASETS = {
    "fhir_r4": {
        "url": (
            "https://synthetichealth.github.io/"
            "synthea-sample-data/downloads/"
            "synthea_sample_data_fhir_r4_sep2019.zip"
        ),
        "zip_name": "synthea_sample_data_fhir_r4.zip",
    },

    "fhir_stu3": {
        "url": (
            "https://synthetichealth.github.io/"
            "synthea-sample-data/downloads/"
            "synthea_sample_data_fhir_stu3_sep2019.zip"
        ),
        "zip_name": "synthea_sample_data_fhir_stu3.zip",
    },

    "fhir_dstu2": {
        "url": (
            "https://synthetichealth.github.io/"
            "synthea-sample-data/downloads/"
            "synthea_sample_data_fhir_dstu2_sep2019.zip"
        ),
        "zip_name": "synthea_sample_data_fhir_dstu2.zip",
    },

    "ccda": {
        "url": (
            "https://synthetichealth.github.io/"
            "synthea-sample-data/downloads/"
            "synthea_sample_data_ccda_sep2019.zip"
        ),
        "zip_name": "synthea_sample_data_ccda.zip",
    },

    "csv": {
        "url": (
            "https://synthetichealth.github.io/"
            "synthea-sample-data/downloads/"
            "synthea_sample_data_csv_apr2020.zip"
        ),
        "zip_name": "synthea_sample_data_csv.zip",
    },
}


# ============================================================
# CREATE DIRECTORIES
# ============================================================

dbutils.fs.mkdirs(DOWNLOAD_DIR)
dbutils.fs.mkdirs(EXTRACT_DIR)


# ============================================================
# DOWNLOAD FUNCTION
# ============================================================

def download_file(url: str, destination_path: str) -> None:

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

    with open(destination_path, "wb") as file:

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):
            if chunk:
                file.write(chunk)
                total_bytes += len(chunk)

    print(
        f"Downloaded: "
        f"{total_bytes / (1024 * 1024):.2f} MB"
    )


# ============================================================
# SAFE ZIP EXTRACTION
# ============================================================

def extract_zip_to_volume(zip_path: str, output_dir: str) -> None:

    print(f"Extracting:\n{zip_path}")

    # Remove previous extraction
    try:
        dbutils.fs.rm(
            output_dir,
            recurse=True
        )
    except Exception:
        pass

    dbutils.fs.mkdirs(output_dir)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:

        members = zip_ref.infolist()

        print(f"Files in ZIP: {len(members)}")

        for member in members:

            # Ignore directory entries
            if member.is_dir():
                continue

            # Normalize the ZIP path
            relative_path = member.filename.replace("\\", "/")

            # Prevent path traversal
            normalized = os.path.normpath(relative_path)

            if normalized.startswith(".."):
                raise ValueError(
                    f"Unsafe ZIP entry: {member.filename}"
                )

            destination = os.path.join(
                output_dir,
                normalized
            )

            destination_parent = os.path.dirname(
                destination
            )

            # Create parent directory
            if destination_parent:
                os.makedirs(
                    destination_parent,
                    exist_ok=True
                )

            # Read from ZIP and write directly to Volume
            with zip_ref.open(member, "r") as source:

                with open(destination, "wb") as target:

                    while True:

                        chunk = source.read(
                            1024 * 1024
                        )

                        if not chunk:
                            break

                        target.write(chunk)

            print(
                f"Extracted: {relative_path}"
            )

    print(
        f"Extraction complete:\n{output_dir}"
    )


# ============================================================
# PROCESS DATASETS
# ============================================================

for dataset_name, config in DATASETS.items():

    print("\n")
    print("#" * 80)
    print(f"PROCESSING DATASET: {dataset_name}")
    print("#" * 80)

    url = config["url"]
    zip_name = config["zip_name"]

    # ZIP location in Unity Catalog Volume
    volume_zip = (
        f"{DOWNLOAD_DIR}/{zip_name}"
    )

    # Extracted location in Unity Catalog Volume
    volume_extract = (
        f"{EXTRACT_DIR}/{dataset_name}"
    )

    # --------------------------------------------------------
    # 1. DOWNLOAD
    # --------------------------------------------------------

    print("\nStep 1: Downloading ZIP...")

    download_file(
        url=url,
        destination_path=volume_zip,
    )

    print(
        f"ZIP saved to:\n{volume_zip}"
    )

    # --------------------------------------------------------
    # 2. EXTRACT DIRECTLY TO VOLUME
    # --------------------------------------------------------

    print(
        "\nStep 2: Extracting ZIP directly into Volume..."
    )

    extract_zip_to_volume(
        zip_path=volume_zip,
        output_dir=volume_extract,
    )

    print(
        f"\nCompleted dataset: {dataset_name}"
    )


# ============================================================
# COMPLETE
# ============================================================

print("\n")
print("#" * 80)
print("ALL SYNTHEA DATASETS DOWNLOADED AND EXTRACTED")
print("#" * 80)

print(
    f"\nDownloads:"
    f"\n{DOWNLOAD_DIR}"
)

print(
    f"\nExtracted data:"
    f"\n{EXTRACT_DIR}"
)