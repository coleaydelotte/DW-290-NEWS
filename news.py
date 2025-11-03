import os
import zipfile
import duckdb
import shutil
import re
import glob
import filecmp
from git import Repo

# --- CONFIG ---
REPO_URL = "https://github.com/Webhose/fake-news-dataset.git"
REPO_DIR = "fake-news-dataset"
DATA_DIR = "./fake-news-dataset/Datasets"
UNZIPPED_FLAT_DIR = "unzipped_flat"
UNZIPPED_ALL_DIR = "unzipped_all"
CSV_FILE = "all_articles_summary.csv"

# --- CLONE OR PULL REPO ---
if not os.path.exists(REPO_DIR):
    print("Cloning fake-news-dataset repo...")
    Repo.clone_from(REPO_URL, REPO_DIR)
else:
    print("Updating existing fake-news-dataset repo...")
    repo = Repo(REPO_DIR)
    origin = repo.remotes.origin
    origin.pull()

# --- FIND ZIP FILES IN REPO ---
repo_zip_files = glob.glob(os.path.join(REPO_DIR, "*.zip"))
os.makedirs(DATA_DIR, exist_ok=True)

# --- CHECK FOR NEW ZIP FILES ---
def zip_is_new(zip_path, data_dir):
    """Check if a zip file is new or has changed compared to the one in data_dir."""
    filename = os.path.basename(zip_path)
    dest_path = os.path.join(data_dir, filename)
    if not os.path.exists(dest_path):
        return True
    return not filecmp.cmp(zip_path, dest_path, shallow=False)

new_zips = [z for z in repo_zip_files if zip_is_new(z, DATA_DIR)]

# --- HANDLE NEW FILES ---
if new_zips:
    print("New ZIP files detected — resetting directories and CSV...")

    # Delete old folders and CSV
    for target in [UNZIPPED_FLAT_DIR, UNZIPPED_ALL_DIR]:
        if os.path.exists(target):
            shutil.rmtree(target)
            print(f"Deleted {target}/")
        os.makedirs(target)
        print(f"Recreated {target}/")

    if os.path.exists(CSV_FILE):
        os.remove(CSV_FILE)
        print(f"Deleted {CSV_FILE}")

    # Copy new ZIPs to data folder
    for zip_path in repo_zip_files:
        shutil.copy2(zip_path, DATA_DIR)
        print(f"Copied {os.path.basename(zip_path)} to data/")
else:
    print("No new ZIP files detected — keeping existing folders and CSV.")

print("Setup complete — continuing with main processing...")

# --- EXISTING SCRIPT CONTENT BELOW ---
# (Paste the rest of your original news.py logic here, e.g. unzip, DuckDB, and data processing steps)

data_dir = "./fake-news-dataset/Datasets"
extract_root = "./unzipped_all"
flat_dir = "./unzipped_flat"
output_csv = "all_articles_summary.csv"

# Extracts the files into their own directory, skips if the directory already exists
if not os.path.exists(flat_dir):
    print("No existing flat directory found, extracting ZIP files...")
    # Clean and prepare directories
    for d in [extract_root, flat_dir]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    # Find all ZIPs
    zip_files = glob.glob(os.path.join(data_dir, "*.zip"))
    if not zip_files:
        raise FileNotFoundError("No ZIP files found in ./fake-news-dataset/Datasets")

    print(f"Found {len(zip_files)} ZIP files to process.")

    json_count = 0

    # Process each ZIP
    for zip_path in zip_files:
        match = re.search(r'(\d{8,})', os.path.basename(zip_path))
        date_code = match.group(1) if match else "unknown"

        zip_extract_dir = os.path.join(
            extract_root, os.path.splitext(os.path.basename(zip_path))[0]
        )
        os.makedirs(zip_extract_dir, exist_ok=True)

        # Extract contents
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(zip_extract_dir)

        # Copy JSONs and rename with date code
        for root, _, files in os.walk(zip_extract_dir):
            for filename in files:
                if filename.lower().endswith(".json"):
                    name_no_ext = os.path.splitext(filename)[0]
                    new_name = f"{name_no_ext}_{date_code}.json"
                    src = os.path.join(root, filename)
                    dst = os.path.join(flat_dir, new_name)
                    shutil.copy(src, dst)
                    json_count += 1

    print(f"Extracted and renamed {json_count} JSON files from all ZIPs.")
else:
    print(f"Flattened directory already exists — skipping extraction and renaming.")

# Loads extracted and renamed files into duckdb
con = duckdb.connect()

query = f"""
SELECT
    json_extract_string(data, '$.author') AS Author,
    json_extract_string(data, '$.url') AS Site,
    json_extract_string(data, '$.published') AS Published,
    json_extract_string(data, '$.title') AS Title,
    json_extract_string(data, '$.country') AS country,
FROM read_json_auto(
    '{flat_dir}/*.json',
    sample_size=-1,
    ignore_errors=true,
    union_by_name=true,
    maximum_depth=10,
    filename=true
) AS data
"""

print("Reading JSON files with DuckDB...")
df = con.execute(query).df()

# Writes the queried data to a CSV
df.to_csv(output_csv, index=False)
print(f"Done! Created combined CSV: {output_csv}")
