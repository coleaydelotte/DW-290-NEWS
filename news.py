import os
import zipfile
import duckdb
import shutil
import re
import glob
import subprocess

# Configuration
GITHUB_REPO = input("Enter GitHub repository URL: ").strip()
REPO_NAME = GITHUB_REPO.rstrip('/').split('/')[-1].replace('.git', '')
data_dir = f"./{REPO_NAME}/Datasets"
extract_root = "./unzipped_all"
flat_dir = "./unzipped_flat"
output_csv = "all_articles_summary.csv"

# Clean up previous run artifacts
print("Cleaning up previous files...")
for path in [extract_root, flat_dir, output_csv]:
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
            print(f"Removed directory: {path}")
        else:
            os.remove(path)
            print(f"Removed file: {path}")

# # Clone or update the repository
if os.path.exists(REPO_NAME):
    print(f"Repository directory '{REPO_NAME}' exists. Removing and re-cloning...")
    shutil.rmtree(REPO_NAME)

print(f"Cloning repository from {GITHUB_REPO}...")
try:
    subprocess.run(["git", "clone", GITHUB_REPO], check=True)
    print("Repository cloned successfully.")
except subprocess.CalledProcessError as e:
    raise RuntimeError(f"Failed to clone repository: {e}")

# # Verify data directory exists
if not os.path.exists(data_dir):
    raise FileNotFoundError(f"Expected data directory not found: {data_dir}")

# # Extracts the files into their own directory
print("Extracting ZIP files...")
os.makedirs(extract_root, exist_ok=True)
os.makedirs(flat_dir, exist_ok=True)

# Find all ZIPs
zip_files = glob.glob(os.path.join(data_dir, "*.zip"))
if not zip_files:
    raise FileNotFoundError(f"No ZIP files found in {data_dir}")

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

# Loads extracted and renamed files into duckdb
con = duckdb.connect()

query = f"""
SELECT
    json_extract_string(data, '$.uuid') AS UUID,
    json_extract_string(data, '$.url') AS URL,
    json_extract_string(data, '$.author') AS Author,
    json_extract_string(data, '$.title') AS Title,
    -- REMOVED: Text (save tons of space)
    json_extract_string(data, '$.published') AS Published,
    json_extract_string(data, '$.language') AS Language,
    json_extract_string(data, '$.rating') AS Rating,

    -- Thread fields
    json_extract_string(data, '$.thread.title') AS ThreadTitle,
    json_extract_string(data, '$.thread.url') AS ThreadURL,
    json_extract_string(data, '$.thread.site_full') AS SiteFull,
    json_extract_string(data, '$.thread.site') AS Site,
    json_extract_string(data, '$.thread.site_section') AS SiteSection,
    json_extract_string(data, '$.thread.section_title') AS SectionTitle,
    json_extract_string(data, '$.thread.published') AS ThreadPublished,
    json_extract_string(data, '$.thread.country') AS Country,
    json_extract_string(data, '$.thread.domain_rank') AS DomainRank,
    json_extract_string(data, '$.thread.replies_count') AS RepliesCount,
    json_extract_string(data, '$.thread.participants_count') AS Participants,
    json_extract_string(data, '$.thread.site_type') AS SiteType,

    filename AS SourceFile

FROM read_json_auto(
    '{flat_dir}/*.json',
    sample_size=-1,
    ignore_errors=true,
    union_by_name=true,
    maximum_depth=12,
    filename=true
) AS data;
"""

print("Reading JSON files with DuckDB...")
df = con.execute(query).df()

# Writes the queried data to a CSV
df.to_csv(output_csv, index=False)
print(f"Done! Created combined CSV: {output_csv}")