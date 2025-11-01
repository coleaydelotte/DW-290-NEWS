import os
import zipfile
import duckdb
import shutil
import re
import glob

data_dir = "./data"
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
        raise FileNotFoundError("No ZIP files found in ./data/")

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
    print(f" Flattened directory already exists — skipping extraction and renaming.")

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
