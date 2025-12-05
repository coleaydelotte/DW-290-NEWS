# DW-290-NEWS

## Collaborators

- Jeffery Eisenhardt: eisenhardtj
- Cole Aydelotte: coleaydelotte
- Harrison Krauss: kraussh-art

## How it works
This program uses DuckDB to combine all the json 
files into one CSV file that then feeds into pandas
where queries are ran.  
The DuckDB queries are the Author, title, the site where it originates from,
the published date, the country of origin, and language of the
article, and the categories from the website.

## Usage

### Step 1: Download the Data

To download and process the data from a GitHub repository, run `data.py`:

```bash
python data.py
```

When prompted, enter the GitHub repository URL containing the data. The script will:
- Clone the repository
- Extract ZIP files from the `Datasets` directory
- Process JSON files using DuckDB
- Create `all_articles_summary.csv` with the combined data

**Note:** Make sure you have the required dependencies installed:
- `duckdb`
- `pandas`

### Step 2: Run the Analysis

After downloading the data, run `news.py` to perform the data wrangling and analysis:

```bash
python news.py
```

This script will:
- Load the `all_articles_summary.csv` file
- Perform data quality checks and transformations
- Create derived variables (date fields, engagement scores, etc.)
- Generate analysis reports on language distribution, author patterns, site statistics, and more