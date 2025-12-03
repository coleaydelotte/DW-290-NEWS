import pandas as pd

def get_unique_values(series):
    vals = series.dropna().unique()
    return sorted(list(vals))

print("=" * 80)
print("DATA WRANGLING PROCESS - FAKE NEWS ARTICLES ANALYSIS")
print("=" * 80)
print()

# Step 1: Load the data
print("STEP 1: Loading data from 'all_articles_summary.csv'")
print("-" * 80)
data = pd.read_csv('all_articles_summary.csv')
print(f"✓ Data loaded successfully")
print()

# Step 2: Initial data exploration
print("STEP 2: Initial data overview - First 5 rows")
print("-" * 80)
print(data.head())
print()

print("STEP 3: Dataset dimensions")
print("-" * 80)
print(f"Shape: {data.shape[0]} rows × {data.shape[1]} columns")
print()

# Step 4: Data quality checks
print("STEP 4: Data quality check - Missing titles")
print("-" * 80)
missing_titles = data["Title"].isnull().sum()
print(f"Number of missing titles: {missing_titles}")
print()

# Step 5: Language distribution
print("STEP 5: Language distribution analysis")
print("-" * 80)
print("Count of articles by language:")
print(data["Language"].value_counts())
print()

# Step 6: Domain rank distribution
print("STEP 6: Domain rank distribution")
print("-" * 80)
print("Count of articles by domain rank:")
print(data["DomainRank"].value_counts())
print()

# Step 7: Site distribution
print("STEP 7: Site distribution analysis")
print("-" * 80)
print("Count of articles by site:")
print(data['Site'].value_counts())
print()

# Step 8: Author distribution
print("STEP 8: Author distribution analysis")
print("-" * 80)
print("Count of articles by author:")
print(data['Author'].value_counts())
print()

# Step 9: Convert numeric columns
print("STEP 9: Converting Participants and RepliesCount to numeric")
print("-" * 80)
data['Participants'] = pd.to_numeric(data['Participants'], errors='coerce')
data['RepliesCount'] = pd.to_numeric(data['RepliesCount'], errors='coerce')
print("✓ Participants and RepliesCount converted to numeric")
print("Average participants per site (sorted descending):")
participants_by_site = data.groupby('Site')['Participants'].mean().sort_values(ascending=False)
print(participants_by_site.head(10))
print()

# Step 9b: Parse Published date column
print("STEP 9b: Parsing Published date column to datetime")
print("-" * 80)
data['Published'] = pd.to_datetime(data['Published'], errors='coerce', utc=True)
print(f"✓ Published column converted to datetime")
print(f"Number of valid dates: {data['Published'].notna().sum()}")
print()

# Step 9c: Create date-related variables
print("STEP 9c: Creating date-related variables")
print("-" * 80)
data['Published_Year'] = data['Published'].dt.year
data['Published_Month'] = data['Published'].dt.month
data['Published_Day'] = data['Published'].dt.day
data['Published_Date'] = data['Published'].dt.normalize()
data['Published_DayOfWeek'] = data['Published'].dt.day_name()
data['Published_Week'] = data['Published'].dt.isocalendar().week
data['Published_Quarter'] = data['Published'].dt.quarter
print("✓ Created date variables: Published_Year, Published_Month, Published_Day, Published_Date, Published_DayOfWeek, Published_Week, Published_Quarter")
print("Sample of date variables:")
print(data[['Published', 'Published_Year', 'Published_Month', 'Published_Day', 
            'Published_Date', 'Published_DayOfWeek', 'Published_Week', 'Published_Quarter']].head())
print()

# Step 9d: Create additional derived variables
print("STEP 9d: Creating additional derived variables")
print("-" * 80)
# Article engagement score (combination of participants and replies)
data['Engagement_Score'] = (data['Participants'].fillna(0) * 0.6 + 
                            data['RepliesCount'].fillna(0) * 0.4)

# High engagement flag
engagement_threshold = data['Engagement_Score'].quantile(0.75)
data['High_Engagement'] = data['Engagement_Score'] > engagement_threshold

# Date-based variables
data['Days_Since_Published'] = (pd.Timestamp.now(tz='UTC') - data['Published']).dt.days
data['Is_Weekend'] = data['Published_DayOfWeek'].isin(['Saturday', 'Sunday'])

# Language-English flag (handle NaN values)
data['Is_English'] = data['Language'].str.lower().fillna('') == 'english'

print("✓ Created derived variables: Engagement_Score, High_Engagement, Days_Since_Published, Is_Weekend, Is_English")
print(f"Engagement_Score statistics:")
print(data['Engagement_Score'].describe())
print(f"\nHigh_Engagement: {data['High_Engagement'].sum()} articles ({data['High_Engagement'].sum()/len(data)*100:.2f}%)")
print(f"Is_Weekend: {data['Is_Weekend'].sum()} articles ({data['Is_Weekend'].sum()/len(data)*100:.2f}%)")
print(f"Is_English: {data['Is_English'].sum()} articles ({data['Is_English'].sum()/len(data)*100:.2f}%)")
print("Sample of new variables:")
print(data[['Engagement_Score', 'High_Engagement', 'Days_Since_Published', 
            'Is_Weekend', 'Is_English']].head(10))
print()

# Step 10: Missing values analysis
print("STEP 10: Missing values analysis - Count by column")
print("-" * 80)
missing_by_column = data.isna().sum().sort_values(ascending=False)
print("Missing values per column:")
print(missing_by_column)
print()

print("STEP 11: Rows with any missing values")
print("-" * 80)
rows_with_missing = data[data.isna().any(axis=1)]
print(f"Number of rows with at least one missing value: {len(rows_with_missing)}")
print("Sample of rows with missing values:")
print(rows_with_missing.head(10))
print()

print("STEP 12: Rows with more than one missing value")
print("-" * 80)
rows_multiple_missing = data[data.isna().sum(axis=1) > 1]
print(f"Number of rows with more than one missing value: {len(rows_multiple_missing)}")
print("Sample of rows with multiple missing values:")
print(rows_multiple_missing.head(10))
print()

# Step 13: Language-based fake news count
print("STEP 13: Fake news count by language")
print("-" * 80)
count_by_language = data.groupby("Language")["URL"].count().reset_index(name="FakeNewsCount").sort_values("FakeNewsCount", ascending=False)
print("Fake news articles count by language:")
print(count_by_language)
print()

# Step 14: Author-country analysis
print("STEP 14: Author-country relationship analysis")
print("-" * 80)
authors_countries = data.groupby("Author")["Country"].apply(get_unique_values).reset_index(name="CountriesPublishedIn")
print("Authors and their countries (first 10):")
print(authors_countries.head(10))
print()
print("Distribution of number of countries per author:")
print(authors_countries["CountriesPublishedIn"].value_counts().head(10))
print()

# Step 15: Author-language analysis
print("STEP 15: Author-language relationship analysis")
print("-" * 80)
authors_languages = data.groupby("Author")["Language"].apply(get_unique_values).reset_index(name="LanguagesUsed")
print("Authors and their languages (first 10):")
print(authors_languages.head(10))
print()
print("Distribution of number of languages per author:")
print(authors_languages["LanguagesUsed"].value_counts().head(10))
print()

# Step 16: Site-based fake news count
print("STEP 16: Fake news count by site")
print("-" * 80)
count_by_site = data.groupby("Site")["URL"].count().reset_index(name="FakeNewsCount").sort_values("FakeNewsCount", ascending=False)
print("Fake news articles count by site (top sites):")
print(count_by_site.head(20))
print()

# Step 17: Final dataset summary
print("STEP 17: Final dataset summary with all new columns")
print("-" * 80)
print(f"Final dataset shape: {data.shape[0]} rows × {data.shape[1]} columns")
print("\nAll columns in final dataset:")
print(data.columns.tolist())
print("\nNew columns created during wrangling:")
new_columns = ['Published_Year', 'Published_Month', 'Published_Day', 'Published_Date', 
               'Published_DayOfWeek', 'Published_Week', 'Published_Quarter',
               'Engagement_Score', 'High_Engagement', 'Days_Since_Published', 
               'Is_Weekend', 'Is_English']
print(new_columns)
print("\nSample of final dataset with new columns:")
print(data[['URL', 'Title', 'Author', 'Site', 'Language', 'Country', 
            'Published_Year', 'Published_Month', 'Published_DayOfWeek',
            'Engagement_Score', 'High_Engagement', 'Is_Weekend', 'Is_English']].head(10))
print()

print("=" * 80)
print("DATA WRANGLING PROCESS COMPLETE")
print("=" * 80)
