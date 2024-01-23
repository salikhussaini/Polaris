Certainly! Here are simplified tutorials for each of the ETL operations using Pandas and Polaris in Python.

### 1. Extract from CSV using Pandas:

```python
import pandas as pd

# Extract from CSV
df = pd.read_csv('data.csv')

# Display the first few rows
print(df.head())
```

### 2. Transform - Filter and Modify using Pandas:

```python
# Transform - Filter and Modify
df_filtered = df[df['sales'] > 1000]
df_transformed = df_filtered[['name', 'sales']].rename(columns={'sales': 'revenue'})

# Display the transformed DataFrame
print(df_transformed.head())
```

### 3. Load - Write to CSV using Pandas:

```python
# Load - Write to CSV
df_transformed.to_csv('transformed_data.csv', index=False)
```

### 4. Merge DataFrames using Pandas:

```python
# Merge DataFrames
df_additional = pd.read_csv('additional_data.csv')
df_merged = pd.merge(df_transformed, df_additional, on='id', how='inner')

# Display the merged DataFrame
print(df_merged.head())
```

### 5. Group By and Aggregate using Pandas:

```python
# Group By and Aggregate
df_grouped = df.groupby('category')['revenue'].sum().reset_index()

# Display the aggregated DataFrame
print(df_grouped.head())
```

### 6. Extract from Database using Pandas:

```python
# Extract from Database
import pandas as pd
from sqlalchemy import create_engine

# Create a database connection
engine = create_engine('postgresql://username:password@localhost:5432/database_name')

# Extract data from the database
df_db = pd.read_sql_query('SELECT * FROM table_name', con=engine)

# Display the DataFrame
print(df_db.head())
```

### 7. Transform - Handle Missing Values using Pandas:

```python
# Transform - Handle Missing Values
df_filled = df.fillna(value=0)  # Fill missing values with 0
# Alternatively: df_dropped = df.dropna()  # Drop rows with missing values

# Display the transformed DataFrame
print(df_filled.head())
```

### 8. Load - Insert into Database using Pandas:

```python
# Load - Insert into Database
df_transformed.to_sql('table_name', con=engine, if_exists='replace', index=False)
```

### 9. Deduplicate Data using Pandas:

```python
# Deduplicate Data
df_deduplicated = df.drop_duplicates()

# Display the deduplicated DataFrame
print(df_deduplicated.head())
```

### 10. Transform - Convert Data Types using Pandas:

```python
# Transform - Convert Data Types
df['date_column'] = pd.to_datetime(df['date_column'])

# Display the DataFrame with converted data types
print(df.dtypes)
```

Please note that Polaris is primarily used for interactive data visualization and is not directly applicable to all ETL operations. If you have specific visualization requirements, please provide more details.
