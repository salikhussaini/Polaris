Certainly! Below are brief tutorials for each ETL operation using both Pandas and Polars in Python:

### 1. Extract from CSV:

#### Pandas:
```python
import pandas as pd

# Extract: Read data from CSV
df = pd.read_csv('data.csv')
print(df.head())
```

#### Polars:
```python
import polars as pl

# Extract: Read data from CSV
df = pl.read_csv('data.csv')
print(df)
```

### 2. Transform - Filter and Modify:

#### Pandas:
```python
# Transform: Filter and modify data
df_filtered = df[df['sales'] > 1000]
df_transformed = df_filtered[['name', 'sales']].rename(columns={'sales': 'revenue'})
print(df_transformed.head())
```

#### Polars:
```python
# Transform: Filter and modify data
df_filtered = df.filter(df['sales'] > 1000)
df_transformed = df_filtered.select(['name', 'sales']).alias({'sales': 'revenue'})
print(df_transformed)
```

### 3. Load - Write to CSV:

#### Pandas:
```python
# Load: Write transformed data to CSV
df_transformed.to_csv('transformed_data.csv', index=False)
```

#### Polars:
```python
# Load: Write transformed data to CSV
df_transformed.write_csv('transformed_data.csv')
```

### 4. Merge DataFrames:

#### Pandas:
```python
# Transform: Merge DataFrames
df_additional = pd.read_csv('additional_data.csv')
df_merged = pd.merge(df_transformed, df_additional, on='id', how='inner')
print(df_merged.head())
```

#### Polars:
```python
# Transform: Merge DataFrames
df_additional = pl.read_csv('additional_data.csv')
df_merged = df_transformed.join(df_additional, on='id', how='inner')
print(df_merged)
```

### 5. Group By and Aggregate:

#### Pandas:
```python
# Transform: Group by and aggregate
df_grouped = df.groupby('category')['revenue'].sum().reset_index()
print(df_grouped.head())
```

#### Polars:
```python
# Transform: Group by and aggregate
df_grouped = df.groupby('category').agg(pl.col('revenue').sum().alias('total_revenue'))
print(df_grouped)
```

### 6. Extract from Database:

#### Pandas:
```python
# Extract: Read data from a relational database
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:password@localhost:5432/database')
df = pd.read_sql_query('SELECT * FROM table', engine)
print(df.head())
```

#### Polars:
```python
# Extract: Read data from a relational database
import polars as pl

df = pl.read_sql('postgresql://user:password@localhost:5432/database', 'SELECT * FROM table')
print(df)
```

### 7. Transform - Handle Missing Values:

#### Pandas:
```python
# Transform: Handle missing values
df_filled = df.fillna(0)
print(df_filled.head())
```

#### Polars:
```python
# Transform: Handle missing values
df_filled = df.fill_none(0)
print(df_filled)
```

### 8. Load - Insert into Database:

#### Pandas:
```python
# Load: Insert transformed data into a relational database
df_transformed.to_sql('table', engine, index=False, if_exists='replace')
```

#### Polars:
```python
# Load: Insert transformed data into a relational database
df_transformed.write_sql('postgresql://user:password@localhost:5432/database', 'table', 'replace')
```

### 9. Deduplicate Data:

#### Pandas:
```python
# Transform: Deduplicate data
df_deduplicated = df.drop_duplicates()
print(df_deduplicated.head())
```

#### Polars:
```python
# Transform: Deduplicate data
df_deduplicated = df.drop_duplicates()
print(df_deduplicated)
```

### 10. Transform - Convert Data Types:

#### Pandas:
```python
# Transform: Convert data types
df['date'] = pd.to_datetime(df['date'])
print(df.dtypes)
```

#### Polars:
```python
# Transform: Convert data types
df = df.with_column(pl.col('date').cast(pl.Date))
print(df.dtypes())
```

These tutorials provide examples of common ETL operations using both Pandas and Polars in Python.
