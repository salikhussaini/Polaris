Certainly! Here are 10 more examples of ETL (Extract, Transform, Load) operations:

21. **Extract from API - Pandas:**
    - Fetch data from a RESTful API and load it into a Pandas DataFrame.

    ```python
    # Extract: Fetch data from API
    import requests
    response = requests.get('https://api.example.com/data')
    data_json = response.json()
    df_api = pd.DataFrame(data_json)
    ```

    **Polars:**
    ```python
    # Extract: Fetch data from API
    response = requests.get('https://api.example.com/data')
    data_json = response.json()
    df_api = pl.DataFrame(data_json)
    ```

22. **Transform - Calculate Moving Average - Pandas:**
    - Compute the moving average of a numeric column.

    ```python
    # Transform: Calculate moving average
    df['moving_avg'] = df['value'].rolling(window=3).mean()
    ```

    **Polars:**
    ```python
    # Transform: Calculate moving average
    df = df.with_column(pl.col('value').rolling_window(3).mean().alias('moving_avg'))
    ```

23. **Load - Insert into Elasticsearch - Pandas:**
    - Insert transformed data into an Elasticsearch index.

    ```python
    # Load: Insert data into Elasticsearch
    from elasticsearch import Elasticsearch
    es = Elasticsearch()
    df_transformed.to_dict('records')
    es.bulk(index='index_name', body=df_transformed.to_dict('records'))
    ```

    **Polars:**
    ```python
    # Load: Insert data into Elasticsearch
    df_transformed.write_es('index_name', hosts=['http://localhost:9200'])
    ```

24. **Transform - Apply String Concatenation - Pandas:**
    - Concatenate two string columns.

    ```python
    # Transform: String concatenation
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    ```

    **Polars:**
    ```python
    # Transform: String concatenation
    df = df.with_column((pl.col('first_name') + ' ' + pl.col('last_name')).alias('full_name'))
    ```

25. **Load - Insert into SQLite Database - Pandas:**
    - Insert transformed data into an SQLite database.

    ```python
    # Load: Insert data into SQLite
    import sqlite3
    conn = sqlite3.connect('database.db')
    df_transformed.to_sql('table_name', conn, index=False, if_exists='replace')
    ```

    **Polars:**
    ```python
    # Load: Insert data into SQLite
    df_transformed.write_sqlite('sqlite://database.db::table_name', mode='replace')
    ```

26. **Transform - Normalize Data - Pandas:**
    - Normalize numeric columns in the dataset.

    ```python
    # Transform: Normalize data
    df_normalized = (df - df.min()) / (df.max() - df.min())
    ```

    **Polars:**
    ```python
    # Transform: Normalize data
    df_normalized = (df - df.min()) / (df.max() - df.min())
    ```

27. **Extract from Parquet File - Pandas:**
    - Read data from a Parquet file into a Pandas DataFrame.

    ```python
    # Extract: Read data from Parquet
    df_parquet = pd.read_parquet('data.parquet')
    ```

    **Polars:**
    ```python
    # Extract: Read data from Parquet
    df_parquet = pl.read_parquet('data.parquet')
    ```

28. **Transform - Calculate Age from Birthdate - Pandas:**
    - Calculate age from a birthdate column.

    ```python
    # Transform: Calculate age
    df['age'] = (pd.to_datetime('today') - pd.to_datetime(df['birthdate'])).astype('<m8[Y]')
    ```

    **Polars:**
    ```python
    # Transform: Calculate age
    df = df.with_column(((pl.col('birthdate').cast(pl.Date)).year() - pl.datetime('today').year()).alias('age'))
    ```

29. **Load - Insert into Amazon Redshift - Pandas:**
    - Insert transformed data into an Amazon Redshift database.

    ```python
    # Load: Insert data into Redshift
    from sqlalchemy import create_engine
    engine = create_engine('redshift+psycopg2://user:password@hostname:5439/database')
    df_transformed.to_sql('table_name', engine, index=False, if_exists='replace')
    ```

    **Polars:**
    ```python
    # Load: Insert data into Redshift
    df_transformed.write_sql('postgresql://user:password@hostname:5439/database', 'table_name', mode='replace')
    ```

30. **Transform - Apply Log Transformation - Pandas:**
    - Apply a log transformation to a numeric column.

    ```python
    # Transform: Apply log transformation
    df['log_value'] = np.log1p(df['value'])
    ```

    **Polars:**
    ```python
    # Transform: Apply log transformation
    df = df.with_column(pl.col('value').apply(pl.ln1p).alias('log_value'))
    ```

These examples cover a variety of ETL operations using both Pandas and Polars in Python, showcasing their versatility in data manipulation and processing tasks.
