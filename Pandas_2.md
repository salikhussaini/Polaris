Certainly! Here are 10 more examples of ETL (Extract, Transform, Load) operations:

11. **Filter and Aggregate - Pandas:**
   - Filter data based on a condition and calculate the average of a numeric column.

    ```python
    # Transform: Filter and aggregate
    df_filtered = df[df['quantity'] > 10]
    avg_price = df_filtered.groupby('category')['price'].mean().reset_index()
    ```

    **Polars:**
    ```python
    # Transform: Filter and aggregate
    df_filtered = df.filter(df['quantity'] > 10)
    avg_price = df_filtered.groupby('category').agg(pl.col('price').mean().alias('avg_price'))
    ```

12. **Convert Text to Lowercase - Pandas:**
    - Convert all text in a column to lowercase.

    ```python
    # Transform: Convert text to lowercase
    df['product_name'] = df['product_name'].str.lower()
    ```

    **Polars:**
    ```python
    # Transform: Convert text to lowercase
    df = df.with_column(pl.col('product_name').apply(str.lower))
    ```

13. **Extract from JSON - Pandas:**
    - Read data from a JSON file into a Pandas DataFrame.

    ```python
    # Extract: Read data from JSON
    df_json = pd.read_json('data.json')
    ```

    **Polars:**
    ```python
    # Extract: Read data from JSON
    df_json = pl.read_json('data.json')
    ```

14. **Transform - Pivot Table - Pandas:**
    - Create a pivot table from a DataFrame.

    ```python
    # Transform: Create a pivot table
    pivot_table = pd.pivot_table(df, values='value', index='category', columns='date', aggfunc='sum')
    ```

    **Polars:**
    ```python
    # Transform: Create a pivot table
    pivot_table = df.pivot(pl.col('value')).groupby(['category', 'date']).agg(pl.col('value').sum().alias('sum_value'))
    ```

15. **Load - Append to Existing CSV - Pandas:**
    - Append new data to an existing CSV file.

    ```python
    # Load: Append data to existing CSV
    new_data.to_csv('existing_data.csv', mode='a', header=False, index=False)
    ```

    **Polars:**
    ```python
    # Load: Append data to existing CSV
    new_data.write_csv('existing_data.csv', mode='append')
    ```

16. **Transform - Calculate Percentile - Pandas:**
    - Calculate the 90th percentile of a numeric column.

    ```python
    # Transform: Calculate percentile
    percentile_90 = df['value'].quantile(0.9)
    ```

    **Polars:**
    ```python
    # Transform: Calculate percentile
    percentile_90 = df.agg(pl.col('value').quantile(0.9).alias('percentile_90'))
    ```

17. **Extract from Excel - Pandas:**
    - Read data from an Excel file into a Pandas DataFrame.

    ```python
    # Extract: Read data from Excel
    df_excel = pd.read_excel('data.xlsx')
    ```

    **Polars:**
    ```python
    # Extract: Read data from Excel
    df_excel = pl.read_excel('data.xlsx')
    ```

18. **Transform - Cumulative Sum - Pandas:**
    - Calculate the cumulative sum of a numeric column.

    ```python
    # Transform: Calculate cumulative sum
    df['cumulative_sum'] = df['value'].cumsum()
    ```

    **Polars:**
    ```python
    # Transform: Calculate cumulative sum
    df = df.with_column(pl.col('value').accumulate())
    ```

19. **Load - Insert into MongoDB - Pandas:**
    - Insert transformed data into a MongoDB collection.

    ```python
    # Load: Insert data into MongoDB
    df_transformed.to_dict('records')
    collection.insert_many(df_transformed.to_dict('records'))
    ```

    **Polars:**
    ```python
    # Load: Insert data into MongoDB
    df_transformed.write_mongo('mongodb://localhost:27017', 'database', 'collection', mode='replace')
    ```

20. **Transform - Apply Custom Function - Pandas:**
    - Apply a custom function to a column.

    ```python
    # Transform: Apply custom function
    df['processed_column'] = df['original_column'].apply(lambda x: custom_function(x))
    ```

    **Polars:**
    ```python
    # Transform: Apply custom function
    df = df.with_column(pl.col('original_column').apply(custom_function).alias('processed_column'))
    ```

These additional examples cover a range of ETL operations using both Pandas and Polars in Python.
