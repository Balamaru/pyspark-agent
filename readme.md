# PySpark Script generator

A simplified service similar to AWS Glue Studio for generating PySpark ETL scripts based on JSON configurations.

## Features

### Supported Data Sources

1. **Amazon S3**
    - type (s3)
    - access_key
    - secret_key
    - bucket
    - format
    - path

2. **S3 Compatible Storage** (MinIO, etc.)
    - type (s3_compatible)
    - access_key
    - secret_key
    - bucket
    - format
    - path
    - endpoint_url

3. **PostgreSQL**
    - type (postgresql0
    - host
    - port
    - database
    - table
    - user
    - password


### Supported Transformations

1. **Filter**
    - Data filtering
    - Parameter: condition (string)

2. **Select**
    - Select specific columns from the dataset
    - Parameter: columns (array string)

3. **Rename Column**
    - Rename columns with a mapping of old names to new names
    - Parameter: old_name, new_name (string)

4. **Drop Column**
    - Remove specified columns from the dataset
    - Parameter: columns (array string)

5. **Add Column**
    - Add a new column based on a PySpark expression
    - Parameter: column_name, expression (string)

6. **Join**
    - Join with another table
    - Parameter: join_type, right_table, join_condition (string)

7. **Group By + Aggregations**
    - Group data by agregation
    - Parameter: group_by (array), aggregations (object)

8. **SQL Transform**
    - Using SQL query
    - Parameter: temp_view (table_name), query (string)

9. **Cast Column**
    - Change data type of columns
    - Parameter: column_name, data_type (string)

10. **Fill NA**
    - Fill null value
    - Parameter: fill_value, columns (optional array)

11. **Drop Duplicates**
    - Remove duplicate
    - Parameter: columns (optional array)

12. **Order By**
    - Sorting data
    - Parameter: columns (array dengan column dan order)

### Supported Targets

Same as data sources:
- Amazon S3
- S3 Compatible Storage
- PostgreSQL

## Getting Started

### Prerequisites

- Go 1.22 or later

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/Balamaru/pyspark-agent.git
   cd pyspark-agent
   ```

2. Build the application:
   ```
   go build
   ```

### Running the Application

```
./pyspark-agent
```

The server will start on port 8080 by default.
