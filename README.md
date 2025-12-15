# PySpark Script generator

A simplified service similar to AWS Glue Studio for generating PySpark ETL scripts based on JSON configurations.
- Receive ETL (PySpark) definitions in JSON format
- Automatically generate PySpark scripts
- Save the script to Kubernetes ConfigMap
- Allows editing ETL jobs (PySpark) without writing PySpark code via the PATCH mechanism.

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
    - type (postgresql)
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

## API References
### 1. POST /generate

Create pyspark script for the first time also creating kubernetes configmap
```json
{
  "jobName": "new-feature",
  "createConfigMap": true,
  "source": {
    "type": "s3_compatible",
    "access_key": "ACCESSKEY",
    "secret_key": "SECRET",
    "bucket": "dummycsv",
    "format": "csv",
    "path": "dirty_lifestyle_health.csv",
    "endpoint_url": "https://enpoint.com"
  },
  "transforms": [
    {
      "type": "select",
      "parameters": {
        "columns": ["Age", "Gender", "Smoking", "BMI", "SleepHours"]
      }
    },
    {
      "type": "fill_na",
      "parameters": {
        "fill_value": "'unknown'",
        "columns": ["Smoking", "Gender"]
      }
    },
    {
      "type": "fill_na",
      "parameters": {
        "fill_value": 0,
        "columns": ["Age","BMI", "SleepHours"]
      }
    }
  ],
  "target": {
    "type": "s3_compatible",
    "access_key": "ACCESSKEY",
    "secret_key": "SECRET",
    "bucket": "dummycsv",
    "format": "csv",
    "path": "analytics/demo-result.csv",
    "endpoint_url": "https://enpoint.com"
  }
}
```
Response 
```json
{
    "script": "<pyspark script>",
    "configMapName": "new-feature",
    "namespace": "spark",
    "configMapStatus": "Created/Updated successfully"
}
```
### 2. GET /jobs

List all created configmap and pyspark script
```json
[
    {
        "name": "new-feature",
        "namespace": "spark",
        "labels": { ... },
        "annotations": { ... }
    }
]
```
### 3. GET /job/{job-name}

Mengambil:
- job definition (annotation)
- pyspark script
- metadata
```json
{
    "name": "new-feature",
    "namespace": "spark",
    "labels": { ... },
    "annotations": { ... },
    "script": "<pyspark script>"
}
```

### 4. PATCH /job/{job-name}
Patch job definition (JSON) + regenerate script + patch ConfigMap. Sopported to edit 
- source
- transforms
- target

Example to add trnasformations 
```json
{
  "transforms": [
    {
      "type": "select",
      "parameters": {
        "columns": ["Age", "Gender", "Smoking", "BMI", "SleepHours"]
      }
    },
    {
      "type": "fill_na",
      "parameters": {
        "fill_value": "'unknown'",
        "columns": ["Smoking", "Gender"]
      }
    },
    {
      "type": "fill_na",
      "parameters": {
        "fill_value": 0,
        "columns": ["Age","BMI", "SleepHours"]
      }
    },
    {
      "type": "order_by",
      "parameters": {
        "columns": [
          { "column": "Age", "order": "desc" }
        ]
      }
    }
  ]
}
```
**Note**: If the field is an array (transforms), then PATCH will replace the entire array, not append.


## Catatan Desain
- PySpark script adalah output, bukan input
- Source of truth adalah job-definition JSON