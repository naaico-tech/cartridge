# BigQuery Connection Examples

This document provides examples of how to connect to Google BigQuery using the cartridge scanner.

## Authentication Methods

### 1. Service Account Key (JSON)

```python
bigquery_config = {
    "type": "bigquery",
    "project_id": "your-gcp-project-id",
    "dataset_id": "your_dataset_name",
    "location": "US",  # Optional, defaults to "US"
    "credentials_json": {
        "type": "service_account",
        "project_id": "your-gcp-project-id",
        "private_key_id": "key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
        "client_email": "service-account@your-gcp-project-id.iam.gserviceaccount.com",
        "client_id": "123456789",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account%40your-gcp-project-id.iam.gserviceaccount.com"
    }
}
```

### 2. Service Account Key (File Path)

```python
bigquery_config = {
    "type": "bigquery",
    "project_id": "your-gcp-project-id",
    "dataset_id": "your_dataset_name",
    "location": "US",
    "credentials_path": "/path/to/service-account-key.json"
}
```

### 3. Application Default Credentials (ADC)

```python
# When running on Google Cloud Platform or with gcloud auth application-default login
bigquery_config = {
    "type": "bigquery",
    "project_id": "your-gcp-project-id",
    "dataset_id": "your_dataset_name",
    "location": "US"
}
```

## CLI Usage

### Scan a BigQuery Dataset

```bash
# Using service account key file
cartridge scan "bigquery://your-gcp-project-id/your_dataset_name?credentials_path=/path/to/key.json"

# Using application default credentials
cartridge scan "bigquery://your-gcp-project-id/your_dataset_name"

# Scan specific tables
cartridge scan "bigquery://your-gcp-project-id/your_dataset_name" --tables="customers,orders,products"

# Export results to file
cartridge scan "bigquery://your-gcp-project-id/your_dataset_name" -o scan_results.json
```

### Generate dbt Models

```bash
# Generate dbt models from BigQuery scan
cartridge generate scan_results.json --output ./generated_dbt_project --project-name my_bigquery_project
```

## API Usage

### Test Connection

```python
import asyncio
from cartridge.scanner.bigquery import BigQueryConnector

async def test_bigquery_connection():
    config = {
        "type": "bigquery",
        "project_id": "your-gcp-project-id", 
        "dataset_id": "your_dataset_name",
        "credentials_path": "/path/to/service-account-key.json"
    }
    
    connector = BigQueryConnector(config)
    result = await connector.test_connection()
    print(f"Connection status: {result['status']}")
    print(f"Message: {result['message']}")

# Run the test
asyncio.run(test_bigquery_connection())
```

### Scan Schema

```python
import asyncio
from cartridge.scanner.bigquery import BigQueryConnector

async def scan_bigquery_schema():
    config = {
        "type": "bigquery",
        "project_id": "your-gcp-project-id",
        "dataset_id": "your_dataset_name",
        "credentials_path": "/path/to/service-account-key.json"
    }
    
    connector = BigQueryConnector(config)
    
    # Perform full schema scan
    scan_result = await connector.scan_schema(
        include_sample_data=True,
        sample_size=100
    )
    
    print(f"Database: {scan_result.database_info.database_name}")
    print(f"Tables found: {len(scan_result.tables)}")
    
    for table in scan_result.tables:
        print(f"\nTable: {table.name}")
        print(f"  Type: {table.table_type}")
        print(f"  Rows: {table.row_count}")
        print(f"  Size: {table.size_bytes} bytes")
        print(f"  Columns: {len(table.columns)}")
        
        # Show partitioning/clustering info
        if table.indexes:
            for index in table.indexes:
                print(f"  {index.type}: {', '.join(index.columns)}")

# Run the scan
asyncio.run(scan_bigquery_schema())
```

## BigQuery-Specific Features

### Supported Data Types

The BigQuery connector maps BigQuery data types to standard types:

| BigQuery Type | Standard Type | Notes |
|---------------|---------------|-------|
| INTEGER, INT64 | BIGINT | |
| FLOAT, FLOAT64 | DOUBLE | |
| NUMERIC, DECIMAL | NUMERIC | High precision |
| BIGNUMERIC, BIGDECIMAL | NUMERIC | Very high precision |
| STRING | TEXT | |
| BYTES | BINARY | |
| DATE | DATE | |
| TIME | TIME | |
| DATETIME | TIMESTAMP | |
| TIMESTAMP | TIMESTAMPTZ | With timezone |
| BOOLEAN, BOOL | BOOLEAN | |
| JSON | JSON | |
| GEOGRAPHY | TEXT | Special handling needed |
| STRUCT/RECORD | JSON | Flattened to JSON |
| ARRAY (REPEATED) | ARRAY | |
| RANGE | TEXT | BigQuery specific |
| INTERVAL | INTERVAL | |

### Partitioning and Clustering

The connector detects and reports BigQuery partitioning and clustering as index-like structures:

- **Time Partitioning**: Reported as `time_partition` index
- **Range Partitioning**: Reported as `range_partition` index  
- **Clustering**: Reported as `clustering` index

### Nested and Repeated Fields

- **STRUCT/RECORD fields**: Mapped to JSON type with full structure in raw_type
- **REPEATED fields**: Mapped to ARRAY type
- **Nested structures**: Flattened column names with proper type mapping

## Configuration Reference

### Required Parameters

- `type`: Must be "bigquery"
- `project_id`: Google Cloud Project ID
- `dataset_id`: BigQuery dataset name (can also use `database` as alias)

### Optional Parameters

- `location`: BigQuery location (default: "US")
- `credentials_json`: Service account key as JSON object or string
- `credentials_path`: Path to service account key file

### Authentication Priority

1. `credentials_json` (service account key as JSON)
2. `credentials_path` (service account key file)
3. Application Default Credentials (ADC)

## Error Handling

Common errors and solutions:

### Authentication Errors
- **Invalid credentials**: Check service account key format and permissions
- **Project not found**: Verify project ID and access permissions
- **Dataset not found**: Confirm dataset exists and access permissions

### Permission Errors
Required BigQuery permissions:
- `bigquery.datasets.get`
- `bigquery.tables.list`
- `bigquery.tables.get`
- `bigquery.tables.getData` (for sample data)

### Performance Considerations
- Large datasets: Consider using `include_sample_data=False` for faster scans
- Sample size: Adjust `sample_size` parameter based on needs
- Query costs: Sample data queries count toward BigQuery usage

## Best Practices

1. **Use service accounts** for production environments
2. **Limit permissions** to only required BigQuery resources
3. **Cache scan results** to avoid repeated queries
4. **Monitor BigQuery usage** as scanning generates billable queries
5. **Use specific dataset scoping** rather than scanning entire projects
