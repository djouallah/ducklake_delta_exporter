# DuckLake Delta Exporter

A Python package for exporting DuckLake snapshots as Delta Lake checkpoint files, enabling compatibility with Delta Lake readers, support local path, s3 and gcs, for onelake use mounted storage as azure storage is not supported

this is just a fun project, please vote for a proper support in duckdb https://github.com/duckdb/duckdb-delta/issues/218

## Repository

https://github.com/djouallah/ducklake_delta_exporter

## Installation

```bash
pip install ducklake-delta-exporter
```

## Usage

```python
from ducklake_delta_exporter import generate_latest_delta_log

# Export all tables from a DuckLake database
generate_latest_delta_log("/path/to/ducklake.db")

# Specify a custom data root directory
generate_latest_delta_log("/path/to/ducklake.db", data_root="/custom/data/path")
```

## What it does

This package converts DuckLake table snapshots into Delta Lake format by:

1. **Reading DuckLake metadata** - Extracts table schemas, file paths, and snapshot information
2. **Creating Delta checkpoint files** - Generates `.checkpoint.parquet` files with Delta Lake metadata
3. **Writing JSON transaction logs** - Creates minimal `.json` log files for Spark compatibility
4. **Mapping data types** - Converts DuckDB types to Spark SQL equivalents

## Features

- ✅ **Spark Compatible** - Generated Delta files can be read by Spark and other Delta Lake tools
- ✅ **Type Mapping** - Automatic conversion between DuckDB and Spark data types
- ✅ **Batch Processing** - Exports all tables in a DuckLake database
- ✅ **Error Handling** - Graceful handling of missing snapshots and other issues
- ✅ **Progress Reporting** - Clear feedback on export progress and results

## Requirements

- Python 3.8+
- DuckDB

## File Structure

After running the exporter, your Delta tables will have the following structure:

```
your_table/
├── data_file_1.parquet
├── data_file_2.parquet
└── _delta_log/
    ├── 00000000000000000000.json
    ├── 00000000000000000000.checkpoint.parquet
    └── _last_checkpoint
```

## Type Mapping

The exporter automatically maps DuckDB types to Spark SQL types:

| DuckDB Type | Spark Type |
|-------------|------------|
| INTEGER     | integer    |
| BIGINT      | long       |
| FLOAT       | double     |
| DOUBLE      | double     |
| DECIMAL     | decimal(10,0) |
| BOOLEAN     | boolean    |
| TIMESTAMP   | timestamp  |
| DATE        | date       |
| VARCHAR     | string     |
| Others      | string     |

## Error Handling

The exporter handles various error conditions:

- **Missing snapshots** - Skips tables with no data
- **Existing checkpoints** - Avoids overwriting existing files
- **Schema changes** - Uses the latest schema for each table
- **File system errors** - Reports and continues with other tables

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.