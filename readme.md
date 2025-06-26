# DuckLake Delta Exporter
A Python utility to synchronize metadata from a DuckLake database with Delta Lake transaction logs. This allows you to manage data in DuckLake and make it discoverable and queryable by Delta Lake compatible tools (e.g., Spark, Delta Lake Rust/Python clients).

# Features
DuckLake to Delta Sync: Generates incremental Delta Lake transaction logs (_delta_log/*.json) and checkpoint files (_delta_log/*.checkpoint.parquet) based on the latest state of tables in a DuckLake database.

Schema Mapping: Automatically maps DuckDB data types to their Spark SQL equivalents for Delta Lake schema definitions.

Change Detection: Identifies added and removed data files since the last Delta export, ensuring only necessary updates are written to the log.

Checkpointing: Supports creating Delta Lake checkpoint files at a configurable interval for efficient state reconstruction.

# Installation
You can install this package using pip:

pip install ducklake-delta-exporter



# Usage
```
from ducklake_delta_exporter import generate_latest_delta_log
generate_latest_delta_log('path/to/your/ducklake.db', data_root='/lakehouse/default/Tables', checkpoint_interval=1)
```
