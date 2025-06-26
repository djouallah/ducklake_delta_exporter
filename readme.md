DuckLake Delta Exporter
A Python utility to synchronize metadata from a DuckLake database with Delta Lake transaction logs. This allows you to manage data in DuckLake and make it discoverable and queryable by Delta Lake compatible tools (e.g., Spark, Delta Lake Rust/Python clients).

Features
DuckLake to Delta Sync: Generates incremental Delta Lake transaction logs (_delta_log/*.json) and checkpoint files (_delta_log/*.checkpoint.parquet) based on the latest state of tables in a DuckLake database.

Schema Mapping: Automatically maps DuckDB data types to their Spark SQL equivalents for Delta Lake schema definitions.

Change Detection: Identifies added and removed data files since the last Delta export, ensuring only necessary updates are written to the log.

Checkpointing: Supports creating Delta Lake checkpoint files at a configurable interval for efficient state reconstruction.

Installation
You can install this package using pip:

pip install ducklake-delta-exporter

Alternatively, if you download the source code:

git clone https://github.com/yourusername/ducklake-delta-exporter.git
cd ducklake-delta-exporter
pip install .

Usage
generate_latest_delta_log function
This function is the main entry point for exporting your DuckLake metadata to Delta Lake.

from ducklake_delta_exporter import generate_latest_delta_log

# Example usage:
# Assuming your DuckLake database is at 'path/to/your/ducklake.db'
# and your data root for the lakehouse is '/lakehouse/default/Tables'
# where your actual parquet files are stored relative to this root.

db_path = 'path/to/your/ducklake.db'
data_root = '/lakehouse/default/Tables' # This should be the root where your schema/table directories are

# Generate Delta logs for all tables in the DuckLake database
# Checkpoints will be created every 1 version by default
generate_latest_delta_log(db_path, data_root=data_root, checkpoint_interval=1)

# To generate checkpoints less frequently (e.g., every 10 versions)
# generate_latest_delta_log(db_path, data_root=data_root, checkpoint_interval=10)

Important Notes:

db_path: This is the path to your DuckDB database file which contains the ducklake_table, ducklake_schema, ducklake_column, and ducklake_data_file tables.

data_root: This path specifies the root directory where your actual Parquet data files are stored. The _delta_log directories will be created inside data_root/{schema_path}/{table_path}/_delta_log. Ensure this matches your actual data storage layout.

Idempotency: The generate_latest_delta_log function is designed to be idempotent. It checks the existing Delta log to avoid re-exporting the same DuckLake snapshot.

Project Structure
ducklake-delta-exporter/
├── ducklake_delta_exporter/
│   └── __init__.py         # Contains the core functions
├── README.md
├── setup.py                # Defines how to package the project
└── requirements.txt        # Lists Python dependencies

Development
Clone the repository:

git clone https://github.com/yourusername/ducklake-delta-exporter.git
cd ducklake-delta-exporter

Create a virtual environment (recommended):

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

Install dependencies:

pip install -e .

The -e flag installs the package in editable mode, meaning any changes you make to the source code will be reflected without needing to reinstall.

Contributing
Feel free to open issues or submit pull requests if you have improvements or bug fixes.