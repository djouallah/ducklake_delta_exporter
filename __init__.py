# File: ducklake_delta_exporter.py
import os
import json
import uuid
import time
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def map_type_ducklake_to_spark(t):
    """Maps DuckDB data types to their Spark SQL equivalents for the Delta schema."""
    t = t.lower()
    if 'int' in t:
        return 'long' if '64' in t else 'integer'
    elif 'float' in t:
        return 'double'
    elif 'double' in t:
        return 'double'
    elif 'decimal' in t:
        return 'decimal(10,0)'
    elif 'bool' in t:
        return 'boolean'
    elif 'timestamp' in t:
        return 'timestamp'
    elif 'date' in t:
        return 'date'
    return 'string'

def create_spark_schema_string(fields):
    """Creates a JSON string for the Spark schema from a list of fields."""
    return json.dumps({"type": "struct", "fields": fields})

def get_spark_checkpoint_schema():
    """Returns the PyArrow schema for a Delta Lake checkpoint file."""
    return pa.schema([
        pa.field("protocol", pa.struct([
            pa.field("minReaderVersion", pa.int32()),   # Made nullable
            pa.field("minWriterVersion", pa.int32())    # Made nullable
        ]), nullable=True),
        pa.field("metaData", pa.struct([
            pa.field("id", pa.string()),
            pa.field("name", pa.string()),
            pa.field("description", pa.string()),
            pa.field("format", pa.struct([
                pa.field("provider", pa.string()),
                pa.field("options", pa.map_(pa.string(), pa.string()))
            ])),
            pa.field("schemaString", pa.string()),
            pa.field("partitionColumns", pa.list_(pa.string())),
            pa.field("createdTime", pa.int64()),
            pa.field("configuration", pa.map_(pa.string(), pa.string()))
        ]), nullable=True),
        pa.field("add", pa.struct([
            pa.field("path", pa.string()),
            pa.field("partitionValues", pa.map_(pa.string(), pa.string())),
            pa.field("size", pa.int64()),
            pa.field("modificationTime", pa.int64()),
            pa.field("dataChange", pa.bool_()),
            pa.field("stats", pa.string(), nullable=True),
            pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=True)
            # Removed deletionVector, baseRowId, defaultRowCommitVersion, clusteringProvider
        ]), nullable=True),
        pa.field("remove", pa.struct([
            pa.field("path", pa.string()),
            pa.field("deletionTimestamp", pa.int64()),
            pa.field("dataChange", pa.bool_())
        ]), nullable=True),
        pa.field("commitInfo", pa.struct([
            pa.field("timestamp", pa.timestamp('ms'), False),  # Changed from pa.int64() to pa.timestamp('ms')
            pa.field("operation", pa.string()),
            pa.field("operationParameters", pa.map_(pa.string(), pa.string())),
            pa.field("isBlindAppend", pa.bool_(), nullable=True),
            pa.field("engineInfo", pa.string(), nullable=True),
            pa.field("clientVersion", pa.string(), nullable=True)
        ]), nullable=True)
    ])

def get_latest_delta_version_info(delta_log_path, con, table_id):
    """
    Determines the latest Delta version exported and reconstructs the set of files
    that were part of that Delta version, based on the embedded DuckLake snapshot ID.
    Also retrieves the consistent metaData.id if available from version 0.

    Returns (latest_delta_version, set_of_files_in_that_version, latest_ducklake_snapshot_id_in_delta, meta_id_from_delta_log).
    """
    last_delta_version_idx = -1
    last_exported_ducklake_snapshot_id = None
    files_in_last_delta_version = set()
    meta_id_from_delta_log = None # This should be consistent for the table

    # Collect all files ending with .json
    log_files = [f for f in os.listdir(delta_log_path) if f.endswith('.json')]
    
    if not log_files:
        return last_delta_version_idx, files_in_last_delta_version, last_exported_ducklake_snapshot_id, meta_id_from_delta_log

    try:
        # Collect valid version numbers from file names
        found_versions = []
        for f_name in log_files:
            base_name = f_name.split('.')[0]
            # Check if filename starts with '0000' and consists entirely of digits
            if base_name.startswith('0000') and base_name.isdigit():
                found_versions.append(int(base_name))

        if not found_versions:
            # No valid versioned log files found with the '0000' prefix
            return last_delta_version_idx, files_in_last_delta_version, last_exported_ducklake_snapshot_id, meta_id_from_delta_log

        # Get the highest version index
        last_delta_version_idx = max(found_versions)
        last_log_file = os.path.join(delta_log_path, f"{last_delta_version_idx:020d}.json")
        
        # Attempt to read the last log file for commitInfo and metaData (if present)
        with open(last_log_file, 'r') as f:
            for line in f:
                try:
                    action = json.loads(line)
                    if 'commitInfo' in action:
                        commit_info = action['commitInfo']
                        if 'operationParameters' in commit_info and 'duckLakeSnapshotId' in commit_info['operationParameters']:
                            last_exported_ducklake_snapshot_id = int(commit_info['operationParameters']['duckLakeSnapshotId'])
                    if 'metaData' in action:
                        meta_id_from_delta_log = action['metaData'].get('id')
                except json.JSONDecodeError as e:
                    print(f"ERROR: Failed to parse JSON line in {last_log_file}: {line.strip()}. Error: {e}")
                except Exception as e:
                    print(f"ERROR: Unexpected error processing line in {last_log_file}: {e}")
        
        # If metaData.id was not found in the latest log file, try to get it from version 0
        if meta_id_from_delta_log is None:
            v0_log_file = os.path.join(delta_log_path, "00000000000000000000.json")
            if os.path.exists(v0_log_file):
                with open(v0_log_file, 'r') as v0f:
                    for v0_line in v0f:
                        try:
                            v0_action = json.loads(v0_line)
                            if 'metaData' in v0_action:
                                meta_id_from_delta_log = v0_action['metaData'].get('id')
                                break
                        except json.JSONDecodeError:
                            pass # Ignore parsing errors for v0 metadata, just try next line

        # If a valid last_exported_ducklake_snapshot_id was found, reconstruct the files
        if last_exported_ducklake_snapshot_id is not None:
            file_rows = con.execute(f"""
                SELECT path FROM ducklake_data_file
                WHERE table_id = {table_id}
                  AND begin_snapshot <= {last_exported_ducklake_snapshot_id} AND (end_snapshot IS NULL OR end_snapshot > {last_exported_ducklake_snapshot_id})
            """).fetchall()
            files_in_last_delta_version = {path.lstrip('/') for path, in file_rows}
        else:
            print(f"WARNING: 'duckLakeSnapshotId' not found or parsed from latest log ({last_log_file}). Cannot reconstruct previous Delta table state accurately for diffing.")

    except Exception as e:
        print(f"ERROR: Unhandled exception in get_latest_delta_version_info for {delta_log_path}. Resetting state. Error: {e}")
        last_delta_version_idx = -1 # Reset to -1 if there's an issue parsing or finding files

    return last_delta_version_idx, files_in_last_delta_version, last_exported_ducklake_snapshot_id, meta_id_from_delta_log


def generate_latest_delta_log(db_path: str, data_root: str='/lakehouse/default/Tables', checkpoint_interval: int = 1):
    """
    Generates a Delta Lake transaction log for the LATEST state of each table in a DuckLake database.
    This creates incremental updates to Delta, not a full history.
    
    Args:
        db_path (str): The path to the DuckLake database file.
        data_root (str): The root directory for the lakehouse data.
        checkpoint_interval (int): The interval at which to create checkpoint files.
    """
    con = duckdb.connect(db_path, read_only=True)

    tables = con.sql("""
        SELECT 
            t.table_id, 
            t.table_name, 
            s.schema_name,
            t.path as table_path, 
            s.path as schema_path
        FROM ducklake_table t
        JOIN ducklake_schema s USING(schema_id)
        WHERE t.end_snapshot IS NULL
    """).df()

    for row in tables.itertuples():
        table_key = f"{row.schema_name}.{row.table_name}"
        table_root = os.path.join(data_root, row.schema_path, row.table_path)
        delta_log_path = os.path.join(table_root, "_delta_log")
        os.makedirs(delta_log_path, exist_ok=True)
        
        # 1. Get the LATEST DuckLake snapshot for this table
        latest_ducklake_snapshot_raw = con.execute(f"""
            SELECT MAX(begin_snapshot) FROM ducklake_data_file
            WHERE table_id = {row.table_id}
        """).fetchone()
        
        if not latest_ducklake_snapshot_raw or latest_ducklake_snapshot_raw[0] is None:
            print(f"⚠️ {table_key}: No data files found in DuckLake, skipping Delta log generation.")
            continue
        
        latest_ducklake_snapshot_id = latest_ducklake_snapshot_raw[0]

        # 2. Determine the current state of the Delta table and next Delta version
        last_delta_version_idx, previously_exported_files, last_exported_ducklake_snapshot_id, existing_meta_id = \
            get_latest_delta_version_info(delta_log_path, con, row.table_id)
        
        next_delta_version = last_delta_version_idx + 1

        # Check if the Delta table is already up-to-date with the latest DuckLake snapshot
        if last_exported_ducklake_snapshot_id == latest_ducklake_snapshot_id:
            print(f"✅ {table_key}: Delta table already at latest DuckLake snapshot {latest_ducklake_snapshot_id} (Delta version {last_delta_version_idx}), skipping export.")
            continue # Nothing to do, skip to next table

        try:
            now = int(time.time() * 1000)
            now_timestamp = datetime.fromtimestamp(now / 1000)  # Convert to datetime for checkpoint
            log_file = os.path.join(delta_log_path, f"{next_delta_version:020d}.json")
            checkpoint_file = os.path.join(delta_log_path, f"{next_delta_version:020d}.checkpoint.parquet")

            # Fetch all current files associated with the LATEST DuckLake snapshot
            file_rows_for_current_version = con.execute(f"""
                SELECT path, file_size_bytes FROM ducklake_data_file
                WHERE table_id = {row.table_id}
                  AND begin_snapshot <= {latest_ducklake_snapshot_id} AND (end_snapshot IS NULL OR end_snapshot > {latest_ducklake_snapshot_id})
            """).fetchall()

            current_files_map = {}
            for path, size in file_rows_for_current_version:
                rel_path = path.lstrip('/')
                full_path = os.path.join(table_root, rel_path)
                mod_time = int(os.path.getmtime(full_path) * 1000) if os.path.exists(full_path) else now
                current_files_map[rel_path] = {
                    "path": rel_path, "size": size, "modification_time": mod_time,
                    "stats": json.dumps({"numRecords": None}) # Stats would require reading files
                }
            current_file_paths = set(current_files_map.keys())

            added_files_data = []
            removed_files_paths = []

            # Calculate the diff between the previous Delta state and the current latest DuckLake snapshot
            added_file_paths = current_file_paths - previously_exported_files
            removed_file_paths_set = previously_exported_files - current_file_paths
            
            added_files_data = [current_files_map[p] for p in added_file_paths]
            # removed_files_paths only need the path, not full dict
            removed_files_paths = list(removed_file_paths_set)

            # If no changes and not the initial version 0, skip writing a log file
            # Version 0 should always be written if it's the first export, even if empty (e.g., empty table)
            if not added_files_data and not removed_files_paths and next_delta_version > 0:
                print(f" {table_key}: No *detectable* changes between previous Delta state and latest DuckLake snapshot {latest_ducklake_snapshot_id}. Skipping new Delta log for version {next_delta_version}.")
                continue # Skip to the next table

            # Get schema for metadata (always from the latest DuckLake snapshot)
            columns = con.execute(f"""
                SELECT column_name, column_type FROM ducklake_column
                WHERE table_id = {row.table_id}
                  AND begin_snapshot <= {latest_ducklake_snapshot_id} AND (end_snapshot IS NULL OR end_snapshot > {latest_ducklake_snapshot_id})
                ORDER BY column_order
            """).fetchall()

            with open(log_file, 'w') as f:
                # Protocol always comes first
                f.write(json.dumps({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}) + "\n")

                # Determine the table_meta_id
                table_meta_id = existing_meta_id if existing_meta_id else str(uuid.uuid4())
                
                # Metadata always comes second
                schema_fields = [{"name": name, "type": map_type_ducklake_to_spark(typ), "nullable": True, "metadata": {}} for name, typ in columns]
                
                # Configuration, including logRetentionDuration
                table_configuration = {"delta.logRetentionDuration": "interval 1 hour"}

                f.write(json.dumps({
                    "metaData": {
                        "id": table_meta_id, 
                        "name": row.table_name if row.table_name else None,
                        "description": None,
                        "format": {"provider": "parquet", "options": {}},
                        "schemaString": create_spark_schema_string(schema_fields),
                        "partitionColumns": [],
                        "createdTime": now, 
                        "configuration": table_configuration
                    }
                }) + "\n")

                # Write remove actions
                for path in removed_files_paths:
                    f.write(json.dumps({"remove": {"path": path, "deletionTimestamp": now, "dataChange": True}}) + "\n")
                
                # Write add actions, excluding the explicitly removed fields
                for af in added_files_data:
                    f.write(json.dumps({
                        "add": {
                            "path": af["path"],
                            "partitionValues": {},
                            "size": af["size"],
                            "modificationTime": af["modification_time"],
                            "dataChange": True,
                            "stats": af["stats"],
                            "tags": None # Set to null as per example
                            # Removed deletionVector, baseRowId, defaultRowCommitVersion, clusteringProvider
                        }
                    }) + "\n")
                
                # Prepare operationParameters for commitInfo based on Delta version
                commit_operation_parameters = {
                    "mode": "Overwrite",
                    "partitionBy": "[]",
                    "duckLakeSnapshotId": str(latest_ducklake_snapshot_id)
                }
                commit_operation = "WRITE"

                if next_delta_version == 0:
                    # For v0, emulate the 'CREATE TABLE' operation parameters as per example
                    commit_operation = "CREATE TABLE"
                    commit_operation_parameters = {
                        "mode": "ErrorIfExists",
                        "location": f"{data_root}/{row.schema_path}/{row.table_path}", # Construct location based on data_root
                        "protocol": json.dumps({"minReaderVersion": 1, "minWriterVersion": 2}),
                        "metadata": json.dumps({ # Stringify metadata object
                            "configuration": table_configuration,
                            "createdTime": now,
                            "description": None,
                            "format": {"options": {}, "provider": "parquet"},
                            "id": table_meta_id,
                            "name": row.table_name if row.table_name else None,
                            "partitionColumns": [],
                            "schemaString": create_spark_schema_string(schema_fields)
                        })
                    }

                # Write CommitInfo
                f.write(json.dumps({
                    "commitInfo": {
                        "timestamp": now,
                        "operation": commit_operation,
                        "operationParameters": commit_operation_parameters,
                        "isBlindAppend": not removed_files_paths,
                        "engineInfo": "DuckLake-Delta-Export-Latest",
                        "clientVersion": "delta-rs.0.18.1" if next_delta_version == 0 else "DuckLake-Delta-Python" # Use example clientVersion for v0
                    }
                }) + "\n")

            print(f"✅ {table_key}: Delta log written v{next_delta_version} (DuckLake snapshot: {latest_ducklake_snapshot_id})")
            
            # --- CHECKPOINT LOGIC ---
            # Create checkpoint if it's a checkpoint version and doesn't already exist
            if next_delta_version > 0 and next_delta_version % checkpoint_interval == 0 and not os.path.exists(checkpoint_file):
                # Fixed checkpoint creation with proper protocol handling
                checkpoint_records = []
                
                # First record: protocol only
                checkpoint_records.append({
                    "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}, 
                    "metaData": None, 
                    "add": None, 
                    "remove": None, 
                    "commitInfo": None
                })
                
                # Second record: metadata only
                checkpoint_meta_id = existing_meta_id if existing_meta_id else str(uuid.uuid4())
                checkpoint_records.append({
                    "protocol": None, 
                    "commitInfo": None, 
                    "remove": None, 
                    "add": None,
                    "metaData": {
                        "id": checkpoint_meta_id, 
                        "name": row.table_name if row.table_name else None,
                        "description": None,
                        "format": {"provider": "parquet", "options": {}},
                        "schemaString": create_spark_schema_string(schema_fields),
                        "partitionColumns": [], 
                        "createdTime": now, 
                        "configuration": {"delta.logRetentionDuration": "interval 1 hour"}
                    },
                })

                # Add all current files from the latest DuckLake snapshot to the checkpoint
                for af_path in current_file_paths:
                    af = current_files_map[af_path]
                    checkpoint_records.append({
                        "protocol": None, 
                        "metaData": None, 
                        "remove": None, 
                        "commitInfo": None,
                        "add": {
                            "path": af["path"],
                            "partitionValues": {},
                            "size": af["size"],
                            "modificationTime": af["modification_time"],
                            "dataChange": True,
                            "stats": af["stats"],
                            "tags": None # Set to null as per example
                            # Removed deletionVector, baseRowId, defaultRowCommitVersion, clusteringProvider
                        },
                    })
                
                # Create PyArrow table with proper handling of None values
                table_data = {
                    'protocol': [record.get("protocol") for record in checkpoint_records],
                    'metaData': [record.get("metaData") for record in checkpoint_records],
                    'add': [record.get("add") for record in checkpoint_records],
                    'remove': [record.get("remove") for record in checkpoint_records],
                    'commitInfo': [record.get("commitInfo") for record in checkpoint_records]
                }
                
                # Create table directly with target schema to avoid casting issues
                target_schema = get_spark_checkpoint_schema()
                table = pa.table(table_data, schema=target_schema)
                pq.write_table(table, checkpoint_file, compression='snappy')
                
                with open(os.path.join(delta_log_path, "_last_checkpoint"), 'w') as f:
                    json.dump({"version": next_delta_version, "size": len(checkpoint_records)}, f)
                
                print(f"📸 {table_key}: Checkpoint created at Delta version {next_delta_version} (DuckLake snapshot: {latest_ducklake_snapshot_id})")

                # --- Cleanup old JSON log files and Checkpoint files ---
                print(f"🧹 {table_key}: Cleaning up old log and checkpoint files before Delta version {next_delta_version}...")
                for f_name in os.listdir(delta_log_path):
                    base_name = f_name.split('.')[0]
                    # Check for versioned JSON log files
                    if f_name.endswith('.json') and base_name.startswith('0000') and base_name.isdigit():
                        log_version = int(base_name)
                        if log_version < next_delta_version:
                            file_to_delete = os.path.join(delta_log_path, f_name)
                            try:
                                os.remove(file_to_delete)
                                print(f"  Deleted JSON log: {f_name}")
                            except OSError as e:
                                print(f"  Error deleting JSON log {f_name}: {e}")
                    # Check for versioned Parquet checkpoint files
                    elif f_name.endswith('.checkpoint.parquet'):
                        checkpoint_base_name = f_name.split('.checkpoint.parquet')[0]
                        if checkpoint_base_name.startswith('0000') and checkpoint_base_name.isdigit():
                            checkpoint_version = int(checkpoint_base_name)
                            if checkpoint_version < next_delta_version:
                                file_to_delete = os.path.join(delta_log_path, f_name)
                                try:
                                    os.remove(file_to_delete)
                                    print(f"  Deleted checkpoint: {f_name}")
                                except OSError as e:
                                    print(f"  Error deleting checkpoint {f_name}: {e}")
                print(f"🧹 {table_key}: Cleanup complete.")

            elif next_delta_version > 0 and next_delta_version % checkpoint_interval == 0 and os.path.exists(checkpoint_file):
                print(f"⏩ {table_key}: Checkpoint for Delta version {next_delta_version} (DuckLake snapshot: {latest_ducklake_snapshot_id}) already exists, skipping generation.")

        except Exception as e:
            print(f"❌ Failed processing {table_key} for Delta version {next_delta_version} (DuckLake snapshot: {latest_ducklake_snapshot_id}): {e}")
            # This should ideally rollback the written log file if it partially succeeded,
            # but for this script, we just log and continue to next table.

    con.close()
    print("Delta export finished.")