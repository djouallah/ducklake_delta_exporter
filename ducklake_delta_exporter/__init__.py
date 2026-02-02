# File: ducklake_delta_exporter.py
import json
import time
import duckdb

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

def get_latest_ducklake_snapshot(con, table_id):
    """
    Get the latest DuckLake snapshot ID for a table.
    """
    latest_snapshot  = con.execute(f""" SELECT MAX(begin_snapshot) as latest_snapshot FROM ducklake_data_file  WHERE table_id = {table_id} """).fetchone()[0]
    return latest_snapshot

def get_latest_delta_checkpoint(con, table_id):
    """
    check how many times a table has being modified.
    """
    delta_checkpoint = con.execute(f""" SELECT count(snapshot_id) FROM ducklake_snapshot_changes
                                   where changes_made like '%:{table_id}' or changes_made like '%:{table_id},%' """).fetchone()[0]
    return delta_checkpoint

def get_file_modification_time(dummy_time):
    """
    Return a dummy modification time for parquet files.
    This avoids the latency of actually reading file metadata.
    
    Args:
        dummy_time: Timestamp in milliseconds to use as modification time
    
    Returns:
        Modification time in milliseconds
    """
    return dummy_time

def create_dummy_json_log(table_root, delta_version, table_info, schema_fields, now):
    """
    Create a minimal JSON log file for Spark compatibility using DuckDB.
    """
    json_log_file = table_root + f"_delta_log/{delta_version:020d}.json"
    
    # Create JSON log entries using DuckDB
    duckdb.execute("DROP TABLE IF EXISTS json_log_table")
    
    # Protocol entry
    protocol_json = json.dumps({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    })
    
    # Metadata entry
    metadata_json = json.dumps({
        "metaData": {
            "id": str(table_info['table_id']),
            "name": table_info['table_name'],
            "description": None,
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": create_spark_schema_string(schema_fields),
            "partitionColumns": [],
            "createdTime": now,
            "configuration": {
                "delta.logRetentionDuration": "interval 1 hour"
            }
        }
    })
    
    # Commit info entry
    commitinfo_json = json.dumps({
        "commitInfo": {
            "timestamp": now,
            "operation": "CONVERT",
            "operationParameters": {
                "convertedFrom": "DuckLake"
            },
            "isBlindAppend": True,
            "engineInfo": "DuckLake-Delta-Exporter",
            "clientVersion": "1.0.0"
        }
    })
    
    # Create table with JSON entries
    duckdb.execute("""
        CREATE TABLE json_log_table AS
        SELECT ? AS json_line
        UNION ALL
        SELECT ? AS json_line
        UNION ALL
        SELECT ? AS json_line
    """, [protocol_json, metadata_json, commitinfo_json])
    
    # Write JSON log file using DuckDB
    duckdb.execute(f"COPY (SELECT json_line FROM json_log_table) TO '{json_log_file}' (FORMAT CSV, HEADER false, QUOTE '')")
    
    # Clean up
    duckdb.execute("DROP TABLE IF EXISTS json_log_table")
    
    return json_log_file

def build_file_path(table_root, relative_path):
    """
    Build full file path from table root and relative path.
    Works with both local paths and S3 URLs.
    """
    table_root = table_root.rstrip('/')
    relative_path = relative_path.lstrip('/')
    return f"{table_root}/{relative_path}"

def create_checkpoint_for_latest_snapshot(con, table_info, data_root):
    """
    Create a Delta checkpoint file for the latest DuckLake snapshot.
    """
    table_root = data_root.rstrip('/') + '/' + table_info['schema_path'] + table_info['table_path']
    
    # Get the latest snapshot
    latest_snapshot = get_latest_ducklake_snapshot(con, table_info['table_id'])
    if latest_snapshot is None:
        print(f"⚠️ {table_info['schema_name']}.{table_info['table_name']}: No snapshots found")
        return False
    delta_version   = get_latest_delta_checkpoint(con, table_info['table_id'])
    checkpoint_file = table_root + f"_delta_log/{delta_version:020d}.checkpoint.parquet"
    json_log_file   = table_root + f"_delta_log/{delta_version:020d}.json"
    
    try:
        con.execute(f"SELECT protocol FROM '{checkpoint_file}' limit 0 ")
        print(f"⚠️ {table_info['schema_name']}.{table_info['table_name']}: Checkpoint file already exists: {checkpoint_file}")
    except:
    
        now = int(time.time() * 1000)
        
        # Get all files for the latest snapshot
        file_rows = con.execute(f"""
            SELECT path, file_size_bytes FROM ducklake_data_file
            WHERE table_id = {table_info['table_id']}
            AND begin_snapshot <= {latest_snapshot} 
            AND (end_snapshot IS NULL OR end_snapshot > {latest_snapshot})
        """).fetchall()
        
        # Get schema for the latest snapshot
        columns = con.execute(f"""
            SELECT column_name, column_type FROM ducklake_column
            WHERE table_id = {table_info['table_id']}
            AND begin_snapshot <= {latest_snapshot} 
            AND (end_snapshot IS NULL OR end_snapshot > {latest_snapshot})
            ORDER BY column_order
        """).fetchall()
        
        # Get or generate table metadata ID
        table_meta_id = str(table_info['table_id'])
        
        # Prepare schema
        schema_fields = [
            {"name": name, "type": map_type_ducklake_to_spark(typ), "nullable": True, "metadata": {}} 
            for name, typ in columns
        ]
        
        # Create checkpoint data using DuckDB directly
        checkpoint_data = []
        
        # Create checkpoint data directly in DuckDB using proper data types
        duckdb.execute("DROP TABLE IF EXISTS checkpoint_table")
        
        # Create the checkpoint table with proper nested structure
        duckdb.execute("""
            CREATE TABLE checkpoint_table AS
            WITH checkpoint_data AS (
                -- Protocol record
                SELECT 
                    {'minReaderVersion': 1, 'minWriterVersion': 2}::STRUCT(minReaderVersion INTEGER, minWriterVersion INTEGER) AS protocol,
                    NULL::STRUCT(id VARCHAR, name VARCHAR, description VARCHAR, format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)), schemaString VARCHAR, partitionColumns VARCHAR[], createdTime BIGINT, configuration MAP(VARCHAR, VARCHAR)) AS metaData,
                    NULL::STRUCT(path VARCHAR, partitionValues MAP(VARCHAR, VARCHAR), size BIGINT, modificationTime BIGINT, dataChange BOOLEAN, stats VARCHAR, tags MAP(VARCHAR, VARCHAR)) AS add,
                    NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                    NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isBlindAppend BOOLEAN, engineInfo VARCHAR, clientVersion VARCHAR) AS commitInfo
                
                UNION ALL
                
                -- Metadata record
                SELECT 
                    NULL::STRUCT(minReaderVersion INTEGER, minWriterVersion INTEGER) AS protocol,
                    {
                        'id': ?, 
                        'name': ?, 
                        'description': NULL, 
                        'format': {'provider': 'parquet', 'options': MAP{}}::STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
                        'schemaString': ?, 
                        'partitionColumns': []::VARCHAR[], 
                        'createdTime': ?, 
                        'configuration': MAP{'delta.logRetentionDuration': 'interval 1 hour'}
                    }::STRUCT(id VARCHAR, name VARCHAR, description VARCHAR, format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)), schemaString VARCHAR, partitionColumns VARCHAR[], createdTime BIGINT, configuration MAP(VARCHAR, VARCHAR)) AS metaData,
                    NULL::STRUCT(path VARCHAR, partitionValues MAP(VARCHAR, VARCHAR), size BIGINT, modificationTime BIGINT, dataChange BOOLEAN, stats VARCHAR, tags MAP(VARCHAR, VARCHAR)) AS add,
                    NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                    NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isBlindAppend BOOLEAN, engineInfo VARCHAR, clientVersion VARCHAR) AS commitInfo
            )
            SELECT * FROM checkpoint_data
        """, [table_meta_id, table_info['table_name'], create_spark_schema_string(schema_fields), now])
        
        # Add file records
        for path, size in file_rows:
            rel_path = path.lstrip('/')
            full_path = build_file_path(table_root, rel_path)
            mod_time = get_file_modification_time(now)
            
            duckdb.execute("""
                INSERT INTO checkpoint_table
                SELECT 
                    NULL::STRUCT(minReaderVersion INTEGER, minWriterVersion INTEGER) AS protocol,
                    NULL::STRUCT(id VARCHAR, name VARCHAR, description VARCHAR, format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)), schemaString VARCHAR, partitionColumns VARCHAR[], createdTime BIGINT, configuration MAP(VARCHAR, VARCHAR)) AS metaData,
                    {
                        'path': ?, 
                        'partitionValues': MAP{}::MAP(VARCHAR, VARCHAR), 
                        'size': ?, 
                        'modificationTime': ?, 
                        'dataChange': true, 
                        'stats': ?, 
                        'tags': NULL::MAP(VARCHAR, VARCHAR)
                    }::STRUCT(path VARCHAR, partitionValues MAP(VARCHAR, VARCHAR), size BIGINT, modificationTime BIGINT, dataChange BOOLEAN, stats VARCHAR, tags MAP(VARCHAR, VARCHAR)) AS add,
                    NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                    NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isBlindAppend BOOLEAN, engineInfo VARCHAR, clientVersion VARCHAR) AS commitInfo
            """, [rel_path, size, mod_time, json.dumps({"numRecords": None})])
        
        # Create the _delta_log directory if it doesn't exist
        duckdb.execute(f"COPY (SELECT 43) TO '{table_root}_delta_log' (FORMAT PARQUET, PER_THREAD_OUTPUT, OVERWRITE_OR_IGNORE)")
            
        # Write the checkpoint file
        duckdb.execute(f"COPY (SELECT * FROM checkpoint_table) TO '{checkpoint_file}' (FORMAT PARQUET)")
        
        # Create dummy JSON log file for Spark compatibility
        create_dummy_json_log(table_root, delta_version, table_info, schema_fields, now)
            
        # Write the _last_checkpoint file
        total_records = 2 + len(file_rows)  # protocol + metadata + file records
        duckdb.execute(f"""
            COPY (SELECT {delta_version} AS version, {total_records} AS size) 
            TO '{table_root}_delta_log/_last_checkpoint' (FORMAT JSON, ARRAY false)
        """)
            
        print(f"✅ Exported DuckLake snapshot {latest_snapshot} as Delta checkpoint v{delta_version}")
        print(f"✅ Created JSON log file: {json_log_file}")
        
        # Clean up temporary tables
        duckdb.execute("DROP TABLE IF EXISTS checkpoint_table")
    
    return True, delta_version, latest_snapshot

def generate_latest_delta_log(db_path: str, data_root: str = None):
    """
    Export the latest DuckLake snapshot for each table as a Delta checkpoint file.
    Creates both checkpoint files and minimal JSON log files for Spark compatibility.
    
    Args:
        db_path (str): The path to the DuckLake database file.
        data_root (str): The root directory for the lakehouse data.
    """
    con = duckdb.connect(db_path, read_only=True)
    
    if data_root is None:
        data_root = con.sql("SELECT value FROM ducklake_metadata WHERE key = 'data_path'").fetchone()[0]
    
    # Get all active tables
    tables = con.execute("""
        SELECT 
            t.table_id, 
            t.table_name, 
            s.schema_name,
            t.path as table_path, 
            s.path as schema_path
        FROM ducklake_table t
        JOIN ducklake_schema s USING(schema_id)
        WHERE t.end_snapshot IS NULL
    """).fetchall()
    
    total_tables = len(tables)
    successful_exports = 0
    
    for table_row in tables:
        table_info = {
            'table_id': table_row[0],
            'table_name': table_row[1],
            'schema_name': table_row[2],
            'table_path': table_row[3],
            'schema_path': table_row[4]
        }
        
        table_key = f"{table_info['schema_name']}.{table_info['table_name']}"
        print(f"Processing {table_key}...")
        
        try:
            result = create_checkpoint_for_latest_snapshot(con, table_info, data_root)
            
            if result:
                successful_exports += 1
            else:
                print(f"⚠️ {table_key}: No data to export")
                
        except Exception as e:
            print(f"❌ {table_key}: Failed to export checkpoint - {e}")
    
    con.close()
    print(f"\n🎉 Export completed! {successful_exports}/{total_tables} tables exported successfully.")