# File: ducklake_delta_exporter.py
import duckdb


def generate_latest_delta_log(db_path: str):
    """
    Export the latest DuckLake snapshot for each table as Delta checkpoint files.
    Uses DuckDB 1.4.4+ native support for writing to abfss://, s3://, etc.

    Args:
        db_path (str): The path to the DuckLake database file (or connection string).
    """
    # For remote paths (abfss://, s3://, etc.), use in-memory connection with ATTACH
    is_remote = any(db_path.startswith(prefix) for prefix in ['abfss://', 's3://', 'gs://', 'az://', 'http://', 'https://'])

    if is_remote:
        con = duckdb.connect()
        # Load required extensions for cloud storage
        if db_path.startswith('abfss://') or db_path.startswith('az://'):
            con.execute("LOAD azure")
            # Load persistent secrets
            con.execute("SELECT * FROM duckdb_secrets()")
        elif db_path.startswith('s3://'):
            con.execute("LOAD httpfs")
        con.execute(f"ATTACH '{db_path}' AS ducklake_db (READ_ONLY)")
        con.execute("USE ducklake_db")
    else:
        con = duckdb.connect(db_path, read_only=True)

    # Build export summary - identify which tables have data
    con.execute("""
        CREATE OR REPLACE TEMP TABLE export_summary AS
        WITH
        data_root_config AS (
            SELECT value AS data_root FROM ducklake_metadata WHERE key = 'data_path'
        ),
        active_tables AS (
            SELECT
                t.table_id,
                t.table_name,
                s.schema_name,
                t.path AS table_path,
                s.path AS schema_path,
                rtrim((SELECT data_root FROM data_root_config), '/') || '/' ||
                    CASE
                        WHEN trim(s.path, '/') != '' THEN trim(s.path, '/') || '/'
                        ELSE ''
                    END ||
                    trim(t.path, '/') AS table_root
            FROM ducklake_table t
            JOIN ducklake_schema s USING(schema_id)
            WHERE t.end_snapshot IS NULL
        ),
        current_snapshot AS (
            SELECT MAX(snapshot_id) AS snapshot_id FROM ducklake_snapshot
        ),
        table_last_modified AS (
            SELECT
                t.*,
                COALESCE(
                    (SELECT MAX(sc.snapshot_id)
                     FROM ducklake_snapshot_changes sc
                     WHERE regexp_matches(sc.changes_made, '[:,]' || t.table_id || '([^0-9]|$)')
                    ),
                    (SELECT cs.snapshot_id
                     FROM current_snapshot cs
                     WHERE EXISTS (
                         SELECT 1 FROM ducklake_data_file df
                         WHERE df.table_id = t.table_id
                           AND df.end_snapshot IS NULL
                     )
                    )
                ) AS last_modified_snapshot,
                (SELECT COUNT(*) FROM ducklake_data_file df
                 WHERE df.table_id = t.table_id
                   AND df.end_snapshot IS NULL
                ) AS file_count
            FROM active_tables t
        )
        SELECT
            table_id,
            schema_name,
            table_name,
            table_root,
            CASE
                WHEN file_count = 0 THEN 'no_data_files'
                WHEN last_modified_snapshot IS NULL THEN 'no_changes'
                ELSE 'needs_export'
            END AS status,
            last_modified_snapshot AS snapshot_id,
            file_count
        FROM table_last_modified
    """)

    # Get tables that need export
    tables_to_export = con.execute("""
        SELECT table_id, schema_name, table_name, table_root, snapshot_id, file_count
        FROM export_summary
        WHERE status = 'needs_export'
    """).fetchall()

    # Show summary
    summary = con.execute("""
        SELECT status, COUNT(*) as cnt FROM export_summary GROUP BY status
    """).fetchall()

    for status, cnt in summary:
        print(f"  {status}: {cnt} tables")

    if not tables_to_export:
        print("\n✅ No tables need export.")
        con.close()
        return

    print(f"\n📦 Exporting {len(tables_to_export)} tables...")

    # Process each table
    for table_id, schema_name, table_name, table_root, snapshot_id, file_count in tables_to_export:
        table_key = f"{schema_name}.{table_name}"

        # Check if checkpoint already exists for this snapshot
        checkpoint_path = f"{table_root}/_delta_log/{snapshot_id:020d}.checkpoint.parquet"
        try:
            con.execute(f"SELECT 1 FROM '{checkpoint_path}' LIMIT 1")
            print(f"  ⏭️  {table_key}: snapshot {snapshot_id} already exported")
            continue
        except Exception:
            pass  # File doesn't exist, proceed with export

        print(f"\n  Processing {table_key}...")

        try:
            # Build checkpoint parquet data for this table
            con.execute("""
                CREATE OR REPLACE TEMP TABLE temp_checkpoint_parquet AS
                WITH
                table_schemas AS (
                    SELECT
                        ? AS table_id,
                        ? AS table_name,
                        ? AS snapshot_id,
                        ? AS table_root,
                        list({
                            'name': c.column_name,
                            'type':
                                CASE
                                    WHEN contains(lower(c.column_type), 'bigint') OR
                                         (contains(lower(c.column_type), 'int') AND contains(c.column_type, '64')) THEN 'long'
                                    WHEN contains(lower(c.column_type), 'int') THEN 'integer'
                                    WHEN contains(lower(c.column_type), 'float') THEN 'double'
                                    WHEN contains(lower(c.column_type), 'double') THEN 'double'
                                    WHEN contains(lower(c.column_type), 'bool') THEN 'boolean'
                                    WHEN contains(lower(c.column_type), 'timestamp') THEN 'timestamp'
                                    WHEN contains(lower(c.column_type), 'date') THEN 'date'
                                    WHEN contains(lower(c.column_type), 'decimal') THEN lower(c.column_type)
                                    ELSE 'string'
                                END,
                            'nullable': true,
                            'metadata': MAP{}::MAP(VARCHAR, VARCHAR)
                        }::STRUCT(name VARCHAR, type VARCHAR, nullable BOOLEAN, metadata MAP(VARCHAR, VARCHAR)) ORDER BY c.column_order) AS schema_fields
                    FROM ducklake_column c
                    WHERE c.table_id = ?
                      AND c.end_snapshot IS NULL
                ),
                file_column_stats_agg AS (
                    SELECT
                        df.data_file_id,
                        c.column_name,
                        ANY_VALUE(c.column_type) AS column_type,
                        MAX(fcs.value_count) AS value_count,
                        MIN(fcs.min_value) AS min_value,
                        MAX(fcs.max_value) AS max_value,
                        MAX(fcs.null_count) AS null_count
                    FROM ducklake_data_file df
                    LEFT JOIN ducklake_file_column_stats fcs ON df.data_file_id = fcs.data_file_id
                    LEFT JOIN ducklake_column c ON fcs.column_id = c.column_id AND c.table_id = df.table_id
                    WHERE df.table_id = ?
                      AND df.end_snapshot IS NULL
                      AND c.column_id IS NOT NULL
                      AND c.end_snapshot IS NULL
                    GROUP BY df.data_file_id, c.column_name
                ),
                file_column_stats_transformed AS (
                    SELECT
                        fca.data_file_id,
                        fca.column_name,
                        fca.column_type,
                        fca.value_count,
                        fca.null_count,
                        CASE
                            WHEN fca.min_value IS NULL THEN NULL
                            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                                regexp_replace(
                                    regexp_replace(replace(fca.min_value, ' ', 'T'), '[+-]\\d{2}(?::\\d{2})?$', ''),
                                    '^([^.]+)$', '\\1.000'
                                ) || 'Z'
                            WHEN contains(lower(fca.column_type), 'date') THEN fca.min_value
                            WHEN contains(lower(fca.column_type), 'bool') THEN CAST(lower(fca.min_value) IN ('true', 't', '1', 'yes') AS VARCHAR)
                            WHEN contains(lower(fca.column_type), 'int') OR contains(lower(fca.column_type), 'float')
                                 OR contains(lower(fca.column_type), 'double') OR contains(lower(fca.column_type), 'decimal') THEN
                                CASE WHEN contains(fca.min_value, '.') OR contains(lower(fca.min_value), 'e')
                                     THEN CAST(TRY_CAST(fca.min_value AS DOUBLE) AS VARCHAR)
                                     ELSE CAST(TRY_CAST(fca.min_value AS BIGINT) AS VARCHAR)
                                END
                            ELSE fca.min_value
                        END AS transformed_min,
                        CASE
                            WHEN fca.max_value IS NULL THEN NULL
                            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                                regexp_replace(
                                    regexp_replace(replace(fca.max_value, ' ', 'T'), '[+-]\\d{2}(?::\\d{2})?$', ''),
                                    '^([^.]+)$', '\\1.000'
                                ) || 'Z'
                            WHEN contains(lower(fca.column_type), 'date') THEN fca.max_value
                            WHEN contains(lower(fca.column_type), 'bool') THEN CAST(lower(fca.max_value) IN ('true', 't', '1', 'yes') AS VARCHAR)
                            WHEN contains(lower(fca.column_type), 'int') OR contains(lower(fca.column_type), 'float')
                                 OR contains(lower(fca.column_type), 'double') OR contains(lower(fca.column_type), 'decimal') THEN
                                CASE WHEN contains(fca.max_value, '.') OR contains(lower(fca.max_value), 'e')
                                     THEN CAST(TRY_CAST(fca.max_value AS DOUBLE) AS VARCHAR)
                                     ELSE CAST(TRY_CAST(fca.max_value AS BIGINT) AS VARCHAR)
                                END
                            ELSE fca.max_value
                        END AS transformed_max
                    FROM file_column_stats_agg fca
                ),
                file_metadata AS (
                    SELECT
                        ts.table_id,
                        ts.table_name,
                        ts.snapshot_id,
                        ts.table_root,
                        ts.schema_fields,
                        df.data_file_id,
                        df.path AS file_path,
                        df.file_size_bytes,
                        COALESCE(MAX(fct.value_count), 0) AS num_records,
                        COALESCE(map_from_entries(list({
                            'key': fct.column_name,
                            'value': fct.transformed_min
                        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.transformed_min IS NOT NULL)), MAP{}::MAP(VARCHAR, VARCHAR)) AS min_values,
                        COALESCE(map_from_entries(list({
                            'key': fct.column_name,
                            'value': fct.transformed_max
                        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.transformed_max IS NOT NULL)), MAP{}::MAP(VARCHAR, VARCHAR)) AS max_values,
                        COALESCE(map_from_entries(list({
                            'key': fct.column_name,
                            'value': fct.null_count
                        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.null_count IS NOT NULL)), MAP{}::MAP(VARCHAR, BIGINT)) AS null_count
                    FROM table_schemas ts
                    JOIN ducklake_data_file df ON df.table_id = ts.table_id
                    LEFT JOIN file_column_stats_transformed fct ON df.data_file_id = fct.data_file_id
                    WHERE df.end_snapshot IS NULL
                    GROUP BY ts.table_id, ts.table_name, ts.snapshot_id,
                             ts.table_root, ts.schema_fields, df.data_file_id, df.path, df.file_size_bytes
                ),
                table_aggregates AS (
                    SELECT
                        table_id,
                        table_name,
                        snapshot_id,
                        table_root,
                        schema_fields,
                        COUNT(*) AS num_files,
                        SUM(num_records) AS total_rows,
                        SUM(file_size_bytes) AS total_bytes,
                        list({
                            'path': ltrim(file_path, '/'),
                            'partitionValues': MAP{}::MAP(VARCHAR, VARCHAR),
                            'size': file_size_bytes,
                            'modificationTime': epoch_ms(now()),
                            'dataChange': true,
                            'stats': COALESCE(to_json({
                                'numRecords': COALESCE(num_records, 0),
                                'minValues': COALESCE(min_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                                'maxValues': COALESCE(max_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                                'nullCount': COALESCE(null_count, MAP{}::MAP(VARCHAR, BIGINT))
                            }), '{"numRecords":0}'),
                            'tags': MAP{}::MAP(VARCHAR, VARCHAR)
                        }::STRUCT(
                            path VARCHAR,
                            partitionValues MAP(VARCHAR, VARCHAR),
                            size BIGINT,
                            modificationTime BIGINT,
                            dataChange BOOLEAN,
                            stats VARCHAR,
                            tags MAP(VARCHAR, VARCHAR)
                        )) AS add_entries
                    FROM file_metadata
                    GROUP BY table_id, table_name, snapshot_id, table_root, schema_fields
                ),
                checkpoint_data AS (
                    SELECT
                        ta.*,
                        epoch_ms(now()) AS now_ms,
                        uuid()::VARCHAR AS txn_id,
                        (substring(md5(ta.table_id::VARCHAR || '-metadata'), 1, 8) || '-' ||
                         substring(md5(ta.table_id::VARCHAR || '-metadata'), 9, 4) || '-' ||
                         substring(md5(ta.table_id::VARCHAR || '-metadata'), 13, 4) || '-' ||
                         substring(md5(ta.table_id::VARCHAR || '-metadata'), 17, 4) || '-' ||
                         substring(md5(ta.table_id::VARCHAR || '-metadata'), 21, 12)) AS meta_id,
                        to_json({'type': 'struct', 'fields': ta.schema_fields}) AS schema_string
                    FROM table_aggregates ta
                ),
                checkpoint_parquet_data AS (
                    SELECT
                        cd.table_id,
                        cd.table_name,
                        cd.snapshot_id,
                        cd.table_root,
                        cd.meta_id,
                        cd.now_ms,
                        cd.txn_id,
                        cd.schema_string,
                        cd.num_files,
                        cd.total_rows,
                        cd.total_bytes,
                        {'minReaderVersion': 1, 'minWriterVersion': 2} AS protocol,
                        NULL AS metaData,
                        NULL AS add,
                        NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                        NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isolationLevel VARCHAR, isBlindAppend BOOLEAN, operationMetrics MAP(VARCHAR, VARCHAR), engineInfo VARCHAR, txnId VARCHAR) AS commitInfo,
                        1 AS row_order
                    FROM checkpoint_data cd
                    UNION ALL
                    SELECT
                        cd.table_id,
                        cd.table_name,
                        cd.snapshot_id,
                        cd.table_root,
                        cd.meta_id,
                        cd.now_ms,
                        cd.txn_id,
                        cd.schema_string,
                        cd.num_files,
                        cd.total_rows,
                        cd.total_bytes,
                        NULL AS protocol,
                        {
                            'id': cd.meta_id,
                            'name': cd.table_name,
                            'format': {'provider': 'parquet', 'options': MAP{}::MAP(VARCHAR, VARCHAR)}::STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
                            'schemaString': cd.schema_string,
                            'partitionColumns': []::VARCHAR[],
                            'createdTime': cd.now_ms,
                            'configuration': MAP{}::MAP(VARCHAR, VARCHAR)
                        }::STRUCT(id VARCHAR, name VARCHAR, format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)), schemaString VARCHAR, partitionColumns VARCHAR[], createdTime BIGINT, configuration MAP(VARCHAR, VARCHAR)) AS metaData,
                        NULL AS add,
                        NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                        NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isolationLevel VARCHAR, isBlindAppend BOOLEAN, operationMetrics MAP(VARCHAR, VARCHAR), engineInfo VARCHAR, txnId VARCHAR) AS commitInfo,
                        2 AS row_order
                    FROM checkpoint_data cd
                    UNION ALL
                    SELECT
                        cd.table_id,
                        cd.table_name,
                        cd.snapshot_id,
                        cd.table_root,
                        cd.meta_id,
                        cd.now_ms,
                        cd.txn_id,
                        cd.schema_string,
                        cd.num_files,
                        cd.total_rows,
                        cd.total_bytes,
                        NULL AS protocol,
                        NULL AS metaData,
                        unnest(cd.add_entries) AS add,
                        NULL::STRUCT(path VARCHAR, deletionTimestamp BIGINT, dataChange BOOLEAN) AS remove,
                        NULL::STRUCT(timestamp TIMESTAMP, operation VARCHAR, operationParameters MAP(VARCHAR, VARCHAR), isolationLevel VARCHAR, isBlindAppend BOOLEAN, operationMetrics MAP(VARCHAR, VARCHAR), engineInfo VARCHAR, txnId VARCHAR) AS commitInfo,
                        3 AS row_order
                    FROM checkpoint_data cd
                )
                SELECT * FROM checkpoint_parquet_data
            """, [table_id, table_name, snapshot_id, table_root, table_id, table_id])

            # Build JSON log content
            con.execute("""
                CREATE OR REPLACE TEMP TABLE temp_checkpoint_json AS
                SELECT DISTINCT
                    p.table_id,
                    p.table_root,
                    p.snapshot_id,
                    p.num_files,
                    to_json({
                        'commitInfo': {
                            'timestamp': p.now_ms,
                            'operation': 'CONVERT',
                            'operationParameters': {
                                'convertedFrom': 'DuckLake',
                                'duckLakeSnapshotId': p.snapshot_id::VARCHAR,
                                'partitionBy': '[]'
                            },
                            'isolationLevel': 'Serializable',
                            'isBlindAppend': false,
                            'operationMetrics': {
                                'numFiles': p.num_files::VARCHAR,
                                'numOutputRows': p.total_rows::VARCHAR,
                                'numOutputBytes': p.total_bytes::VARCHAR
                            },
                            'engineInfo': 'DuckLake-Delta-Exporter/1.0.0',
                            'txnId': p.txn_id
                        }
                    }) || chr(10) ||
                    to_json({
                        'metaData': {
                            'id': p.meta_id,
                            'name': p.table_name,
                            'format': {'provider': 'parquet', 'options': MAP{}},
                            'schemaString': p.schema_string::VARCHAR,
                            'partitionColumns': [],
                            'createdTime': p.now_ms,
                            'configuration': MAP{}
                        }
                    }) || chr(10) ||
                    to_json({
                        'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}
                    }) AS content
                FROM temp_checkpoint_parquet p
                WHERE p.row_order = 1
            """)

            # Build last checkpoint content
            con.execute("""
                CREATE OR REPLACE TEMP TABLE temp_last_checkpoint AS
                SELECT
                    table_id,
                    table_root,
                    snapshot_id,
                    '{"version":' || snapshot_id || ',"size":' || (2 + num_files) || '}' AS content
                FROM temp_checkpoint_parquet
                WHERE row_order = 1
            """)

            # Get file paths
            paths = con.execute("""
                SELECT
                    table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' AS checkpoint_file,
                    table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' AS json_file,
                    table_root || '/_delta_log/_last_checkpoint' AS last_checkpoint_file,
                    table_root || '/_delta_log' AS delta_log_path
                FROM temp_checkpoint_parquet
                WHERE row_order = 1
                LIMIT 1
            """).fetchone()

            checkpoint_file, json_file, last_checkpoint_file, delta_log_path = paths

            # Create delta_log directory for local paths
            if not any(table_root.startswith(prefix) for prefix in ['abfss://', 's3://', 'gs://', 'az://', 'http://', 'https://']):
                con.execute(f"""
                    COPY (SELECT 1 AS id, 1 AS ".duckdb_init")
                    TO '{delta_log_path}'
                    (FORMAT CSV, PARTITION_BY (".duckdb_init"), OVERWRITE_OR_IGNORE)
                """)

            # Write checkpoint parquet
            con.execute(f"""
                COPY (SELECT protocol, metaData, add, remove, commitInfo
                      FROM temp_checkpoint_parquet ORDER BY row_order)
                TO '{checkpoint_file}' (FORMAT PARQUET)
            """)

            # Write JSON log
            con.execute(f"""
                COPY (SELECT content FROM temp_checkpoint_json)
                TO '{json_file}' (FORMAT CSV, HEADER false, QUOTE '')
            """)

            # Write last checkpoint
            con.execute(f"""
                COPY (SELECT content FROM temp_last_checkpoint)
                TO '{last_checkpoint_file}' (FORMAT CSV, HEADER false, QUOTE '')
            """)

            print(f"  ✅ {table_key}: exported snapshot {snapshot_id} ({file_count} files)")

        except Exception as e:
            print(f"  ❌ {table_key}: {e}")

    # Cleanup temp tables
    con.execute("DROP TABLE IF EXISTS export_summary")
    con.execute("DROP TABLE IF EXISTS temp_checkpoint_parquet")
    con.execute("DROP TABLE IF EXISTS temp_checkpoint_json")
    con.execute("DROP TABLE IF EXISTS temp_last_checkpoint")

    con.close()
    print("\n🎉 Export completed!")
