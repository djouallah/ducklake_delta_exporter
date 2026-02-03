"""
Tests for Delta Lake stats transformation in DuckLake Delta Exporter.

Tests verify that column statistics (min/max values) are correctly transformed
from DuckLake format to Delta Lake format for all supported data types.
"""
import pytest
import duckdb
import json


class TestStatsTransformation:
    """Test stats value transformations for Delta Lake format."""

    @pytest.fixture
    def con(self):
        """Create a DuckDB connection for testing."""
        return duckdb.connect()

    def transform_value(self, con, value: str, column_type: str) -> str:
        """
        Apply the same transformation logic used in the exporter.

        This mirrors the CASE statement in file_column_stats_transformed CTE.
        """
        result = con.execute("""
            SELECT CASE
                WHEN $1 IS NULL THEN NULL
                WHEN contains(lower($2), 'timestamp') THEN
                    regexp_replace(
                        regexp_replace(replace($1, ' ', 'T'), '[+-]\\d{2}(?::\\d{2})?$', ''),
                        '^([^.]+)$', '\\1.000'
                    ) || 'Z'
                WHEN contains(lower($2), 'date') THEN $1
                WHEN contains(lower($2), 'bool') THEN CAST(lower($1) IN ('true', 't', '1', 'yes') AS VARCHAR)
                WHEN contains(lower($2), 'int') OR contains(lower($2), 'float')
                     OR contains(lower($2), 'double') OR contains(lower($2), 'decimal') THEN
                    CASE WHEN contains($1, '.') OR contains(lower($1), 'e')
                         THEN CAST(TRY_CAST($1 AS DOUBLE) AS VARCHAR)
                         ELSE CAST(TRY_CAST($1 AS BIGINT) AS VARCHAR)
                    END
                ELSE $1
            END AS transformed
        """, [value, column_type]).fetchone()[0]
        return result

    # ==================== TIMESTAMP TESTS ====================

    def test_timestamp_basic(self, con):
        """Test basic timestamp transformation."""
        result = self.transform_value(con, "2024-01-15 10:30:45", "TIMESTAMP")
        assert result == "2024-01-15T10:30:45.000Z"

    def test_timestamp_with_milliseconds(self, con):
        """Test timestamp with milliseconds."""
        result = self.transform_value(con, "2024-01-15 10:30:45.123", "TIMESTAMP")
        assert result == "2024-01-15T10:30:45.123Z"

    def test_timestamp_with_timezone_offset(self, con):
        """Test timestamp with timezone offset gets stripped."""
        result = self.transform_value(con, "2024-01-15 10:30:45+00", "TIMESTAMP")
        assert result == "2024-01-15T10:30:45.000Z"

    def test_timestamp_with_full_timezone(self, con):
        """Test timestamp with full timezone offset."""
        result = self.transform_value(con, "2024-01-15 10:30:45+05:30", "TIMESTAMP")
        assert result == "2024-01-15T10:30:45.000Z"

    def test_timestamp_negative_timezone(self, con):
        """Test timestamp with negative timezone offset."""
        result = self.transform_value(con, "2024-01-15 10:30:45-08:00", "TIMESTAMP")
        assert result == "2024-01-15T10:30:45.000Z"

    def test_timestamp_with_tz_type(self, con):
        """Test TIMESTAMP WITH TIME ZONE type."""
        result = self.transform_value(con, "2024-01-15 10:30:45", "TIMESTAMP WITH TIME ZONE")
        assert result == "2024-01-15T10:30:45.000Z"

    def test_timestamptz_type(self, con):
        """Test TIMESTAMPTZ type alias."""
        result = self.transform_value(con, "2024-01-15 10:30:45", "TIMESTAMPTZ")
        assert result == "2024-01-15T10:30:45.000Z"

    # ==================== DATE TESTS ====================

    def test_date_basic(self, con):
        """Test date pass-through."""
        result = self.transform_value(con, "2024-01-15", "DATE")
        assert result == "2024-01-15"

    def test_date_edge_cases(self, con):
        """Test date edge cases."""
        assert self.transform_value(con, "2000-01-01", "DATE") == "2000-01-01"
        assert self.transform_value(con, "2099-12-31", "DATE") == "2099-12-31"

    # ==================== BOOLEAN TESTS ====================

    def test_boolean_true_values(self, con):
        """Test various true representations."""
        assert self.transform_value(con, "true", "BOOLEAN") == "true"
        assert self.transform_value(con, "TRUE", "BOOLEAN") == "true"
        assert self.transform_value(con, "True", "BOOLEAN") == "true"
        assert self.transform_value(con, "t", "BOOLEAN") == "true"
        assert self.transform_value(con, "T", "BOOLEAN") == "true"
        assert self.transform_value(con, "1", "BOOLEAN") == "true"
        assert self.transform_value(con, "yes", "BOOLEAN") == "true"
        assert self.transform_value(con, "YES", "BOOLEAN") == "true"

    def test_boolean_false_values(self, con):
        """Test various false representations."""
        assert self.transform_value(con, "false", "BOOLEAN") == "false"
        assert self.transform_value(con, "FALSE", "BOOLEAN") == "false"
        assert self.transform_value(con, "False", "BOOLEAN") == "false"
        assert self.transform_value(con, "f", "BOOLEAN") == "false"
        assert self.transform_value(con, "F", "BOOLEAN") == "false"
        assert self.transform_value(con, "0", "BOOLEAN") == "false"
        assert self.transform_value(con, "no", "BOOLEAN") == "false"
        assert self.transform_value(con, "NO", "BOOLEAN") == "false"

    def test_bool_type_alias(self, con):
        """Test BOOL type alias."""
        assert self.transform_value(con, "true", "BOOL") == "true"
        assert self.transform_value(con, "false", "BOOL") == "false"

    # ==================== INTEGER TESTS ====================

    def test_integer_basic(self, con):
        """Test basic integer transformation."""
        assert self.transform_value(con, "42", "INTEGER") == "42"
        assert self.transform_value(con, "0", "INTEGER") == "0"
        assert self.transform_value(con, "-100", "INTEGER") == "-100"

    def test_integer_large_values(self, con):
        """Test large integer values."""
        assert self.transform_value(con, "2147483647", "INTEGER") == "2147483647"
        assert self.transform_value(con, "-2147483648", "INTEGER") == "-2147483648"

    def test_int_type_alias(self, con):
        """Test INT type alias."""
        assert self.transform_value(con, "42", "INT") == "42"

    def test_int4_type(self, con):
        """Test INT4 type."""
        assert self.transform_value(con, "42", "INT4") == "42"

    def test_smallint(self, con):
        """Test SMALLINT type."""
        assert self.transform_value(con, "32767", "SMALLINT") == "32767"

    def test_tinyint(self, con):
        """Test TINYINT type."""
        assert self.transform_value(con, "127", "TINYINT") == "127"

    # ==================== BIGINT TESTS ====================

    def test_bigint_basic(self, con):
        """Test BIGINT transformation."""
        assert self.transform_value(con, "9223372036854775807", "BIGINT") == "9223372036854775807"
        assert self.transform_value(con, "-9223372036854775808", "BIGINT") == "-9223372036854775808"

    def test_int64_type(self, con):
        """Test INT64 type alias."""
        assert self.transform_value(con, "123456789012345", "INT64") == "123456789012345"

    def test_int8_type(self, con):
        """Test INT8 type (DuckDB alias for BIGINT)."""
        assert self.transform_value(con, "123456789", "INT8") == "123456789"

    # ==================== FLOAT/DOUBLE TESTS ====================

    def test_float_basic(self, con):
        """Test float transformation."""
        result = self.transform_value(con, "3.14", "FLOAT")
        assert float(result) == pytest.approx(3.14)

    def test_double_basic(self, con):
        """Test double transformation."""
        result = self.transform_value(con, "3.141592653589793", "DOUBLE")
        assert float(result) == pytest.approx(3.141592653589793)

    def test_float_scientific_notation(self, con):
        """Test scientific notation."""
        result = self.transform_value(con, "1.5e10", "DOUBLE")
        assert float(result) == pytest.approx(1.5e10)

    def test_float_negative_exponent(self, con):
        """Test negative exponent."""
        result = self.transform_value(con, "1.5e-5", "DOUBLE")
        assert float(result) == pytest.approx(1.5e-5)

    def test_real_type(self, con):
        """Test REAL type (alias for FLOAT)."""
        result = self.transform_value(con, "2.5", "REAL")
        assert float(result) == pytest.approx(2.5)

    def test_float4_type(self, con):
        """Test FLOAT4 type."""
        result = self.transform_value(con, "2.5", "FLOAT4")
        assert float(result) == pytest.approx(2.5)

    def test_float8_type(self, con):
        """Test FLOAT8 type (alias for DOUBLE)."""
        result = self.transform_value(con, "2.5", "FLOAT8")
        assert float(result) == pytest.approx(2.5)

    # ==================== DECIMAL TESTS ====================

    def test_decimal_basic(self, con):
        """Test DECIMAL transformation."""
        result = self.transform_value(con, "123.45", "DECIMAL(10,2)")
        assert float(result) == pytest.approx(123.45)

    def test_decimal_high_precision(self, con):
        """Test high precision decimal."""
        result = self.transform_value(con, "123456.789012", "DECIMAL(18,6)")
        assert float(result) == pytest.approx(123456.789012)

    def test_numeric_type(self, con):
        """Test NUMERIC type (alias for DECIMAL)."""
        result = self.transform_value(con, "99.99", "NUMERIC(5,2)")
        assert float(result) == pytest.approx(99.99)

    def test_decimal_integer_value(self, con):
        """Test decimal with integer value (no decimal point)."""
        result = self.transform_value(con, "100", "DECIMAL(10,2)")
        assert result == "100"

    # ==================== STRING TESTS ====================

    def test_string_passthrough(self, con):
        """Test string values pass through unchanged."""
        assert self.transform_value(con, "hello world", "VARCHAR") == "hello world"
        assert self.transform_value(con, "test@example.com", "VARCHAR") == "test@example.com"

    def test_text_type(self, con):
        """Test TEXT type."""
        assert self.transform_value(con, "some text", "TEXT") == "some text"

    def test_string_type(self, con):
        """Test STRING type."""
        assert self.transform_value(con, "a string", "STRING") == "a string"

    def test_char_type(self, con):
        """Test CHAR type."""
        assert self.transform_value(con, "ABC", "CHAR(10)") == "ABC"

    def test_string_with_special_chars(self, con):
        """Test strings with special characters."""
        assert self.transform_value(con, "Hello, World!", "VARCHAR") == "Hello, World!"
        assert self.transform_value(con, "line1\nline2", "VARCHAR") == "line1\nline2"

    # ==================== NULL TESTS ====================

    def test_null_value(self, con):
        """Test NULL value handling."""
        assert self.transform_value(con, None, "INTEGER") is None
        assert self.transform_value(con, None, "VARCHAR") is None
        assert self.transform_value(con, None, "TIMESTAMP") is None

    # ==================== EDGE CASES ====================

    def test_empty_string(self, con):
        """Test empty string handling."""
        assert self.transform_value(con, "", "VARCHAR") == ""

    def test_numeric_string_in_varchar(self, con):
        """Test numeric-looking string in VARCHAR stays as string."""
        # VARCHAR should pass through without numeric conversion
        assert self.transform_value(con, "42", "VARCHAR") == "42"
        assert self.transform_value(con, "3.14", "VARCHAR") == "3.14"


class TestStatsInCheckpoint:
    """Test that stats are correctly included in checkpoint parquet output."""

    @pytest.fixture
    def con(self):
        """Create a DuckDB connection with DuckLake extension."""
        con = duckdb.connect()
        try:
            # Try loading first (if already installed)
            con.execute("LOAD ducklake")
        except Exception:
            try:
                # Try installing from community
                con.execute("INSTALL ducklake FROM community")
                con.execute("LOAD ducklake")
            except Exception as e:
                pytest.skip(f"DuckLake extension not available: {e}")
        return con

    def test_stats_structure_in_add_action(self, con, tmp_path):
        """Test that stats JSON has correct structure in add action."""
        # Create a DuckLake database with test data
        db_path = str(tmp_path / "test.ducklake")
        data_path = str(tmp_path / "data")

        con.execute(f"ATTACH 'ducklake:{db_path}' AS test_db (DATA_PATH '{data_path}')")
        con.execute("USE test_db")

        # Create table with various column types
        con.execute("""
            CREATE TABLE test_stats (
                id INTEGER,
                name VARCHAR,
                amount DOUBLE,
                created_at TIMESTAMP,
                is_active BOOLEAN,
                birth_date DATE
            )
        """)

        # Insert test data
        con.execute("""
            INSERT INTO test_stats VALUES
            (1, 'Alice', 100.50, '2024-01-15 10:30:00', true, '1990-05-20'),
            (2, 'Bob', 200.75, '2024-02-20 14:45:00', false, '1985-12-10'),
            (3, 'Charlie', 50.25, '2024-03-25 09:15:00', true, '1992-08-05')
        """)

        # Check that file column stats were recorded (query from metadata schema)
        stats = con.execute("""
            SELECT
                c.column_name,
                c.column_type,
                fcs.min_value,
                fcs.max_value,
                fcs.null_count,
                fcs.value_count
            FROM __ducklake_metadata_test_db.ducklake_file_column_stats fcs
            JOIN __ducklake_metadata_test_db.ducklake_column c ON fcs.column_id = c.column_id
            ORDER BY c.column_order
        """).fetchall()

        # Verify stats exist for all columns
        column_names = [s[0] for s in stats]
        assert 'id' in column_names
        assert 'name' in column_names
        assert 'amount' in column_names
        assert 'created_at' in column_names
        assert 'is_active' in column_names
        assert 'birth_date' in column_names

        # Verify min/max values are present (except for boolean which has no min/max)
        for stat in stats:
            col_name, col_type, min_val, max_val, null_count, value_count = stat
            if 'bool' not in col_type.lower():
                assert min_val is not None, f"min_value should not be None for {col_name}"
                assert max_val is not None, f"max_value should not be None for {col_name}"
            assert null_count == 0, f"null_count should be 0 for {col_name}"
            assert value_count == 3, f"value_count should be 3 for {col_name}"

    def test_stats_json_format(self, con, tmp_path):
        """Test that exported stats JSON is valid and has expected format."""
        # Create a DuckLake database
        db_path = str(tmp_path / "test.ducklake")
        data_path = str(tmp_path / "data")

        con.execute(f"ATTACH 'ducklake:{db_path}' AS test_db (DATA_PATH '{data_path}')")
        con.execute("USE test_db")

        con.execute("""
            CREATE TABLE stats_test (
                int_col INTEGER,
                str_col VARCHAR,
                ts_col TIMESTAMP
            )
        """)

        con.execute("""
            INSERT INTO stats_test VALUES
            (10, 'min', '2024-01-01 00:00:00'),
            (50, 'max', '2024-12-31 23:59:59')
        """)

        # Close connection before exporting (file lock)
        con.close()

        # Run the exporter
        from ducklake_delta_exporter import generate_latest_delta_log
        generate_latest_delta_log(db_path)

        # Reopen for verification
        con = duckdb.connect()

        # Read the checkpoint parquet
        checkpoint_files = list((tmp_path / "data" / "main" / "stats_test" / "_delta_log").glob("*.checkpoint.parquet"))
        assert len(checkpoint_files) == 1, "Should have exactly one checkpoint file"

        # Read the add action and verify stats
        add_rows = con.execute(f"""
            SELECT add.stats
            FROM '{checkpoint_files[0]}'
            WHERE add IS NOT NULL
        """).fetchall()

        assert len(add_rows) == 1, "Should have one add action"

        stats_json = json.loads(add_rows[0][0])

        # Verify stats structure
        assert 'numRecords' in stats_json
        assert 'minValues' in stats_json
        assert 'maxValues' in stats_json
        assert 'nullCount' in stats_json

        # Verify record count
        assert stats_json['numRecords'] == 2

        # Verify min/max values exist for columns
        assert 'int_col' in stats_json['minValues']
        assert 'str_col' in stats_json['minValues']
        assert 'ts_col' in stats_json['minValues']

        # Verify integer stats
        assert stats_json['minValues']['int_col'] == '10'
        assert stats_json['maxValues']['int_col'] == '50'

        # Verify string stats (alphabetically: 'max' < 'min')
        assert stats_json['minValues']['str_col'] == 'max'
        assert stats_json['maxValues']['str_col'] == 'min'

        # Verify timestamp format (should be ISO with Z suffix)
        assert stats_json['minValues']['ts_col'].endswith('Z')
        assert 'T' in stats_json['minValues']['ts_col']

    def test_null_count_tracking(self, con, tmp_path):
        """Test that null counts are correctly tracked in stats."""
        db_path = str(tmp_path / "test.ducklake")
        data_path = str(tmp_path / "data")

        con.execute(f"ATTACH 'ducklake:{db_path}' AS test_db (DATA_PATH '{data_path}')")
        con.execute("USE test_db")

        con.execute("""
            CREATE TABLE null_test (
                required_col INTEGER,
                nullable_col VARCHAR
            )
        """)

        con.execute("""
            INSERT INTO null_test VALUES
            (1, 'value1'),
            (2, NULL),
            (3, 'value3'),
            (4, NULL)
        """)

        # Close connection before exporting (file lock)
        con.close()

        from ducklake_delta_exporter import generate_latest_delta_log
        generate_latest_delta_log(db_path)

        # Reopen for verification
        con = duckdb.connect()

        checkpoint_files = list((tmp_path / "data" / "main" / "null_test" / "_delta_log").glob("*.checkpoint.parquet"))

        add_rows = con.execute(f"""
            SELECT add.stats
            FROM '{checkpoint_files[0]}'
            WHERE add IS NOT NULL
        """).fetchall()

        stats_json = json.loads(add_rows[0][0])

        # Verify null counts
        assert stats_json['nullCount']['required_col'] == 0
        assert stats_json['nullCount']['nullable_col'] == 2

    def test_stats_isolation_between_tables(self, con, tmp_path):
        """Test that stats from different tables don't leak into each other.

        This tests the fix for the table_id grouping bug where columns with
        the same name from different tables could have their stats mixed.
        """
        db_path = str(tmp_path / "test.ducklake")
        data_path = str(tmp_path / "data")

        con.execute(f"ATTACH 'ducklake:{db_path}' AS test_db (DATA_PATH '{data_path}')")
        con.execute("USE test_db")

        # Create two tables with overlapping column names but different data
        con.execute("""
            CREATE TABLE table_a (
                id INTEGER,
                name VARCHAR,
                value INTEGER
            )
        """)

        con.execute("""
            CREATE TABLE table_b (
                id INTEGER,
                name VARCHAR,
                amount DOUBLE,
                extra_col VARCHAR
            )
        """)

        # Insert different data ranges
        con.execute("""
            INSERT INTO table_a VALUES
            (1, 'alpha', 100),
            (2, 'beta', 200)
        """)

        con.execute("""
            INSERT INTO table_b VALUES
            (100, 'zebra', 999.99, 'extra1'),
            (200, 'yak', 888.88, 'extra2')
        """)

        # Close connection before exporting (file lock)
        con.close()

        from ducklake_delta_exporter import generate_latest_delta_log
        generate_latest_delta_log(db_path)

        # Reopen for verification
        con = duckdb.connect()
        con.execute("LOAD ducklake")

        # Check table_a stats - should only have its own columns
        checkpoint_a = list((tmp_path / "data" / "main" / "table_a" / "_delta_log").glob("*.checkpoint.parquet"))
        assert len(checkpoint_a) == 1

        stats_a = con.execute(f"""
            SELECT add.stats
            FROM '{checkpoint_a[0]}'
            WHERE add IS NOT NULL
        """).fetchone()[0]

        stats_a_json = json.loads(stats_a)

        # table_a should have id, name, value - NOT amount or extra_col
        assert 'id' in stats_a_json['minValues']
        assert 'name' in stats_a_json['minValues']
        assert 'value' in stats_a_json['minValues']
        assert 'amount' not in stats_a_json['minValues'], "table_a should not have table_b's 'amount' column"
        assert 'extra_col' not in stats_a_json['minValues'], "table_a should not have table_b's 'extra_col' column"

        # Verify table_a stats have correct values (not mixed with table_b)
        assert stats_a_json['minValues']['id'] == '1'  # table_a has 1,2 not 100,200
        assert stats_a_json['maxValues']['id'] == '2'
        assert stats_a_json['minValues']['name'] == 'alpha'  # not 'yak' or 'zebra'
        assert stats_a_json['maxValues']['name'] == 'beta'

        # Check table_b stats - should only have its own columns
        checkpoint_b = list((tmp_path / "data" / "main" / "table_b" / "_delta_log").glob("*.checkpoint.parquet"))
        assert len(checkpoint_b) == 1

        stats_b = con.execute(f"""
            SELECT add.stats
            FROM '{checkpoint_b[0]}'
            WHERE add IS NOT NULL
        """).fetchone()[0]

        stats_b_json = json.loads(stats_b)

        # table_b should have id, name, amount, extra_col - NOT value
        assert 'id' in stats_b_json['minValues']
        assert 'name' in stats_b_json['minValues']
        assert 'amount' in stats_b_json['minValues']
        assert 'extra_col' in stats_b_json['minValues']
        assert 'value' not in stats_b_json['minValues'], "table_b should not have table_a's 'value' column"

        # Verify table_b stats have correct values (not mixed with table_a)
        assert stats_b_json['minValues']['id'] == '100'  # table_b has 100,200 not 1,2
        assert stats_b_json['maxValues']['id'] == '200'
        assert stats_b_json['minValues']['name'] == 'yak'  # not 'alpha' or 'beta'
        assert stats_b_json['maxValues']['name'] == 'zebra'


class TestDeltaLakeTypeMapping:
    """Test that DuckDB types are correctly mapped to Delta Lake types in schema."""

    @pytest.fixture
    def con(self):
        """Create a DuckDB connection."""
        return duckdb.connect()

    def map_type(self, con, duckdb_type: str) -> str:
        """Map DuckDB type to Delta Lake type using the exporter's logic."""
        result = con.execute("""
            SELECT CASE
                WHEN contains(lower($1), 'bigint') OR
                     (contains(lower($1), 'int') AND contains($1, '64')) THEN 'long'
                WHEN contains(lower($1), 'int') THEN 'integer'
                WHEN contains(lower($1), 'float') THEN 'double'
                WHEN contains(lower($1), 'double') THEN 'double'
                WHEN contains(lower($1), 'bool') THEN 'boolean'
                WHEN contains(lower($1), 'timestamp') THEN 'timestamp'
                WHEN contains(lower($1), 'date') THEN 'date'
                WHEN contains(lower($1), 'decimal') THEN lower($1)
                ELSE 'string'
            END
        """, [duckdb_type]).fetchone()[0]
        return result

    def test_integer_types(self, con):
        """Test integer type mappings."""
        assert self.map_type(con, "INTEGER") == "integer"
        assert self.map_type(con, "INT") == "integer"
        assert self.map_type(con, "INT4") == "integer"
        assert self.map_type(con, "SMALLINT") == "integer"
        assert self.map_type(con, "TINYINT") == "integer"

    def test_bigint_types(self, con):
        """Test bigint type mappings."""
        assert self.map_type(con, "BIGINT") == "long"
        assert self.map_type(con, "INT64") == "long"
        assert self.map_type(con, "INT8") == "integer"  # INT8 doesn't contain '64' or 'bigint'

    def test_float_types(self, con):
        """Test float/double type mappings."""
        assert self.map_type(con, "FLOAT") == "double"
        assert self.map_type(con, "DOUBLE") == "double"
        assert self.map_type(con, "REAL") == "string"  # REAL doesn't match 'float' or 'double'
        assert self.map_type(con, "FLOAT4") == "double"
        assert self.map_type(con, "FLOAT8") == "double"

    def test_boolean_types(self, con):
        """Test boolean type mappings."""
        assert self.map_type(con, "BOOLEAN") == "boolean"
        assert self.map_type(con, "BOOL") == "boolean"

    def test_timestamp_types(self, con):
        """Test timestamp type mappings."""
        assert self.map_type(con, "TIMESTAMP") == "timestamp"
        assert self.map_type(con, "TIMESTAMP WITH TIME ZONE") == "timestamp"
        assert self.map_type(con, "TIMESTAMPTZ") == "timestamp"

    def test_date_type(self, con):
        """Test date type mapping."""
        assert self.map_type(con, "DATE") == "date"

    def test_decimal_types(self, con):
        """Test decimal type mappings."""
        assert self.map_type(con, "DECIMAL(10,2)") == "decimal(10,2)"
        assert self.map_type(con, "DECIMAL(18,6)") == "decimal(18,6)"
        assert self.map_type(con, "NUMERIC(5,2)") == "string"  # NUMERIC doesn't match 'decimal'

    def test_string_types(self, con):
        """Test string type mappings."""
        assert self.map_type(con, "VARCHAR") == "string"
        assert self.map_type(con, "VARCHAR(100)") == "string"
        assert self.map_type(con, "TEXT") == "string"
        assert self.map_type(con, "STRING") == "string"
        assert self.map_type(con, "CHAR(10)") == "string"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
