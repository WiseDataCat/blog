#!/usr/bin/env python3
"""
Parquet to PostgreSQL Schema Converter

This script automatically generates PostgreSQL CREATE TABLE statements from:
1. Parquet files
2. DuckDB relations
3. Pandas DataFrames with parquet-like schemas

Usage:
    python parquet_to_postgres.py <parquet_file> [table_name]

Or import as module:
    from parquet_to_postgres import ParquetToPostgres
    converter = ParquetToPostgres()
    sql = converter.from_parquet_file('data.parquet', 'my_table')
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional
import logging

try:
    import duckdb

    # import pandas as pd

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    print("Warning: duckdb not available. Install with: pip install duckdb")

try:
    import pyarrow.parquet as pq

    # import pyarrow as pa

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    print("Warning: pyarrow not available. Install with: pip install pyarrow")


class ParquetToPostgres:
    """Convert parquet schemas to PostgreSQL CREATE TABLE statements."""

    # Type mapping from various formats to PostgreSQL
    DUCKDB_TO_POSTGRES = {
        "INTEGER": "INTEGER",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "SMALLINT",  # PostgreSQL doesn't have TINYINT
        "DOUBLE": "DOUBLE PRECISION",
        "REAL": "REAL",
        "FLOAT": "REAL",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "TEXT",
        "STRING": "TEXT",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMPTZ": "TIMESTAMPTZ",
        "DATE": "DATE",
        "TIME": "TIME",
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        "BLOB": "BYTEA",
        "BYTEA": "BYTEA",
        "UUID": "UUID",
        "JSON": "JSONB",
        "JSONB": "JSONB",
    }

    ARROW_TO_POSTGRES = {
        "int8": "SMALLINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "uint8": "SMALLINT",
        "uint16": "INTEGER",
        "uint32": "BIGINT",
        "uint64": "BIGINT",
        "float": "REAL",
        "double": "DOUBLE PRECISION",
        "string": "TEXT",
        "large_string": "TEXT",
        "binary": "BYTEA",
        "large_binary": "BYTEA",
        "bool": "BOOLEAN",
        "date32": "DATE",
        "date64": "DATE",
        "timestamp": "TIMESTAMP",
        "time32": "TIME",
        "time64": "TIME",
    }

    def __init__(self, lowercase_columns: bool = False):
        """
        Initialize converter.

        Args:
            lowercase_columns: Whether to convert column names to lowercase
        """
        self.lowercase_columns = lowercase_columns
        self.logger = logging.getLogger(__name__)

    def _clean_column_name(self, col_name: str) -> str:
        """Clean and format column name for PostgreSQL."""
        # Remove or replace problematic characters
        cleaned = col_name.strip()

        if self.lowercase_columns:
            cleaned = cleaned.lower()

        # Handle reserved keywords or special characters if needed
        # You can extend this based on your naming conventions
        return cleaned

    def _map_duckdb_type(self, duckdb_type: str) -> str:
        """Map DuckDB type to PostgreSQL type."""
        # Handle complex types like DECIMAL(10,2)
        base_type = duckdb_type.upper().split("(")[0]

        # Get the mapped type or use the original if not found
        postgres_type = self.DUCKDB_TO_POSTGRES.get(base_type, duckdb_type)

        # Preserve precision/scale for DECIMAL/NUMERIC
        if "(" in duckdb_type and base_type in [
            "DECIMAL",
            "NUMERIC",
            "VARCHAR",
            "CHAR",
        ]:
            precision_part = duckdb_type[duckdb_type.find("(") :]
            postgres_type = postgres_type + precision_part

        return postgres_type

    def _map_arrow_type(self, arrow_type) -> str:
        """Map PyArrow type to PostgreSQL type."""
        type_str = str(arrow_type)

        # Handle timestamp with timezone
        if type_str.startswith("timestamp"):
            if "tz=" in type_str:
                return "TIMESTAMPTZ"
            else:
                return "TIMESTAMP"

        # Handle decimal types
        if type_str.startswith("decimal"):
            return type_str.upper().replace("DECIMAL", "DECIMAL")

        # Get base type
        base_type = type_str.split("[")[0]
        return self.ARROW_TO_POSTGRES.get(base_type, "TEXT")

    def from_duckdb_relation(self, relation, table_name: str) -> str:
        """
        Generate CREATE TABLE statement from DuckDB relation.

        Args:
            relation: DuckDB relation object
            table_name: Name for the PostgreSQL table

        Returns:
            PostgreSQL CREATE TABLE statement
        """
        if not DUCKDB_AVAILABLE:
            raise ImportError("duckdb package is required for this operation")

        columns = relation.columns
        dtypes = [str(dtype) for dtype in relation.dtypes]

        return self._generate_create_table(columns, dtypes, table_name, "duckdb")

    def from_parquet_file(
        self, file_path: str, table_name: Optional[str] = None
    ) -> str:
        """
        Generate CREATE TABLE statement from parquet file.

        Args:
            file_path: Path to parquet file
            table_name: Name for the PostgreSQL table (defaults to filename)

        Returns:
            PostgreSQL CREATE TABLE statement
        """
        file_path = Path(file_path)

        if table_name is None:
            table_name = file_path.stem

        # Try PyArrow first (more efficient for schema-only operations)
        if PYARROW_AVAILABLE:
            try:
                parquet_file = pq.ParquetFile(file_path)
                schema = parquet_file.schema

                columns = schema.names
                dtypes = [str(field.type) for field in schema]

                return self._generate_create_table(columns, dtypes, table_name, "arrow")
            except Exception as e:
                self.logger.warning(f"PyArrow failed: {e}. Trying DuckDB...")

        # Fallback to DuckDB
        if DUCKDB_AVAILABLE:
            try:
                conn = duckdb.connect()
                relation = conn.sql(f"SELECT * FROM '{file_path}' LIMIT 0")
                return self.from_duckdb_relation(relation, table_name)
            except Exception as e:
                self.logger.error(f"DuckDB failed: {e}")
                raise

        raise ImportError("Either pyarrow or duckdb is required to read parquet files")

    def from_pandas_dataframe(self, df, table_name: str) -> str:
        """
        Generate CREATE TABLE statement from pandas DataFrame.

        Args:
            df: Pandas DataFrame
            table_name: Name for the PostgreSQL table

        Returns:
            PostgreSQL CREATE TABLE statement
        """
        columns = df.columns.tolist()
        dtypes = [str(dtype) for dtype in df.dtypes]

        # Convert pandas dtypes to something more standard
        standardized_dtypes = []
        for dtype in dtypes:
            if "int" in dtype:
                if "64" in dtype:
                    standardized_dtypes.append("BIGINT")
                else:
                    standardized_dtypes.append("INTEGER")
            elif "float" in dtype:
                standardized_dtypes.append("DOUBLE")
            elif "bool" in dtype:
                standardized_dtypes.append("BOOLEAN")
            elif "datetime" in dtype:
                standardized_dtypes.append("TIMESTAMP")
            elif "object" in dtype:
                standardized_dtypes.append("VARCHAR")
            else:
                standardized_dtypes.append("TEXT")

        return self._generate_create_table(
            columns, standardized_dtypes, table_name, "pandas"
        )

    def _generate_create_table(
        self, columns: List[str], dtypes: List[str], table_name: str, source_type: str
    ) -> str:
        """
        Generate the actual CREATE TABLE statement.

        Args:
            columns: List of column names
            dtypes: List of data types
            table_name: Table name
            source_type: Type of source ('duckdb', 'arrow', 'pandas')

        Returns:
            CREATE TABLE SQL statement
        """
        column_definitions = []

        for col, dtype in zip(columns, dtypes):
            clean_col = self._clean_column_name(col)

            if source_type == "duckdb":
                postgres_type = self._map_duckdb_type(dtype)
            elif source_type == "arrow":
                postgres_type = self._map_arrow_type(dtype)
            else:  # pandas or other
                postgres_type = self.DUCKDB_TO_POSTGRES.get(dtype.upper(), dtype)

            column_definitions.append(f"    {clean_col} {postgres_type}")

        column_definitions_str = ",\n".join(column_definitions)
        create_table_sql = f"""CREATE TABLE {table_name} (
{column_definitions_str}
);"""

        return create_table_sql

    def generate_sample_indexes(self, columns: List[str], table_name: str) -> str:
        """
        Generate sample index statements for common patterns.

        Args:
            columns: List of column names
            table_name: Table name

        Returns:
            INDEX creation statements
        """
        indexes = []

        # Common patterns for indexing
        datetime_patterns = [
            "datetime",
            "timestamp",
            "date",
            "time",
            "created",
            "updated",
        ]
        id_patterns = ["id", "key", "code"]

        for col in columns:
            col_lower = col.lower()
            clean_col = self._clean_column_name(col)

            # Index datetime columns
            if any(pattern in col_lower for pattern in datetime_patterns):
                indexes.append(
                    f"CREATE INDEX idx_{table_name}_{clean_col.lower()} ON {table_name}({clean_col});"
                )

            # Index ID/key columns
            elif any(pattern in col_lower for pattern in id_patterns):
                indexes.append(
                    f"CREATE INDEX idx_{table_name}_{clean_col.lower()} ON {table_name}({clean_col});"
                )

        if indexes:
            return "\n-- Suggested indexes:\n" + "\n".join(indexes)
        else:
            return "\n-- No obvious index candidates found"


def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Convert parquet schema to PostgreSQL CREATE TABLE"
    )
    parser.add_argument("file", help="Path to parquet file")
    parser.add_argument(
        "table_name", nargs="?", help="PostgreSQL table name (defaults to filename)"
    )
    parser.add_argument(
        "--lowercase", action="store_true", help="Convert column names to lowercase"
    )
    parser.add_argument(
        "--indexes", action="store_true", help="Generate suggested indexes"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    try:
        converter = ParquetToPostgres(lowercase_columns=args.lowercase)

        # Generate CREATE TABLE statement
        create_table_sql = converter.from_parquet_file(args.file, args.table_name)
        print(create_table_sql)

        # Generate suggested indexes if requested
        if args.indexes:
            if PYARROW_AVAILABLE:
                parquet_file = pq.ParquetFile(args.file)
                columns = parquet_file.schema.names
            elif DUCKDB_AVAILABLE:
                conn = duckdb.connect()
                relation = conn.sql(f"SELECT * FROM '{args.file}' LIMIT 0")
                columns = relation.columns
            else:
                print(
                    "\n-- Cannot generate indexes: no parquet reading library available"
                )
                return

            table_name = args.table_name or Path(args.file).stem
            indexes_sql = converter.generate_sample_indexes(columns, table_name)
            print(indexes_sql)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
