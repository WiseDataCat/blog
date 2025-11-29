#!/usr/bin/env python3
"""
Parquet to PostgreSQL Loader

This script automates the process of:
1. Reading parquet file schema
2. Creating PostgreSQL table
3. Loading data from parquet into PostgreSQL

Supports multiple loading strategies for different data sizes.
"""

import argparse
import sys
import logging
from pathlib import Path
import duckdb
import psycopg
import tempfile


class ParquetToPostgresLoader:
    """Load parquet data into PostgreSQL using DuckDB."""

    def __init__(self, pg_connection_string: str):
        """
        Initialize the loader.

        Args:
            pg_connection_string: PostgreSQL connection string
        """
        self.pg_conn_str = pg_connection_string
        self.logger = logging.getLogger(__name__)

    def create_table_from_parquet(
        self, parquet_file: str, table_name: str, drop_if_exists: bool = False
    ) -> str:
        """
        Create PostgreSQL table from parquet schema.

        Args:
            parquet_file: Path to parquet file
            table_name: PostgreSQL table name
            drop_if_exists: Whether to drop table if it exists

        Returns:
            CREATE TABLE SQL statement used
        """
        # Type mapping from DuckDB to PostgreSQL
        type_mapping = {
            "INTEGER": "INTEGER",
            "BIGINT": "BIGINT",
            "SMALLINT": "SMALLINT",
            "TINYINT": "SMALLINT",
            "DOUBLE": "DOUBLE PRECISION",
            "REAL": "REAL",
            "FLOAT": "REAL",
            "DECIMAL": "DECIMAL",
            "NUMERIC": "NUMERIC",
            "VARCHAR": "TEXT",  # Use TEXT for flexibility
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
            "UUID": "UUID",
            "JSON": "JSONB",
        }

        # Get schema from parquet using DuckDB
        conn = duckdb.connect()
        try:
            # Get schema without loading data
            relation = conn.execute(
                f"DESCRIBE SELECT * FROM '{parquet_file}'"
            ).fetchall()

            # Build column definitions
            column_definitions = []
            for row in relation:
                col_name = row[0]
                col_type = row[1].upper()

                # Handle complex types like DECIMAL(10,2)
                base_type = col_type.split("(")[0]
                postgres_type = type_mapping.get(base_type, col_type)

                # Preserve precision for DECIMAL/NUMERIC
                if "(" in col_type and base_type in ["DECIMAL", "NUMERIC"]:
                    precision_part = col_type[col_type.find("(") :]
                    postgres_type = postgres_type + precision_part

                column_definitions.append(f"    {col_name} {postgres_type}")

            # Generate CREATE TABLE statement
            drop_sql = f"DROP TABLE IF EXISTS {table_name};" if drop_if_exists else ""
            column_definitions_str = ",\n".join(column_definitions)
            create_sql = f"""CREATE TABLE {table_name} (
{column_definitions_str}
);"""

            full_sql = drop_sql + "\n" + create_sql if drop_sql else create_sql

            # Execute in PostgreSQL
            with psycopg.connect(self.pg_conn_str) as pg_conn:
                with pg_conn.cursor() as cur:
                    if drop_if_exists:
                        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                    cur.execute(create_sql)
                    pg_conn.commit()

            self.logger.info(f"Created table {table_name}")
            return full_sql

        finally:
            conn.close()

    def load_data_direct_duckdb(self, parquet_file: str, table_name: str):
        """
        Load data using DuckDB's PostgreSQL extension (requires postgres extension).

        Args:
            parquet_file: Path to parquet file
            table_name: PostgreSQL table name
        """
        conn = duckdb.connect()
        try:
            # Install and load postgres extension
            conn.execute("INSTALL postgres")
            conn.execute("LOAD postgres")

            # Parse connection string to build DuckDB postgres attach
            # This is a simple parser - you might want to use urllib.parse for robustness
            if "postgresql://" in self.pg_conn_str:
                # Extract components for DuckDB attach
                _attach_str = self.pg_conn_str.replace("postgresql://", "")

                # Attach PostgreSQL database
                conn.execute(
                    f"ATTACH '{self.pg_conn_str}' AS postgres_db (TYPE POSTGRES)"
                )

                # Insert data
                conn.execute(
                    f"""
                    INSERT INTO postgres_db.{table_name} 
                    SELECT * FROM '{parquet_file}'
                """
                )

                self.logger.info(
                    f"Data loaded into {table_name} using DuckDB direct method"
                )
            else:
                raise ValueError(
                    "Connection string must be in postgresql:// format for direct DuckDB method"
                )

        finally:
            conn.close()

    def load_data_copy(
        self, parquet_file: str, table_name: str, chunk_size: int = 100000
    ):
        """
        Load data using PostgreSQL COPY via DuckDB CSV export.

        Args:
            parquet_file: Path to parquet file
            table_name: PostgreSQL table name
            chunk_size: Number of rows to process at once
        """
        conn = duckdb.connect()

        try:
            # Create temporary CSV file
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".csv", delete=False
            ) as temp_file:
                csv_path = temp_file.name

            # Export parquet to CSV using DuckDB
            conn.execute(
                f"""
                COPY (SELECT * FROM '{parquet_file}') 
                TO '{csv_path}' (HEADER false, DELIMITER ',', QUOTE '"')
            """
            )

            # Get row count for progress tracking
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM '{parquet_file}'"
            ).fetchone()[0]

            # Use PostgreSQL COPY to load CSV
            with psycopg.connect(self.pg_conn_str) as pg_conn:
                with pg_conn.cursor() as cur:
                    with open(csv_path, "r") as csv_file:
                        with cur.copy(f"COPY {table_name} FROM STDIN WITH CSV") as copy:
                            copy.write(csv_file.read())

                    pg_conn.commit()

            # Clean up temporary file
            Path(csv_path).unlink()

            self.logger.info(
                f"Loaded {row_count:,} rows into {table_name} using COPY method"
            )

        finally:
            conn.close()

    def load_data_batch_insert(
        self, parquet_file: str, table_name: str, batch_size: int = 10000
    ):
        """
        Load data using batch INSERT statements.

        Args:
            parquet_file: Path to parquet file
            table_name: PostgreSQL table name
            batch_size: Number of rows per batch
        """
        conn = duckdb.connect()

        try:
            # Get total row count
            total_rows = conn.execute(
                f"SELECT COUNT(*) FROM '{parquet_file}'"
            ).fetchone()[0]

            # Get column names
            columns = [
                row[0]
                for row in conn.execute(
                    f"DESCRIBE SELECT * FROM '{parquet_file}'"
                ).fetchall()
            ]

            with psycopg.connect(self.pg_conn_str) as pg_conn:
                with pg_conn.cursor() as cur:
                    # Process in batches
                    for offset in range(0, total_rows, batch_size):
                        # Fetch batch from parquet
                        batch_data = conn.execute(
                            f"""
                            SELECT * FROM '{parquet_file}' 
                            LIMIT {batch_size} OFFSET {offset}
                        """
                        ).fetchall()

                        if not batch_data:
                            break

                        # Prepare INSERT statement
                        placeholders = ",".join(["%s"] * len(columns))
                        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

                        # Execute batch insert
                        cur.executemany(insert_sql, batch_data)
                        pg_conn.commit()

                        self.logger.info(
                            f"Loaded batch {offset//batch_size + 1}: {offset + len(batch_data):,}/{total_rows:,} rows"
                        )

            self.logger.info(f"Completed loading {total_rows:,} rows into {table_name}")

        finally:
            conn.close()

    def load_parquet_to_postgres(
        self,
        parquet_file: str,
        table_name: str,
        method: str = "copy",
        drop_if_exists: bool = False,
        create_indexes: bool = True,
    ):
        """
        Complete workflow: create table and load data.

        Args:
            parquet_file: Path to parquet file
            table_name: PostgreSQL table name
            method: Loading method ('copy', 'direct', 'batch')
            drop_if_exists: Whether to drop/recreate table
            create_indexes: Whether to create suggested indexes
        """
        self.logger.info(
            f"Starting parquet-to-postgres load: {parquet_file} -> {table_name}"
        )

        # Step 1: Create table
        _create_sql = self.create_table_from_parquet(
            parquet_file, table_name, drop_if_exists
        )

        # Step 2: Load data
        if method == "direct":
            self.load_data_direct_duckdb(parquet_file, table_name)
        elif method == "copy":
            self.load_data_copy(parquet_file, table_name)
        elif method == "batch":
            self.load_data_batch_insert(parquet_file, table_name)
        else:
            raise ValueError(f"Unknown method: {method}")

        # Step 3: Create indexes (optional)
        if create_indexes:
            self.create_suggested_indexes(table_name)

        self.logger.info(
            f"Successfully loaded {parquet_file} into PostgreSQL table {table_name}"
        )

    def create_suggested_indexes(self, table_name: str):
        """Create suggested indexes based on column names."""
        with psycopg.connect(self.pg_conn_str) as pg_conn:
            with pg_conn.cursor() as cur:
                # Get column names
                cur.execute(
                    """
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """,
                    (table_name,),
                )

                columns = cur.fetchall()

                # Create indexes for common patterns
                indexes_created = 0
                for col_name, data_type in columns:
                    col_lower = col_name.lower()

                    # Index datetime columns
                    if any(
                        pattern in col_lower
                        for pattern in ["datetime", "timestamp", "date", "time"]
                    ):
                        try:
                            cur.execute(
                                f"CREATE INDEX idx_{table_name}_{col_name.lower()} ON {table_name}({col_name})"
                            )
                            indexes_created += 1
                        except psycopg.Error:
                            pass  # Index might already exist

                    # Index ID columns
                    elif (
                        any(pattern in col_lower for pattern in ["id", "key", "code"])
                        and col_lower != "id"
                    ):
                        try:
                            cur.execute(
                                f"CREATE INDEX idx_{table_name}_{col_name.lower()} ON {table_name}({col_name})"
                            )
                            indexes_created += 1
                        except psycopg.Error:
                            pass

                pg_conn.commit()
                self.logger.info(f"Created {indexes_created} indexes on {table_name}")


def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(description="Load parquet file into PostgreSQL")
    parser.add_argument("parquet_file", help="Path to parquet file")
    parser.add_argument("table_name", help="PostgreSQL table name")
    parser.add_argument(
        "--connection",
        "-c",
        required=True,
        help="PostgreSQL connection string (postgresql://user:pass@host:port/db)",
    )
    parser.add_argument(
        "--method",
        choices=["copy", "direct", "batch"],
        default="copy",
        help="Loading method (default: copy)",
    )
    parser.add_argument("--drop", action="store_true", help="Drop table if exists")
    parser.add_argument(
        "--no-indexes", action="store_true", help="Skip creating indexes"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    try:
        loader = ParquetToPostgresLoader(args.connection)
        loader.load_parquet_to_postgres(
            parquet_file=args.parquet_file,
            table_name=args.table_name,
            method=args.method,
            drop_if_exists=args.drop,
            create_indexes=not args.no_indexes,
        )
        print(f"Successfully loaded {args.parquet_file} into {args.table_name}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
