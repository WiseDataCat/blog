#!/usr/bin/env python3
"""
Example usage of the ParquetToPostgres converter
"""

from parquet_to_postgres import ParquetToPostgres


def example_with_your_data():
    """Example using your taxi data structure"""

    # Simulate your DuckDB relation (replace with your actual data loading)
    # This would be your: df = duckdb.sql("SELECT * FROM 'your_file.parquet'")

    # Create converter
    converter = ParquetToPostgres(lowercase_columns=False)  # Keep original case

    # Method 1: From DuckDB relation (what you have)
    # Assuming you have your df relation loaded:
    """
    sql = converter.from_duckdb_relation(df, 'taxi_trips')
    print("=== CREATE TABLE from DuckDB relation ===")
    print(sql)
    """

    # Method 2: From parquet file directly
    try:
        sql = converter.from_parquet_file("taxi_data.parquet", "taxi_trips")
        print("=== CREATE TABLE from parquet file ===")
        print(sql)

        # Generate suggested indexes
        columns = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
        ]
        indexes = converter.generate_sample_indexes(columns, "taxi_trips")
        print(indexes)

    except FileNotFoundError:
        print("Parquet file not found - this is just an example")
    except ImportError as e:
        print(f"Missing dependency: {e}")


def example_usage_patterns():
    """Show different usage patterns"""

    _converter = ParquetToPostgres()

    print("=== Usage Examples ===\n")

    print("1. From command line:")
    print("   python parquet_to_postgres.py data.parquet my_table")
    print("   python parquet_to_postgres.py data.parquet --lowercase --indexes")
    print()

    print("2. From Python code:")
    print(
        """
   from parquet_to_postgres import ParquetToPostgres
   
   converter = ParquetToPostgres()
   
   # From parquet file
   sql = converter.from_parquet_file('data.parquet', 'my_table')
   
   # From DuckDB relation (your use case)
   df = duckdb.sql("SELECT * FROM 'data.parquet'")
   sql = converter.from_duckdb_relation(df, 'my_table')
   
   # From pandas DataFrame
   sql = converter.from_pandas_dataframe(pandas_df, 'my_table')
   
   print(sql)
"""
    )

    print("3. Your specific example:")
    print(
        """
   # With your existing DuckDB relation:
   converter = ParquetToPostgres()
   create_table_sql = converter.from_duckdb_relation(df, 'taxi_trips')
   
   # Save to file
   with open('create_taxi_table.sql', 'w') as f:
       f.write(create_table_sql)
   
   # Or execute directly with psycopg
   import psycopg
   with psycopg.connect("postgresql://localhost:5432/your_db") as conn:
       conn.execute(create_table_sql)
"""
    )


def test_type_mappings():
    """Test the type mapping functionality"""

    converter = ParquetToPostgres()

    print("=== Type Mapping Tests ===\n")

    # Test DuckDB type mappings
    duckdb_types = [
        "INTEGER",
        "BIGINT",
        "DOUBLE",
        "VARCHAR",
        "TIMESTAMP",
        "DECIMAL(10,2)",
    ]

    print("DuckDB -> PostgreSQL mappings:")
    for dtype in duckdb_types:
        mapped = converter._map_duckdb_type(dtype)
        print(f"  {dtype:15} -> {mapped}")

    print()


if __name__ == "__main__":
    example_usage_patterns()
    print()
    test_type_mappings()
    print()
    example_with_your_data()
