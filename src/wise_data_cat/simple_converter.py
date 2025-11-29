#!/usr/bin/env python3
"""
Simple script to convert your DuckDB relation to PostgreSQL CREATE TABLE statement
"""


def duckdb_relation_to_postgres_sql(relation, table_name: str) -> str:
    """
    Convert DuckDB relation to PostgreSQL CREATE TABLE statement.

    Args:
        relation: Your DuckDB relation (df)
        table_name: Name for the PostgreSQL table

    Returns:
        PostgreSQL CREATE TABLE statement
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
        "UUID": "UUID",
        "JSON": "JSONB",
    }

    # Get columns and types from your relation
    columns = relation.columns
    dtypes = [str(dtype) for dtype in relation.dtypes]

    # Generate column definitions
    column_definitions = []
    for col, dtype in zip(columns, dtypes):
        # Map the type
        postgres_type = type_mapping.get(dtype.upper(), dtype)
        column_definitions.append(f"    {col} {postgres_type}")

    # Generate CREATE TABLE statement
    column_definitions_str = ",\n".join(column_definitions)
    create_table_sql = f"""CREATE TABLE {table_name} (
{column_definitions_str}
);"""

    return create_table_sql


def generate_indexes_for_taxi_data(table_name: str = "taxi_trips") -> str:
    """Generate useful indexes for taxi trip data."""

    indexes = f"""
-- Suggested indexes for {table_name}:
CREATE INDEX idx_{table_name}_pickup_datetime ON {table_name}(tpep_pickup_datetime);
CREATE INDEX idx_{table_name}_dropoff_datetime ON {table_name}(tpep_dropoff_datetime);
CREATE INDEX idx_{table_name}_vendor_id ON {table_name}(VendorID);
CREATE INDEX idx_{table_name}_pu_location ON {table_name}(PULocationID);
CREATE INDEX idx_{table_name}_do_location ON {table_name}(DOLocationID);
CREATE INDEX idx_{table_name}_payment_type ON {table_name}(payment_type);

-- Composite indexes for common query patterns:
CREATE INDEX idx_{table_name}_pickup_vendor ON {table_name}(tpep_pickup_datetime, VendorID);
CREATE INDEX idx_{table_name}_location_pair ON {table_name}(PULocationID, DOLocationID);
"""

    return indexes


# Example usage with your data:
if __name__ == "__main__":
    print("=== Usage with your DuckDB relation ===")
    print(
        """
# Assuming you have your df relation loaded:
# df = duckdb.sql("SELECT * FROM 'your_parquet_file.parquet'")

from simple_converter import duckdb_relation_to_postgres_sql, generate_indexes_for_taxi_data

# Generate CREATE TABLE statement
create_table_sql = duckdb_relation_to_postgres_sql(df, 'taxi_trips')
print(create_table_sql)

# Generate indexes
indexes_sql = generate_indexes_for_taxi_data('taxi_trips')
print(indexes_sql)

# Save to file
with open('taxi_schema.sql', 'w') as f:
    f.write(create_table_sql)
    f.write('\\n')
    f.write(indexes_sql)

print("Schema saved to taxi_schema.sql")
"""
    )

    print("\n=== Example output ===")

    # Simulate your data structure for demonstration
    class MockRelation:
        columns = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "Airport_fee",
            "cbd_congestion_fee",
        ]

        dtypes = [
            "INTEGER",
            "TIMESTAMP",
            "TIMESTAMP",
            "BIGINT",
            "DOUBLE",
            "BIGINT",
            "VARCHAR",
            "INTEGER",
            "INTEGER",
            "BIGINT",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
            "DOUBLE",
        ]

    mock_df = MockRelation()

    # Generate the SQL
    sql = duckdb_relation_to_postgres_sql(mock_df, "taxi_trips")
    print(sql)

    # Generate indexes
    indexes = generate_indexes_for_taxi_data("taxi_trips")
    print(indexes)
