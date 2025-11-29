#!/usr/bin/env python3
"""
Parquet Sampler - Extract samples from parquet files

Usage:
    python parquet_sampler.py input.parquet output.parquet --rows 100
    python parquet_sampler.py input.parquet output.parquet --percentage 1.0
"""

import argparse
import sys
from pathlib import Path
import duckdb


def sample_parquet_file(
    input_file: str,
    output_file: str,
    num_rows: int = None,
    percentage: float = None,
    order_by: str = None,
    random_sample: bool = False,
):
    """
    Sample rows from a parquet file and save to another parquet file.

    Args:
        input_file: Input parquet file path
        output_file: Output parquet file path
        num_rows: Number of rows to sample (mutually exclusive with percentage)
        percentage: Percentage of rows to sample (0.0-100.0)
        order_by: Column to order by before sampling
        random_sample: Whether to use random sampling
    """

    conn = duckdb.connect()

    try:
        # Build the query
        if random_sample:
            if percentage:
                query = f"SELECT * FROM '{input_file}' USING SAMPLE {percentage}%"
            else:
                # For random sampling with fixed count, we need total count first
                total_rows = conn.execute(
                    f"SELECT COUNT(*) FROM '{input_file}'"
                ).fetchone()[0]
                sample_percentage = (num_rows / total_rows) * 100
                query = (
                    f"SELECT * FROM '{input_file}' USING SAMPLE {sample_percentage}%"
                )
        else:
            # Sequential sampling
            order_clause = f"ORDER BY {order_by}" if order_by else ""

            if percentage:
                total_rows = conn.execute(
                    f"SELECT COUNT(*) FROM '{input_file}'"
                ).fetchone()[0]
                limit_rows = int(total_rows * percentage / 100)
                query = (
                    f"SELECT * FROM '{input_file}' {order_clause} LIMIT {limit_rows}"
                )
            else:
                query = f"SELECT * FROM '{input_file}' {order_clause} LIMIT {num_rows}"

        # Execute the sampling
        full_query = f"COPY ({query}) TO '{output_file}' (FORMAT PARQUET)"
        conn.execute(full_query)

        # Get info about what was sampled
        result_count = conn.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[
            0
        ]
        original_count = conn.execute(
            f"SELECT COUNT(*) FROM '{input_file}'"
        ).fetchone()[0]

        print(f"Sampled {result_count:,} rows from {original_count:,} total rows")
        print(f"Sample rate: {(result_count/original_count)*100:.2f}%")
        print(f"Output saved to: {output_file}")

    finally:
        conn.close()


def get_parquet_info(file_path: str):
    """Get basic info about a parquet file."""
    conn = duckdb.connect()
    try:
        # Get row count
        count = conn.execute(f"SELECT COUNT(*) FROM '{file_path}'").fetchone()[0]

        # Get columns
        schema = conn.execute(f"DESCRIBE SELECT * FROM '{file_path}'").fetchall()

        print(f"File: {file_path}")
        print(f"Rows: {count:,}")
        print(f"Columns: {len(schema)}")
        print("Schema:")
        for col_name, col_type, null, key, default, extra in schema:
            print(f"  {col_name}: {col_type}")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Sample rows from parquet files")
    parser.add_argument("input_file", help="Input parquet file")
    parser.add_argument("output_file", help="Output parquet file")

    # Sampling options (mutually exclusive)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--rows", "-n", type=int, help="Number of rows to sample")
    group.add_argument(
        "--percentage",
        "-p",
        type=float,
        help="Percentage of rows to sample (0.0-100.0)",
    )

    # Additional options
    parser.add_argument("--order-by", help="Column to order by before sampling")
    parser.add_argument("--random", action="store_true", help="Use random sampling")
    parser.add_argument("--info", action="store_true", help="Show input file info")

    args = parser.parse_args()

    # Validate inputs
    if not Path(args.input_file).exists():
        print(f"Error: Input file '{args.input_file}' not found", file=sys.stderr)
        sys.exit(1)

    if args.percentage and (args.percentage < 0 or args.percentage > 100):
        print("Error: Percentage must be between 0.0 and 100.0", file=sys.stderr)
        sys.exit(1)

    try:
        # Show file info if requested
        if args.info:
            get_parquet_info(args.input_file)
            print()

        # Perform sampling
        sample_parquet_file(
            input_file=args.input_file,
            output_file=args.output_file,
            num_rows=args.rows,
            percentage=args.percentage,
            order_by=args.order_by,
            random_sample=args.random,
        )

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
