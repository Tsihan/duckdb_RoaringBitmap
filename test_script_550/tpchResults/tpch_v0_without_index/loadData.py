import os
import duckdb
import time
import csv

def run_sql_from_file(conn, filepath):
    """Execute SQL statements read from a file and measure execution time."""
    start_time = time.time()
    with open(filepath, 'r') as file:
        sql_script = file.read()
    conn.execute(sql_script)
    end_time = time.time()
    return os.path.basename(filepath), end_time - start_time  # Return filename and execution time in seconds

def load_data_from_tbl(conn, table_name, file_path):
    """Load data from a .tbl file into the specified table and measure execution time."""
    start_time = time.time()
    conn.execute(f"COPY {table_name} FROM '{file_path}' (DELIMITER '|', HEADER FALSE);")
    end_time = time.time()
    return table_name, end_time - start_time  # Return table name and execution time in seconds

def write_execution_times_to_csv(execution_times, csv_file_path):
    """Write the execution times to a CSV file."""
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['File/Table', 'Execution Time (s)'])
        for execution_time in execution_times:
            writer.writerow(execution_time)

def main():
    # Connect to DuckDB (change to a file path if persistence is required)
    conn = duckdb.connect(database='/home/xuzhilong/gaowanen/tpch_data/duckv0.db')

    # List to store execution times and file names
    execution_times = []

    # Create tables by executing SQL from the 'tpch-schema.sql' file
    # create_tables_sql_path = '/home/xuzhilong/gaowanen/tpchData/tpch-schema.sql'
    # execution_times.append(run_sql_from_file(conn, create_tables_sql_path))

    # Dictionary mapping table names to their respective .tbl file paths
    tbl_files = {
        'customer': '/home/xuzhilong/gaowanen/tpchData/customer.tbl',
        'lineitem': '/home/xuzhilong/gaowanen/tpchData/lineitem.tbl',
        'nation': '/home/xuzhilong/gaowanen/tpchData/nation.tbl',
        'orders': '/home/xuzhilong/gaowanen/tpchData/orders.tbl',
        'part': '/home/xuzhilong/gaowanen/tpchData/part.tbl',
        'partsupp': '/home/xuzhilong/gaowanen/tpchData/partsupp.tbl',
        'region': '/home/xuzhilong/gaowanen/tpchData/region.tbl',
        'supplier': '/home/xuzhilong/gaowanen/tpchData/supplier.tbl'
    }

    # Load data into each table and record execution times
    # for table, tbl_file_path in tbl_files.items():
    #     execution_times.append(load_data_from_tbl(conn, table, tbl_file_path))

    # Directory containing all your SQL query files
    query_dir = '/home/xuzhilong/gaowanen/tpchData/sample_queries_tpch'

    # Execute all query files and record execution times
    for query_file in sorted(os.listdir(query_dir)):
        if query_file.endswith('.sql'):
            file_path = os.path.join(query_dir, query_file)
            execution_times.append(run_sql_from_file(conn, file_path))

    # Write execution times to a CSV file
    csv_file_path = '/home/xuzhilong/gaowanen/tpchResults/tpch_v0_without_index/execution_times.csv'
    write_execution_times_to_csv(execution_times, csv_file_path)

if __name__ == "__main__":
    main()
