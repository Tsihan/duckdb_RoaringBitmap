import duckdb
import os
import time
import csv  # Import the csv module for writing to CSV files

# Initialize DuckDB connection
con = duckdb.connect(database=':memory:')

def execute_sql_file(file_path, csv_writer):
    with open(file_path, 'r') as file:
        sql_content = file.read()
        start_time = time.time()
        con.execute(sql_content)
        end_time = time.time()
        execution_time = end_time - start_time
        file_name = os.path.basename(file_path)  # Get only the file name
        csv_writer.writerow([file_name, f"{execution_time:.3f}"])  # Write to CSV with formatted time
        print(f"Execution time for {file_name}: {execution_time:.3f} seconds")

def main():
    # Open a CSV file to write execution times
    with open('execution_times.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['File Name', 'Execution Time (Seconds)'])  # Writing header to CSV file

        # Create the schema
        execute_sql_file('/home/xuzhilong/gaowanen/imdbData/schema.sql', csv_writer)

        # Load the data
        execute_sql_file('/home/xuzhilong/gaowanen/imdbData/load.sql', csv_writer)

        # Create Index
        execute_sql_file('/home/xuzhilong/gaowanen/imdbData/fkindexes.sql', csv_writer)

        # # Add FK
        # execute_sql_file('/home/xuzhilong/gaowanen/imdbData/add_fks.sql', csv_writer)


        # Execute queries
        queries_dir = '/home/xuzhilong/duckdb/benchmark/imdb_plan_cost/queries/'
        for query_file in os.listdir(queries_dir):
            if query_file.endswith('.sql'):
                print(f'Executing {query_file}...')
                execute_sql_file(os.path.join(queries_dir, query_file), csv_writer)

    # Close the connection
    con.close()

if __name__ == '__main__':
    main()
