import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def load_and_plot_data(csv_path1, csv_path2, output_graph_path):
    # Load data from CSV files
    df1 = pd.read_csv(csv_path1)
    df2 = pd.read_csv(csv_path2)

    # Merge the dataframes on the 'File/Table' column and sort alphabetically
    merged_df = df1.merge(df2, on='File/Table', suffixes=('_v0', '_v1'))
    merged_df.sort_values('File/Table', inplace=True)  # Sort alphabetically by table names

    # Set the figure size
    plt.figure(figsize=(12, 6))

    # Define the bar width and positions
    bar_width = 0.35
    index = np.arange(len(merged_df))
    
    # Plotting the bar chart
    bars1 = plt.bar(index, merged_df['Execution Time (s)_v0'], bar_width, label='v0', alpha=0.8)
    bars2 = plt.bar(index + bar_width, merged_df['Execution Time (s)_v1'], bar_width, label='v1', alpha=0.8)

    # Adding labels, title, and custom x-axis tick labels
    plt.xlabel('Query')
    plt.ylabel('Execution Time (s)')
    plt.title('Comparison of Execution Times from withoutIndex(v0) and withIndex(v1)')
    plt.xticks(index + bar_width / 2, merged_df['File/Table'], rotation=45)
    
    # Adding a legend
    plt.legend()

    # Ensure everything is visible by adjusting the layout
    plt.tight_layout()

    # Save the plot to a file
    plt.savefig(output_graph_path)
    plt.close()  # Close the figure to free up memory

# Paths to the CSV files and the output graph
csv_file_1 = '/home/xuzhilong/gaowanen/tpchResults/tpch_v0_without_index/execution_times.csv'
csv_file_2 = '/home/xuzhilong/gaowanen/tpchResults/tpch_v1_with_index/execution_times.csv'
output_graph = '/home/xuzhilong/gaowanen/tpchResults/execution_times_comparison_barchart.png'

# Call the function with the specified paths
load_and_plot_data(csv_file_1, csv_file_2, output_graph)
