import os
import pandas as pd
from pathlib import Path


def sort_and_split_parquets():
    # Define the directory containing the parquet files
    directory_path = 'data/gdb_parquet'
    output_directory = 'data/gdb_parquet_sorted'

    # Step 1: Read all parquet files in the directory into one DataFrame
    all_files = Path(directory_path).rglob('*.parquet')
    df_list = [pd.read_parquet(file) for file in all_files]
    combined_df = pd.concat(df_list, ignore_index=True)

    # Step 2: Sort the DataFrame by the datetime field in ascending order
    combined_df['datetime_field'] = pd.to_datetime(combined_df['BaseDateTime'])  # Ensure it's datetime type
    sorted_df = combined_df.sort_values(by='datetime_field')

    # Step 3: Split the DataFrame into different DataFrames for each day
    sorted_df['date'] = sorted_df['datetime_field'].dt.date  # Extract date part
    grouped = sorted_df.groupby('date')

    # Step 4: Save each daily DataFrame into a separate parquet file
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for date, group in grouped:
        output_file = os.path.join(output_directory,  f'AIS_{date.strftime("%Y_%m_%d")}.parquet')
        group.drop(columns=['date', 'datetime_field'], inplace=True)  # Drop the extra 'date' column
        group.to_parquet(output_file, index=False)

    print(f"Data successfully split and saved to {output_directory}")


if __name__ == '__main__':
    sort_and_split_parquets()
