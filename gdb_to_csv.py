# pip install gdal pandas

from osgeo import ogr
import pandas as pd
import os
from pathlib import Path

from constants import GDB_FILE_PATH, GDB_EXTRACTED_FILE_PATH


def parse_gdb_to_csv(gdb_path):
    # Open the GDB file
    driver = ogr.GetDriverByName('OpenFileGDB')
    gdb = driver.Open(GDB_FILE_PATH+gdb_path, 0)  # 0 means read-only mode
    # Loop through the layers in the GDB and export each to CSV
    output_path = GDB_EXTRACTED_FILE_PATH + gdb_path.split(".")[0]
    directory_path = Path(output_path)

    directory_path.mkdir(parents=True, exist_ok=True)

    for layer_num in range(gdb.GetLayerCount()):
        layer = gdb.GetLayerByIndex(layer_num)
        layer_name = layer.GetName()
        features = []

        # Fetch each feature from the layer
        for feature in layer:
            features.append(feature.items())

        # Create a DataFrame from the features and write it to CSV
        df = pd.DataFrame(features)
        df.to_csv(f'{output_path}/{layer_name}.csv', index=False)
        print(f'Saved {layer_name}.csv')
    # Close the GDB
    gdb = None


def list_directories(directory):
    # Get a list of all items in the directory
    items = os.listdir(directory)

    # Filter out directories from the list of items
    directories = [item for item in items if os.path.isdir(os.path.join(directory, item))]

    return directories


if __name__ == '__main__':
    gdb_files = list_directories(GDB_FILE_PATH)
    print("Directories in", GDB_FILE_PATH, ":", gdb_files)
    # gdb_files = ['Zone9_2014_08.gdb']
    for gdb_file in gdb_files:
        parse_gdb_to_csv(gdb_path=gdb_file)
