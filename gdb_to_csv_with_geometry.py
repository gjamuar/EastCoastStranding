from pathlib import Path

import fiona
import csv
import os
from shapely.geometry import shape

from constants import GDB_FILE_PATH, GDB_EXTRACTED_FILE_PATH


def gdb_to_csv(gdb_path, output_directory):
    # Open the GDB using Fiona
    with fiona.Env():
        layers = fiona.listlayers(gdb_path)
        for layer in layers:
            output_csv_path = os.path.join(output_directory, f"{layer}.csv")
            with fiona.open(gdb_path, 'r', layer=layer) as source:
                # Add 'geometry' to the list of fieldnames for the CSV
                fieldnames = list(source.schema['properties'].keys()) + ['geometry']
                with open(output_csv_path, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for feature in source:
                        # print(feature['id'])
                        # Convert the geometry to WKT format using shapely
                        if 'geometry' in feature and feature['geometry']:
                            geom = shape(feature['geometry'])
                            feature['properties']['geometry'] = geom.wkt
                        writer.writerow(feature['properties'])
            print(f"Data from layer '{layer}' exported to {output_csv_path}")


# if __name__ == "__main__":
#     gdb_path = input("Enter the path to the GDB: ")
#     output_directory = input("Enter the directory where CSV files should be saved: ")
#
#     # Check if the output directory exists, if not, create it
#     if not os.path.exists(output_directory):
#         os.makedirs(output_directory)
#
#     gdb_to_csv(gdb_path, output_directory)

def list_directories(directory):
    # Get a list of all items in the directory
    items = os.listdir(directory)

    # Filter out directories from the list of items
    directories = [item for item in items if os.path.isdir(os.path.join(directory, item))]

    return directories


def process_gdb_to_csv(gdb_path):
    output_path = GDB_EXTRACTED_FILE_PATH + gdb_path.split(".")[0]
    directory_path = Path(output_path)

    directory_path.mkdir(parents=True, exist_ok=True)

    gdb_to_csv(GDB_FILE_PATH+gdb_path, directory_path)


if __name__ == '__main__':
    gdb_files = list_directories(GDB_FILE_PATH)
    print("Directories in", GDB_FILE_PATH, ":", gdb_files)
    # gdb_files = ['Zone9_2014_08.gdb']
    for gdb_file in gdb_files:
        process_gdb_to_csv(gdb_path=gdb_file)

