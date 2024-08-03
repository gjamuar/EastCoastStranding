import logging
import multiprocessing
import os
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from dateutil.rrule import rrule, DAILY

import database as db
import utils
from constants import DOWNLOAD_PATH, CSV_PATH, PARQUET_PATH, GDB_FILE_PATH, GDB_EXTRACTED_FILE_PATH, \
    GDB_PARQUET_FILE_PATH
from gdb_to_csv import parse_gdb_to_csv

logging.basicConfig(filename='app.log',
                    filemode='w',
                    format='%(asctime)s - %(levelname)s-8s %(name)s  - %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')


def extract_and_push(file):
    logging.debug(f"processing file {file}")
    df = pd.read_csv(file, skip_blank_lines=True, on_bad_lines='skip', low_memory=False)
    east_coast_df = df[(df['LAT'] >= 36.5) & (df['LAT'] <= 46.0) &
                       (df['LON'] >= -87.0) & (df['LON'] <= -65.0)]
    qr = east_coast_df[(east_coast_df['Status'] == 3)]
    '''
    select distinct(vessel_name),call_sign,vessel_type,cargo,length,width from vessels where cargo != 55 
    and cargo != 52 and ( cargo < 30 or cargo > 37) and cargo != 0 and (cargo >69 or cargo <60 ) 
    and cargo != 51 and ( vessel_type > 69 or vessel_type <60 ) and vessel_name not like '%CG %';

    '''
    qr = qr.query(" VesselType >= 90 or Cargo >= 90 or VesselType == 70")
    qr = qr.query(" VesselType > 69 or VesselType < 60 ")
    qr = qr.query(
        " (Cargo < 51 or Cargo > 55 ) and Cargo != 58 and (Cargo > 69 or Cargo <60 ) and ( Cargo < 30 or Cargo > 37) ")
    qr = qr.query('not VesselName.str.contains("CG ")')
    qr.to_parquet(PARQUET_PATH + file.split('/')[-1].split('.')[0] + '.parquet')

    # db.push_data(qr)
    logging.debug(f"processing done {file}")


def process_gdb_csv(zone):
    print(f'Processing {zone}')
    df_brd_cast = pd.read_csv(f"{GDB_EXTRACTED_FILE_PATH}{zone}/{zone}_Broadcast.csv", skip_blank_lines=True, on_bad_lines='skip',
                              low_memory=False)
    print(df_brd_cast.shape)
    print(df_brd_cast.head().to_string())

    df_brd_cast = df_brd_cast[(df_brd_cast['Status'] == 3)]
    print(df_brd_cast.head().to_string())
    print(df_brd_cast.shape)

    df_vessel = pd.read_csv(f"{GDB_EXTRACTED_FILE_PATH}{zone}/{zone}_Vessel.csv", skip_blank_lines=True, on_bad_lines='skip',
                            low_memory=False)

    print(df_vessel.head().to_string())

    df_vessel = df_vessel.drop('geometry', axis=1)

    df_voyage = pd.read_csv(f"{GDB_EXTRACTED_FILE_PATH}{zone}/{zone}_Voyage.csv", skip_blank_lines=True, on_bad_lines='skip',
                            low_memory=False)
    print(df_voyage.head().to_string())
    df_voyage = df_voyage.drop('geometry', axis=1)

    left_merged = pd.merge(
        df_brd_cast, df_vessel, how="inner", on=["MMSI"]
    )

    final_merged = pd.merge(
        left_merged, df_voyage, how="inner", on=["VoyageID", "MMSI"]
    )

    final_merged = final_merged.query(" VesselType >= 90 or Cargo >= 90 or VesselType == 70")
    final_merged = final_merged.query(" VesselType > 69 or VesselType < 60 ")
    final_merged = final_merged.query(
        " (Cargo < 51 or Cargo > 55 ) and Cargo != 58 and (Cargo > 69 or Cargo <60 ) and ( Cargo < 30 or Cargo > 37) ")

    print(final_merged.shape)
    print(final_merged.columns)
    print(final_merged.head(50).to_string())
    if final_merged.shape[0] <= 0:
        logging.info(f'Skipping {zone}')
        print(f'Skipping {zone}')
        return

    final_merged['geometry'] = final_merged['geometry'].str.replace('POINT (', '')
    final_merged['geometry'] = final_merged['geometry'].str.replace(')', '')
    final_merged[['LON', 'LAT']] = final_merged['geometry'].str.split(' ', expand=True)
    final_merged['LON'] = final_merged['LON'].astype(float)
    final_merged['LAT'] = final_merged['LAT'].astype(float)

    # east_coast_df = final_merged[(final_merged['LAT'] >= 36.5) & (final_merged['LAT'] <= 46.0) &
    #                              (final_merged['LON'] >= -87.0) & (final_merged['LON'] <= -65.0)]

    final_merged[['SOG', 'COG', 'Heading', 'BaseDateTime', 'Status',
                  'MMSI', 'geometry', 'IMO', 'CallSign',
                  'Name', 'VesselType', 'Length', 'Width',
                  'Cargo', 'LON', 'LAT']].to_parquet(f'{GDB_PARQUET_FILE_PATH}{zone}.parquet')


def list_directories(directory):
    # Get a list of all items in the directory
    items = os.listdir(directory)

    # Filter out directories from the list of items
    directories = [item for item in items if os.path.isdir(os.path.join(directory, item))]

    return directories


if __name__ == '__main__':
    # parse_gdb_to_csv('Zone10_2014_01')
    # process_gdb_csv('Zone10_2014_01')
    gdb_files = list_directories(GDB_EXTRACTED_FILE_PATH)
    print("Directories in", GDB_EXTRACTED_FILE_PATH, ":", gdb_files)
    # gdb_files = ['Zone9_2014_08.gdb']
    # directory_path = Path(GDB_PARQUET_FILE_PATH)
    # directory_path.mkdir(parents=True, exist_ok=True)
    for gdb_file in gdb_files:
        process_gdb_csv(zone=gdb_file)

