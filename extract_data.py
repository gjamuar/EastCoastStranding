import logging
import multiprocessing
import os
import zipfile
from datetime import date

import pandas as pd
import requests
from dateutil.rrule import rrule, DAILY

import database as db
import utils
from constants import DOWNLOAD_PATH, CSV_PATH, PARQUET_PATH, OVERWRITE_DOWNLOAD, ZIP_DOWNLOAD, SKIP_DOWNLOAD

logging.basicConfig(filename='app.log',
                    filemode='w',
                    format='%(asctime)s - %(levelname)s-8s %(name)s  - %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')


def extract_and_push(file):
    logging.debug(f"processing file {file}")
    df = pd.read_csv(file, skip_blank_lines=True, on_bad_lines='skip', low_memory=False)
    # east_coast_df = df[(df['LAT'] >= 36.5) & (df['LAT'] <= 46.0) &
    #                    (df['LON'] >= -87.0) & (df['LON'] <= -65.0)]
    east_coast_df = df[(df['LAT'] >= 24.0) & (df['LAT'] <= 56.0) &
                       (df['LON'] >= -99.0) & (df['LON'] <= -48.0)]
    qr = east_coast_df[(east_coast_df['Status'] == 3)]
    '''
    56,-97,
    56,-48
    25,-97
    25,-48
    
    Bella Marie- sonar
    Deep Helder- sonar
    Marcelle Bordelon- sonar
    Time and Tide- sonar
    Shearwater- sonar & drill
    Fugro Brasilis
    
    SELECT DISTINCT vessel_name, vessel_type, cargo 
        FROM vessels 
        WHERE LOWER(vessel_name) LIKE LOWER('%Deep Helder%');
    
    '''
    '''
    select distinct(vessel_name),call_sign,vessel_type,cargo,length,width from vessels where cargo != 55 
    and cargo != 52 and ( cargo < 30 or cargo > 37) and cargo != 0 and (cargo >69 or cargo <60 ) 
    and cargo != 51 and ( vessel_type > 69 or vessel_type <60 ) and vessel_name not like '%CG %';

    '''
    # qr = qr.query(" VesselType >= 90 or Cargo >= 90 or VesselType == 70")
    # qr = qr.query(" VesselType > 69 or VesselType < 60 ")
    # qr = qr.query(
    #     " (Cargo < 51 or Cargo > 55 ) and Cargo != 58 and (Cargo > 69 or Cargo <60 ) and ( Cargo < 30 or Cargo > 37) ")
    # qr = qr.query('not VesselName.str.contains("CG ")')
    qr.to_parquet(PARQUET_PATH + file.split('/')[-1].split('.')[0] + '.parquet')

    # db.push_data(qr)
    logging.debug(f"processing done {file}")


def main():
    db.create_table()
    files_list = utils.get_files(CSV_PATH, "csv")
    # files_list = ["../data/AIS_2023_01_01.csv"]
    # for file in files_list:
    #     extract_and_push(file)
    with multiprocessing.Pool(processes=20) as pool:
        pool.map(extract_and_push, files_list)
        pool.close()
        pool.join()


def test(num):
    print(num)


def process_files(date_list):
    with multiprocessing.Pool(processes=5) as pool:
        pool.map(download_data, date_list)
        pool.close()
        pool.join()


def file_exits(filedate, filetype='parquet'):
    if filetype != 'zip':
        filename = filedate.strftime("AIS_%Y_%m_%d")
    else:
        filename = filedate

    if filetype == 'csv':
        filepath = CSV_PATH + filename + '.csv'
    elif filetype == 'parquet':
        filepath = PARQUET_PATH + filename + '.parquet'
    elif filetype == 'zip':
        filepath = filename
    else:
        return False
    if os.path.isfile(filepath):
        logging.debug(f'skipping file, {filename}.{filetype} exits ')
        return True
    return False


def clear_files(file_list):
    for file in file_list:
        if os.path.exists(file):
            logging.debug(f'removing file {file}')
            os.remove(file)


def download_files(start_year, start_month, start_day, end_year, end_month, end_day):
    # initializing the start and end date
    start_date = date(start_year, start_month, start_day)
    end_date = date(end_year, end_month, end_day)
    date_list = []

    # iterating over the dates
    for d in rrule(DAILY, dtstart=start_date, until=end_date):
        logging.debug(d.strftime("%Y-%m-%d"))
        if OVERWRITE_DOWNLOAD or not file_exits(d, filetype='parquet'):
            date_list.append(d)
        # download_data(d)

    process_files(date_list)


def download_data(filedate):
    url = filedate.strftime('https://coast.noaa.gov/htdata/CMSP/AISDataHandler/%Y/AIS_%Y_%m_%d.zip')
    logging.debug(f'downloading {url}')
    file_list = []
    try:
        if not file_exits(filedate, filetype='csv'):
            filename = download_file(url)
            # file_list.append(filename)
            logging.debug(f'extracting {filename}')
            with zipfile.ZipFile(filename) as zf:
                zf.extractall(CSV_PATH)

        csv_filename = filedate.strftime("AIS_%Y_%m_%d")
        csv_file = CSV_PATH + csv_filename + '.csv'
        extract_and_push(csv_file)
        file_list.append(csv_file)
        clear_files(file_list)
    except:
        logging.exception(f'error for {filedate}')


def download_file(url):
    local_filename = DOWNLOAD_PATH + url.split('/')[-1]
    if SKIP_DOWNLOAD and file_exits(local_filename, filetype='zip'):
        logging.info(f"Skipping downloading file {local_filename} from URL {url}")
        return local_filename
    if ZIP_DOWNLOAD:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192 * 1000):
                    f.write(chunk)
    return local_filename


if __name__ == "__main__":
    # main()
    # download_files(2023, 10, 1, 2023, 12, 31)
    download_files(2015, 1, 1, 2019, 12, 31)
    # process_files()
