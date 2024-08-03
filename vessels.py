import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import argparse

from main import load_from_file
from vessels_include import filter_vessels_by_name, VESSELS_NAMES


def map_vessels(vessels: pd.DataFrame, whales_df: pd.DataFrame):
    fig = px.scatter_mapbox(vessels, lat="LAT", lon="LON", hover_name="IMO",
                            hover_data=["VesselName", "IMO", "Heading", 'CallSign', 'Cargo', 'COG',
                                        'SOG', 'VesselType', 'MMSI'],
                            color="VesselName",
                            # animation_frame="datetime",
                            # color_discrete_sequence=["fuchsia"],
                            zoom=4, height=1000, mapbox_style="open-street-map")

    # fig1 = px.scatter_mapbox(whales_df, lat="lat", lon="lon", hover_name="ComName",
    #                          hover_data=["State", "ComName", "County", 'Year_new',
    #                                      "NatlDBNum", 'Field Number', 'Species', 'Genus'],
    #                          # animation_frame="Year",
    #                          color_discrete_sequence=["black"]
    #                          , zoom=4,
    #                          height=1000, mapbox_style="open-street-map"
    #                          )
    fig1 = px.scatter_mapbox(whales_df, lat="lat", lon="lon", hover_name="ComName",
                             hover_data=["State", "ComName", "County", 'Year_new', 'Carcass Condition'],
                             color_discrete_sequence=["black"],
                             zoom=4, height=1000, mapbox_style="open-street-map")

    fig.add_trace(fig1.data[0])
    # fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()


def read_vessel_files(sdate, edate):
    # Create an empty dataframe to hold our combined data
    merged_df = pd.DataFrame()
    date_range = pd.date_range(sdate, edate, freq='d')
    # print(date_range)

    filenames = [d.strftime('data/parquet/AIS_%Y_%m_%d.parquet') for d in date_range]

    # print(filenames)

    # Iterate over all of the files 
    for filename in filenames:

        # Check if the file is actually a file (not a directory) and make sure it is a parquet file
        if os.path.isfile(filename):
            try:
                # Perform a read on our dataframe
                temp_df = pd.read_parquet(filename)
                print(f'{filename} : {temp_df.shape}')

                # Attempt to merge it into our combined dataframe
                merged_df = pd.concat([merged_df, temp_df], ignore_index=True)

            except Exception as e:
                print('Skipping {} due to error: {}'.format(filename, e))
                continue;
        else:
            print('Not a file {}'.format(filename))

    merged_df = filter_vessels_by_name(merged_df, VESSELS_NAMES)
    # Return the result!
    return merged_df


def parse_arg():
    arg_desc = '''\
            Stranded whales and Vessels Traffic data on Map!
        ---------------------------------------------------------
        This program displays Stranded whales and Vessels Traffic
        in north-east coast during selected time window.
        '''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=arg_desc)

    parser.add_argument("-ws", "--whale_sdate", help="Start date for stranded whale time window in YYYY-MM-DD format")
    parser.add_argument("-we", "--whale_edate", help="End date for stranded whale time window in YYYY-MM-DD format")
    parser.add_argument("-vs", "--vessel_sdate", help="Start date for vessel traffic time window in YYYY-MM-DD format")
    parser.add_argument("-ve", "--vessel_edate", help="End date for vessel traffic time window in YYYY-MM-DD format")
    return vars(parser.parse_args())


def read_whale_data(whale_start_date, whale_end_date):
    df_whales = pd.read_csv('2000_2015ume2016_2023_merged.csv')
    # df_whales = load_from_file('merged_whales.parquet')
    df_whales['Year_new'] = pd.to_datetime(df_whales['Obs_Date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
    # df_whales['Year_new'] = pd.to_datetime(df_whales['Year'], yearfirst=True, format='%Y%m%d').dt.strftime(
    #     '%Y-%m-%d')
    # df_whales.sort_values(by=['Year_new'], ascending=True, na_position='first', inplace=True)

    df_whales_window: pd.DataFrame = df_whales.loc[df_whales["Year_new"].between(whale_start_date, whale_end_date)]
    print(f'df_whales_window shape: {df_whales_window.shape}')
    print(df_whales_window)
    return df_whales_window


def read_traffic_data(vessel_start_date, vessel_end_date):
    sdate = datetime.strptime(vessel_start_date, "%Y-%m-%d")
    edate = datetime.strptime(vessel_end_date, "%Y-%m-%d")
    vessel_df = read_vessel_files(sdate, edate)

    print(f'merged dataframe {vessel_df.shape}')
    vessel_df["Date"] = pd.to_datetime(vessel_df["BaseDateTime"]).dt.strftime('%Y-%m-%d')

    print(vessel_df)

    df_vessel_window: pd.DataFrame = vessel_df.loc[vessel_df["Date"].between(vessel_start_date, vessel_end_date)]
    print(f'df_vessel_window {df_vessel_window.shape}')
    return df_vessel_window


if __name__ == '__main__':
    args = parse_arg()
    print(args)

    vessel_start_date = args['vessel_sdate']
    vessel_end_date = args['vessel_edate']
    whale_start_date = args['whale_sdate']
    whale_end_date = args['whale_edate']

    df_whales_window = read_whale_data(whale_start_date, whale_end_date)

    df_vessel_window = read_traffic_data(vessel_start_date, vessel_end_date)

    # df_vessel_window = filter_vessels_by_name(df_vessel_window, VESSELS_NAMES)

    # print(df_vessel_window.head(10))

    map_vessels(df_vessel_window, df_whales_window)

# Create your connection.

# cnx = sqlite3.connect('aisdata_new.db')
#
# df = pd.read_sql_query("SELECT * FROM vessels", cnx)
# print(df.head())
# # for col in df.columns:
# #     print(col)
# df['geohash12'] = df.apply(lambda x: gh.encode(x.lat, x.long, precision=12), axis=1)
# df['geohash11'] = df.apply(lambda x: gh.encode(x.lat, x.long, precision=11), axis=1)
# df['geohash10'] = df.apply(lambda x: gh.encode(x.lat, x.long, precision=10), axis=1)
# df['geohash9'] = df.apply(lambda x: gh.encode(x.lat, x.long, precision=9), axis=1)
# df['geohash8'] = df.apply(lambda x: gh.encode(x.lat, x.long, precision=8), axis=1)
# df['geohash7'] = df.apply(lambda x: gh.encode(x.lat, x.long, precision=7), axis=1)
# df['geohash6'] = df.apply(lambda x: gh.encode(x.lat, x.long, precision=6), axis=1)
# print(df[['lat', 'long', 'geohash12', 'geohash11', 'geohash10', 'geohash9', 'geohash8']].head())
#
# df.to_parquet('vessels.parquet')

# df = pd.read_parquet('vessels.parquet')

# df["Year"] = pd.to_datetime(df["datetime"]).dt.strftime('%Y-%m-%d')


# # df2 = df.groupby(['date', 'geohash8'])['geohash8'].count().sort_values(ascending=False)
# # Group the DataFrame by team and position
# group = df.groupby(['date', 'geohash8'])
#
# # Count the number of players in each group
# count = group.size().sort_values(ascending=False)
#
# # Convert the count to a DataFrame
# df_count = count.reset_index(name='count')
#
# # View the DataFrame
# print(df_count)
# df_count['location'] = df_count.apply(lambda rec: gh.decode(rec['geohash8']), axis=1)
# # df_count['lat', 'long'] = gh.decode(df_count['geohash8'])
#
# print(df_count)
# print(gh.decode('dqgnj8es'))
#
# df3 = df.groupby(['date', 'geohash9'])['geohash9'].count().sort_values(ascending=False)
# print(df3)
#
# df4 = df.groupby(['date', 'geohash10'])['geohash10'].count().sort_values(ascending=False)
# print(df4)
