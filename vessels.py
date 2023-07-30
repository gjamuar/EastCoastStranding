import glob
import os
import sqlite3
from datetime import timedelta, date
from datetime import datetime

import pandas as pd
import pygeohash as gh
import plotly.express as px

from main import load_from_file


def map_vessels(vessels: pd.DataFrame, whales_df: pd.DataFrame):
    fig = px.scatter_mapbox(vessels, lat="LAT", lon="LON", hover_name="IMO",
                            hover_data=["VesselName", "IMO", "Heading", 'CallSign', 'Cargo', 'COG',
                                        'SOG', 'VesselType', 'MMSI'],
                            color="VesselName",
                            # animation_frame="datetime",
                            # color_discrete_sequence=["fuchsia"],
                            zoom=4, height=1000, mapbox_style="open-street-map")

    fig1 = px.scatter_mapbox(whales_df, lat="lat", lon="lon", hover_name="SpeciesCategory",
                             hover_data=["NatlDBNum", "State", "SpeciesCategory", "County", 'Species', 'Genus', 'Date',
                                         'State'],
                             # animation_frame="Year",
                             color_discrete_sequence=["black"]
                             , zoom=4,
                             height=1000, mapbox_style="open-street-map"
                             )

    fig.add_trace(fig1.data[0])
    # fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()


def read_vessel_files(sdate, edate):
    # Create an empty dataframe to hold our combined data
    merged_df = pd.DataFrame()
    date_range = pd.date_range(sdate, edate, freq='d')
    print(date_range)

    filenames = [d.strftime('data/parquet/AIS_%Y_%m_%d.parquet') for d in date_range]

    print(filenames)

    # Iterate over all of the files in the provided directory and
    # configure if we want to recursively search the directory
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

    # Return the result!
    return merged_df


if __name__ == '__main__':
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

    vessel_start_date = '2023-01-01'
    vessel_end_date = '2023-03-28'
    # sdate = date(2019, 3, 22)  # start date
    # edate = date(2019, 4, 9)
    sdate = datetime.strptime(vessel_start_date, "%Y-%m-%d")
    edate = datetime.strptime(vessel_end_date, "%Y-%m-%d")
    vessel_df = read_vessel_files(sdate, edate)

    print(f'merged dataframe {vessel_df.shape}')
    vessel_df["Date"] = pd.to_datetime(vessel_df["BaseDateTime"]).dt.strftime('%Y-%m-%d')

    print(vessel_df)

    df_vessel_window: pd.DataFrame = vessel_df.loc[vessel_df["Date"].between("2023-03-01", "2023-03-15")]
    print(f'df_vessel_window {df_vessel_window.shape}')


    # df = pd.read_parquet('vessels.parquet')

    # df["Year"] = pd.to_datetime(df["datetime"]).dt.strftime('%Y-%m-%d')
    df_whales = load_from_file('whales.parquet')
    df_whales['Year'] = pd.to_datetime(df_whales['Date'], yearfirst=True, format='%Y-%b-%d').dt.strftime(
        '%Y-%m-%d')  # dt.strftime('%Y%m%d')
    # df['Year'] = df['Year'].apply(pd.to_datetime(yearfirst=True))
    # df['Year'] = df['Year'].apply(np.int64)
    df_whales.sort_values(by=['Year'], ascending=True, na_position='first', inplace=True)
    # df.sort_values(by=['Year'], ascending=True, na_position='first', inplace=True)

    # df_vessels: pd.DataFrame = df.loc[df["Year"].between("2023-03-01", "2023-03-15")]
    # print(df_vessels.shape)

    df_whales_window: pd.DataFrame = df_whales.loc[df_whales["Year"].between("2020-01-01", "2020-12-15")]
    print(df_whales_window.shape)

    map_vessels(df_vessel_window, df_whales_window)

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
