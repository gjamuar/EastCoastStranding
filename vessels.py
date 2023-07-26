import sqlite3
import pandas as pd
import pygeohash as gh
import plotly.express as px

from main import load_from_file


def map_vessels(vessels: pd.DataFrame, whales_df: pd.DataFrame):
    fig = px.scatter_mapbox(vessels, lat="lat", lon="long", hover_name="imo",
                            hover_data=["vessel_name", "imo", "id", "heading", 'call_sign', 'cargo', 'cog',
                                        'sog', 'vessel_type'],
                            color="vessel_name",
                            # animation_frame="datetime",
                            # color_discrete_sequence=["fuchsia"],
                            zoom=4, height=1000, mapbox_style="open-street-map")

    fig1 = px.scatter_mapbox(whales_df, lat="lat", lon="lon", hover_name="SpeciesCategory",
                             hover_data=["NatlDBNum", "State", "SpeciesCategory", "County", 'Species', 'Genus', 'Date',
                                         'State'],
                             # animation_frame="Year",
                             color_discrete_sequence=["black"]
                             , zoom=4,
                             height=1000 , mapbox_style="open-street-map"
                             )

    fig.add_trace(fig1.data[0])
    # fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()


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

    df = pd.read_parquet('vessels.parquet')

    df["date"] = pd.to_datetime(df["datetime"]).dt.strftime('%Y-%m-%d')
    df_whales = load_from_file('whales.parquet')
    df_whales['Year'] = pd.to_datetime(df_whales['Date'], yearfirst=True, format='%Y-%b-%d').dt.strftime(
        '%Y')  # dt.strftime('%Y%m%d')
    # df['Year'] = df['Year'].apply(pd.to_datetime(yearfirst=True))
    # df['Year'] = df['Year'].apply(np.int64)
    df_whales.sort_values(by=['Year'], ascending=True, na_position='first', inplace=True)
    map_vessels(df, df_whales)

    # df2 = df.groupby(['date', 'geohash8'])['geohash8'].count().sort_values(ascending=False)
    # Group the DataFrame by team and position
    group = df.groupby(['date', 'geohash8'])

    # Count the number of players in each group
    count = group.size().sort_values(ascending=False)

    # Convert the count to a DataFrame
    df_count = count.reset_index(name='count')

    # View the DataFrame
    print(df_count)
    df_count['location'] = df_count.apply(lambda rec: gh.decode(rec['geohash8']), axis=1)
    # df_count['lat', 'long'] = gh.decode(df_count['geohash8'])

    print(df_count)
    print(gh.decode('dqgnj8es'))

    df3 = df.groupby(['date', 'geohash9'])['geohash9'].count().sort_values(ascending=False)
    print(df3)

    df4 = df.groupby(['date', 'geohash10'])['geohash10'].count().sort_values(ascending=False)
    print(df4)
