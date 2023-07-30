import pandas as pd

from main import load_from_file


def merge_whale_data():
    df = load_from_file('sorted_whales.parquet')
    # df['Year'] = pd.to_datetime(df['Date'], yearfirst=True, format='%Y-%b-%d').dt.strftime(
    #     '%Y')  # dt.strftime('%Y%m%d')

    df_2 = pd.read_csv('Humpback UME Stranding Locations.csv')
    print(df.head())
    print(df_2.head())

    print(df.columns.sort_values())
    print(df_2.columns.sort_values())

    # Index(['ComName', 'ConfCode', 'County', 'Date', 'Genus', 'LatAcc', 'LongAcc',
    #        'NatlDBNum', 'OBJECTID', 'ObsDay', 'ObsStatus', 'Species',
    #        'SpeciesCategory', 'SpeciesID', 'State', 'WaterBody', 'Year', 'lat',
    #        'lon', 'spatialReference', 'x', 'y'],
    #       dtype='object')
    # Index(['Carcass Condition', 'Common Name', 'Country', 'County', 'Field Number',
    #        'Latitude', 'Longitude', 'Month', 'ObjectID', 'Obs Date', 'Sex',
    #        'State/Province', 'Year'],
    #       dtype='object')
    #
    # After
    # modifying
    # column:
    # Index(['Carcass Condition', 'ComName', 'Country', 'County', 'Field Number',
    #        'Month', 'OBJECTID', 'ObsDay', 'Sex', 'State', 'Year', 'lat', 'lon'],
    #       dtype='object')
    #

    df_2.rename(columns={'Common Name': 'ComName', 'Latitude': 'lat', 'Longitude': 'lon', 'ObjectID': 'OBJECTID',
                         'Obs Date': 'ObsDay', 'State/Province': 'State', }, inplace=True)

    # After renaming the columns
    print("\nAfter modifying column:\n", df_2.columns.sort_values())
    df_2['Year'] = pd.to_datetime(df_2['ObsDay'], yearfirst=True, format='%B %d, %Y').dt.strftime('%Y%m%d')
    print(df_2.head())

    df['County'] = df['County'].str.split("(").str[0].str.strip().str.lower()
    df_2['County'] = df_2['County'].str.strip().str.lower()

    print(df['County'].values)
    print(df_2['County'].values)

    # df = df.drop(columns=['Carcass Condition', 'Month', 'Sex'])

    outer_merged = pd.merge(
        df, df_2, how="outer", on=["Year", "State", "County"])  # 'lat', 'lon'
    print(outer_merged.head(50))
    print(outer_merged.shape)
    print(outer_merged.columns)
    new_df = outer_merged[['lat_x', 'lat_y']]
    print(new_df.head(50))

    new_df = outer_merged[['lat_x', 'lat_y', 'lon_x', 'lon_y', 'Year', 'County', 'State']]
    print(new_df.head(50))

    new_df = outer_merged[['lon_x', 'lon_y', 'County']]
    print(new_df.head(50))

    outer_merged['Lat_diff'] = outer_merged['lat_x'] - outer_merged['lat_y']
    outer_merged['Lon_diff'] = outer_merged['lon_x'] - outer_merged['lon_y']

    print(outer_merged['Lat_diff'].sort_values().values)
    print(outer_merged['Lon_diff'].sort_values().values)

    rslt_df = outer_merged[(abs(outer_merged['Lat_diff']) < 0.0001) & abs((outer_merged['Lon_diff'] < 0.0001))]

    print('\nResult dataframe :\n', rslt_df[['lat_x', 'lat_y', 'lon_x', 'lon_y', 'Year', 'County', 'State','Year']].to_string())
    print('\nResult dataframe :\n', rslt_df['Field Number'].values)


if __name__ == '__main__':
    merge_whale_data()
