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

    df_2.rename(columns={'Common Name': 'ComName', 'Latitude': 'lat', 'Longitude': 'lon', 'ObjectID': 'OBJECTID',
                         'Obs Date': 'ObsDay', 'State/Province': 'State', }, inplace=True)

    # After renaming the columns
    print("\nAfter modifying column:\n", df_2.columns.sort_values())
    df_2['Year'] = pd.to_datetime(df_2['ObsDay'], yearfirst=True, format='%B %d, %Y').dt.strftime('%Y%m%d')
    print(df_2.head())

    outer_merged = pd.merge(
        df, df_2, how="inner", on=["Year", "State", ])  # 'lat', 'lon'
    print(outer_merged.head(50))
    print(outer_merged.shape)
    print(outer_merged.columns)
    new_df = outer_merged[['lat_x', 'lat_y']]
    print(new_df.head(20))

    new_df = outer_merged[['lat_x', 'lat_y', 'lon_x', 'lon_y', 'Year', 'County_x', 'County_y', 'State']]
    print(new_df.head(20))

    new_df = outer_merged[['lon_x', 'lon_y', 'County_x', 'County_y']]
    print(new_df.head(20))


if __name__ == '__main__':
    merge_whale_data()
