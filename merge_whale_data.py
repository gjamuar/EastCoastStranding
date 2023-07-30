import pandas as pd

from main import load_from_file


def merge_whale_data():
    df = load_from_file('sorted_whales.parquet')
    # df['Year'] = pd.to_datetime(df['Date'], yearfirst=True, format='%Y-%b-%d').dt.strftime(
    #     '%Y')  # dt.strftime('%Y%m%d')

    df_2 = pd.read_csv('Humpback UME Stranding Locations.csv')
    print(df.head(50).to_string())
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
    df_2['Date'] = pd.to_datetime(df_2['ObsDay'], yearfirst=True, format='%B %d, %Y').dt.strftime('%Y-%b-%d')
    print(df_2.head())

    df['County'] = df['County'].str.split("(").str[0].str.strip().str.lower()
    df_2['County'] = df_2['County'].str.strip().str.lower()

    print(df['County'].values)
    print(df_2['County'].values)

    # df = df.drop(columns=['Carcass Condition', 'Month', 'Sex'])

    outer_merged = pd.merge(
        df, df_2, how="inner", on=["Year", "State", "County"])  # 'lat', 'lon'
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

    df_removed_dup = df_2[~df_2['Field Number'].isin(rslt_df['Field Number'].values)]

    print(f'df_removed_dup shape: {df_removed_dup.shape}')
    print(df_removed_dup.to_string())

    merged_df = pd.concat([df, df_removed_dup[['Carcass Condition', 'ComName', 'Country', 'County', 'Field Number',
       'Month', 'OBJECTID', 'Sex', 'State', 'Year', 'lat', 'lon']]], ignore_index=True)
    merged_df.to_parquet('merged_whales.parquet')
    merged_df.to_csv('merged_whales.csv')
    






if __name__ == '__main__':
    merge_whale_data()

# Result dataframe :
#  ['VAQS20161004' 'SYMn1610' 'NEAQ-16-023-Mn' 'NY5460-2016' 'NY5481-2016'
#  'MERRST1511Mn' 'NY5488-2016' 'SSC-16-022 Mn' 'COA160825Mn' 'MMSC-16-083'
#  'NMFSGAR111816Mn' 'NY5541-2016' 'VAQS20161078' 'MMSC-17-002'
#  'VAQS20171004' 'VAQS20171005' 'VAQS20171006' 'VAQS20171007' 'MMSC-17-008'
#  'VAQS20171008' 'MERR-ST1580Mn' 'IFAW17-270Mn' 'IFAW17-274Mn'
#  'IFAW17-317Mn' 'NMFSGAR080817Mn' 'NMFSGAR102017Mn' 'NMFSGAR102017Mn'
#  'AMCS121Mn2017' 'VAQS20171090' 'AMCS154Mn2017' 'VAQS20181002'
#  'VAQS20181006' 'VAQS20181007' 'AMCS06Mn2018' 'COA180417Mn '
#  'AMCS69Mn2018' 'AMCS73Mn2018' 'AMCS76Mn2018' 'AMCS83Mn2018'
#  'AMCS123Mn2018' 'AMCS139Mn2018' 'SSC-18-173Mn' 'MERR-ST1730Mn'
#  'VAQS20191003' 'VAQs20191009' 'VAQS20191011' 'VAQS20191024'
#  'IFAW19-287Mn' 'AMCS110Mn2019' 'AMCS165Mn2019' 'AMCS165Mn2019'
#  'AMCS171Mn2019' 'AMCS171Mn2019' 'MMSC-19-177' 'MMSC-19-181' 'MMSC-19-185'
#  'VAQS20191101' 'AMCS207Mn2019' 'SYMn19123' 'MERR-ST1843Mn' 'VAQS20201002'
#  'VAQS20201004' 'VAQS20201005' 'AMCS42Mn2020' 'MMSC-20-073' 'COA200709Mn'
#  'MMAN20-169' 'MMSC-20-144_AMCS140Mn2020 ' 'MMSC-20-160']

#  ['VAQS20161004' 'SYMn1610' 'NEAQ-16-023-Mn' 'NY5460-2016' 'NY5481-2016'
#  'MERRST1511Mn' 'NY5488-2016' 'SSC-16-022 Mn' 'COA160825Mn' 'MMSC-16-083'
#  'NMFSGAR111816Mn' 'NY5541-2016' 'VAQS20161078' 'MMSC-17-002'
#  'VAQS20171004' 'VAQS20171005' 'VAQS20171006' 'VAQS20171007' 'MMSC-17-008'
#  'VAQS20171008' 'MERR-ST1580Mn' 'IFAW17-270Mn' 'IFAW17-274Mn'
#  'IFAW17-317Mn' 'NMFSGAR080817Mn' 'NMFSGAR102017Mn' 'NMFSGAR102017Mn'
#  'AMCS121Mn2017' 'VAQS20171090' 'AMCS154Mn2017' 'VAQS20181002'
#  'VAQS20181006' 'VAQS20181007' 'AMCS06Mn2018' 'COA180417Mn '
#  'AMCS69Mn2018' 'AMCS73Mn2018' 'AMCS76Mn2018' 'AMCS83Mn2018'
#  'AMCS123Mn2018' 'AMCS139Mn2018' 'SSC-18-173Mn' 'MERR-ST1730Mn'
#  'VAQS20191003' 'VAQs20191009' 'VAQS20191011' 'VAQS20191024'
#  'IFAW19-287Mn' 'AMCS110Mn2019' 'AMCS165Mn2019' 'AMCS165Mn2019'
#  'AMCS171Mn2019' 'AMCS171Mn2019' 'MMSC-19-177' 'MMSC-19-181' 'MMSC-19-185'
#  'VAQS20191101' 'AMCS207Mn2019' 'SYMn19123' 'MERR-ST1843Mn' 'VAQS20201002'
#  'VAQS20201004' 'VAQS20201005' 'AMCS42Mn2020' 'MMSC-20-073' 'COA200709Mn'
#  'MMAN20-169' 'MMSC-20-144_AMCS140Mn2020 ' 'MMSC-20-160']