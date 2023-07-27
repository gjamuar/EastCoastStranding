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


if __name__ == '__main__':
    merge_whale_data()
