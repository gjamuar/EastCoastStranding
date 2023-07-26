import pandas as pd


if __name__ == '__main__':
    df = pd.read_csv("AIS_2023_01_10.csv")
    print(df.head())
    df.query('VesselType' == '90')
    # df.filter()
    # new_df = df.loc[df['VesselType'] == '90']
    # print(new_df)