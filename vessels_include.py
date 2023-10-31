import pandas as pd

VESSEL_NAME_FILTER = True

VESSELS_NAMES = [
    'Bella Marie',
    'Deep Helder',
    'Marcelle Bordelon',
    'Time and Tide',
    'Shearwater',
    'Fugro Brasilis'
]


def filter_vessels_by_name(df_vessels: pd.DataFrame, vessel_name_list=None):
    if not VESSEL_NAME_FILTER:
        return df_vessels

    if vessel_name_list is None:
        vessel_name_list = []
    return df_vessels[df_vessels['VesselName'].str.lower().isin([x.lower() for x in vessel_name_list])]
