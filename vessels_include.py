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
    df_vessels_mod = df_vessels.query(" VesselType >= 90 or Cargo >= 90 or VesselType == 70")
    df_vessels_mod = df_vessels_mod.query(" VesselType > 69 or VesselType < 60 ")
    df_vessels_mod = df_vessels_mod.query(
        " (Cargo < 51 or Cargo > 55 ) and Cargo != 58 and (Cargo > 69 or Cargo <60 ) and ( Cargo < 30 or Cargo > 37) ")
    df_vessels_mod = df_vessels_mod.query('not VesselName.str.contains("CG ")')

    if not VESSEL_NAME_FILTER:
        return df_vessels_mod

    if vessel_name_list is None:
        vessel_name_list = []

    df_vessels_name = df_vessels[df_vessels['VesselName'].str.lower().isin([x.lower() for x in vessel_name_list])]

    frames = [df_vessels_mod, df_vessels_name]

    result = pd.concat(frames)

    return result
