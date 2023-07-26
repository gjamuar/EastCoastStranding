import numpy as np
import plotly.express as px
import json
import pandas as pd
import requests
import datetime as dt
from dataprep.clean import clean_lat_long, validate_lat_long
from lat_lon_parser import parse
import re
import geopandas as gpd
from shapely import Polygon

from Converter import convert_point_to_latlon
from map_geocoordinates import display_polygon_on_map


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def map_cities():
    us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")
    fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
                            color_discrete_sequence=["fuchsia"], zoom=3, height=1000)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()


def parse_json_to_dataframe():
    input_file = open('humpback.json')
    json_array = json.load(input_file)
    whale_list = []

    for item in json_array:

        if len(item['features']) > 0:
            item_feature = item['features'][0]
            whale_with_date = find_whale_data_with_date(item_feature['geometry']['x'], item_feature['geometry']['y'])
            obs_date = whale_with_date['features'][0]['attributes']['ObsDate'] if whale_with_date['features'] and \
                                                                                  whale_with_date['features'][0] and \
                                                                                  whale_with_date['features'][0][
                                                                                      'attributes'] and \
                                                                                  whale_with_date['features'][0][
                                                                                      'attributes']['ObsDate'] else None
            print(obs_date)
            whale_details = item_feature['attributes']
            whale_details['x'] = item_feature['geometry']['x']
            whale_details['y'] = item_feature['geometry']['y']
            whale_details['spatialReference'] = item['spatialReference']['wkid']
            whale_details['Date'] = obs_date
            # {
            #     "OBJECTID": 5056,
            #     "NatlDBNum": "NE-2017-1198957",
            #     "ConfCode": "Confirmed - High Report",
            #     "SpeciesID": 7,
            #     "ComName": "Whale, humpback",
            #     "Genus": "Megaptera",
            #     "Species": "novaeangliae",
            #     "County": "Barnstable",
            #     "State": "MA",
            #     "WaterBody": "ATLANTIC",
            #     "LatAcc": "actual",
            #     "LongAcc": "actual",
            #     "ObsDay": 18,
            #     "ObsStatus": "Alive",
            #     "SpeciesCategory": "Humpback Whale"
            # }

            # whale_details = {
            #     'NatlDBNum': item_feature['attributes']['NatlDBNum'],
            #     'x': item_feature['geometry']['x'], 'y': item_feature['geometry']['y'],
            #     'Genus': item_feature['attributes']['Genus'],
            #     'Species': item_feature['attributes']['Species'],
            #     'County': item_feature['attributes']['County'],
            #     'State': item_feature['attributes']['State'],
            #     'SpeciesCategory': item_feature['attributes']['SpeciesCategory'],
            #     'spatialReference': item['spatialReference']['wkid'],
            #     'obsDate': item_feature['attributes']['ObsDay'],
            #     'Date': obs_date}
            lat, long = convert_point_to_latlon(item_feature['geometry']['x'], item_feature['geometry']['y'],
                                                whale_details['spatialReference'])
            # print(f'lat: {lat} , longitude:{long}')

            # if '2017' in item_feature['attributes']['NatlDBNum']:
            #     print(item_feature['attributes']['NatlDBNum'])
            #     print(item_feature['attributes']['ObsDay'])
            #     print(f'lat: {lat} , longitude:{long}')

            whale_details['lat'] = lat
            whale_details['lon'] = long
            whale_list.append(whale_details)
        else:
            print(item['features'])

    print(whale_list)
    return pd.DataFrame(whale_list)


def map_whales(whales: pd.DataFrame):
    fig = px.scatter_mapbox(whales, lat="lat", lon="lon", hover_name="SpeciesCategory",
                            hover_data=["NatlDBNum", "State", "SpeciesCategory", "County", 'Species', 'Genus', 'Date',
                                        'State'],
                            # animation_frame="Year",
                            color_discrete_sequence=["fuchsia"], zoom=4, height=1000)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()


def get_whale_data(x_lat, y_lon, server):
    # print(f'server number: {server}')
    params = {
        "geometry": f'{x_lat},{y_lon},{x_lat},{y_lon}',
        "geometryType": "esriGeometryEnvelope",
        "inSR": "3857",
        "outSR": '3857',
        "spatialRel": 'esriSpatialRelIntersects',
        'f': 'json',
        'returnGeometry': 'true',
        'outFields': '*'
    }
    response = requests.get(
        f"https://oceandata.rad.rutgers.edu/arcgis/rest/services/MarineLife/MarineMammalStrandings_2000_20_ME_VA/MapServer/{server}/query",
        params
    )
    if response.status_code == 200:
        body = response.json()
        if 'error' in body:
            # print('found error')
            return None
        return body
        # print(body)
        # print(response.status_code)
    return None


def find_whale_data_with_date(x_lat, y_lon):
    server_list = range(2, 19)
    for server_num in server_list:
        if server_num not in [3, 9, 14]:
            resp_data = get_whale_data(x_lat, y_lon, server_num)
            if resp_data and resp_data['features'] and resp_data['features'][0] and resp_data['features'][0][
                'attributes'] and resp_data['features'][0]['attributes']['ObsDate']:
                return resp_data
    return None


# Press the green button in the gutter to run the script.
def load_from_file(filename):
    return pd.read_parquet(filename, engine='pyarrow')


def parse_boat_json_to_dataframe():
    input_file = open('data.json')
    boat_data = json.load(input_file)
    boat_list = []
    for key, value in boat_data.items():
        print(key)
        for boat in value:
            for name, details in boat.items():
                boat_details = details
                boat_details['year_week'] = key
                boat_details['boat_name'] = name
                boat_list.append(boat_details)
                boat_details['parsed_coord'] = parse_coordinates(boat_details['coords'])
                # print(parse_coordinates(boat_details['coords']))
                # boat_details['geometry'] = geo_frames(boat_details['parsed_coord'])

    boat_df = pd.DataFrame(boat_list)
    print(boat_df)
    boat_df['geometry'] = boat_df.apply(geo_frames, axis=1 )
    print(boat_df.head())
    return boat_df


def geo_frames(boatdf):
    lon_lat_list = boatdf['parsed_coord']
    polygon_geom = Polygon(lon_lat_list)
    polygon = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[polygon_geom])
    return polygon


def parse_coordinates(coord_list):
    print(coord_list)
    # df2 = clean_lat_long(pd.DataFrame({'lat_lon': coord_list}),"lat_lon")
    # validate_lat_long(pd.DataFrame({'lat_lon': coord_list}), "lat_lon")
    # print(df2)
    list = []
    for coord in coord_list:
        print(coord)
        result_cord = re.finditer(r"(.*?)N.*?([0-9].*)W", coord)
        for match in result_cord:
            # for index in range(0, match.lastindex + 1):
            lat = parse(match.group(1))
            lon = parse(match.group(2))
            print(f'Lat: {lat} , Long: {lon}')
            list.append((lat, -lon))

    if len(list) < 4:
        list.extend(list)
    print(len(list))
    print(list)
    return list


def map_boats(boats: pd.DataFrame):
    fig = px.line_geo(boats, geojson=boats['geometry'], locations=boats.index, height=1000, hover_name=boats.boat_name)
    # fig = px.scatter_mapbox(boats, lat="lat", lon="lon", hover_name="SpeciesCategory",
    #                         hover_data=["NatlDBNum", "State", "SpeciesCategory", "County", 'Species', 'Genus', 'Date',
    #                                     'State'],
    #                         animation_frame="Year",
    #                         color_discrete_sequence=["fuchsia"], zoom=4, height=1000)
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()


if __name__ == '__main__':
    print_hi('PyCharm')
    # fig = px.bar(x=["a", "b", "c"], y=[1, 3, 2])
    # fig.write_html('first_figure.html', auto_open=True)
    # df = parse_json_to_dataframe()
    # df.to_csv('complete_whale_list.csv', sep='\t')
    # df.to_parquet('whales.parquet')
    df = load_from_file('whales.parquet')
    df['Year'] = pd.to_datetime(df['Date'], yearfirst=True, format='%Y-%b-%d').dt.strftime(
        '%Y')  # dt.strftime('%Y%m%d')
    # df['Year'] = df['Year'].apply(pd.to_datetime(yearfirst=True))
    # df['Year'] = df['Year'].apply(np.int64)
    df.sort_values(by=['Year'], ascending=True, na_position='first', inplace=True)
    # print(df.dtypes)
    # pd.to_datetime(df.Date).dt.strftime('%m%Y')

    print(df.head(n=50))
    # df.to_csv('sorted_whale_list.csv', sep='\t')
    # df.to_parquet('sorted_whales.parquet')
    # map_cities()
    map_whales(df)
    # x = -7790538.7158801369
    # y = 5101226.1087373206
    # whale_with_date = find_whale_data_with_date(x, y)
    # print(whale_with_date)
    # print(whale_with_date['features'][0]['attributes']['ObsDate'])

    bdf = parse_boat_json_to_dataframe()
    display_polygon_on_map(bdf['parsed_coord'].values.tolist())
    # map_boats(bdf)
