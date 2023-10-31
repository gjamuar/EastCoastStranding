import pandas as pd
import argparse
from datetime import datetime
import os
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import traceback
from shapely.geometry import Point, Polygon
import math
from typing import List, Tuple
from datetime import timedelta

from vessels_include import filter_vessels_by_name, VESSELS_NAMES


def load_from_file(filename):
    return pd.read_parquet(filename)
def generate_grid(min_lon, max_lon, min_lat, max_lat, spacing):
    lon_range = np.arange(min_lon, max_lon + spacing, spacing)
    lat_range = np.arange(min_lat, max_lat + spacing, spacing)

    grid_lines = []

    # Longitude grid lines
    for lon in lon_range:
        line = go.Scattermapbox(
            lat=[min_lat, max_lat],
            lon=[lon, lon],
            mode='lines',
            line=dict(color='rgba(0, 0, 0, 0.5)', width=1),
            hoverinfo='none',
            name='Grid Lines',  # Add a name for grid lines
            showlegend=False,  # Hide grid lines from the legend
        )
        grid_lines.append(line)

    # Latitude grid lines
    for lat in lat_range:
        line = go.Scattermapbox(
            lat=[lat, lat],
            lon=[min_lon, max_lon],
            mode='lines',
            line=dict(color='rgba(0, 0, 0, 0.5)', width=1),
            hoverinfo='none',
            showlegend=False,  # Hide grid lines from the legend
        )
        grid_lines.append(line)

    return grid_lines
def map_vessels(vessels: pd.DataFrame, whales_df: pd.DataFrame, polygons: List[List[Tuple[float, float]]], show_only_whales=False):
    print("\n[INFO] Starting map_vessels function...")

    if not only_whales:
        fig = px.scatter_mapbox(vessels, lat="LAT", lon="LON", hover_name="IMO",
                                hover_data=["VesselName", "IMO", "Heading", 'CallSign', 'Cargo', 'COG',
                                            'SOG', 'VesselType', 'MMSI'],
                                color="VesselName",
                                zoom=4, height=1000, mapbox_style="open-street-map")
    else:
        fig = go.Figure()

    fig1 = px.scatter_mapbox(whales_df, lat="lat", lon="lon", hover_name="ComName",
                             hover_data=["State", "ComName", "County", 'Year_new', 'Carcass Condition'],
                             color_discrete_sequence=["black"],
                             zoom=4, height=1000, mapbox_style="open-street-map")

    # Adding diagnostic prints for dataframes
    #print("\n[INFO] Vessels DataFrame:")
    #print(vessels.head())
    #print("\n[INFO] Whales DataFrame:")
    #print(whales_df.head())

    fig1.update_traces(marker=dict(size=20), name='Whales')

    grid_spacing = 0.290  
    min_lon, max_lon = min(vessels["LON"].min(), whales_df["lon"].min()), max(vessels["LON"].max(), whales_df["lon"].max())
    min_lat, max_lat = min(vessels["LAT"].min(), whales_df["lat"].min()), max(vessels["LAT"].max(), whales_df["lat"].max())
    grid_lines = generate_grid(min_lon, max_lon, min_lat, max_lat, grid_spacing)
    for line in grid_lines:
        fig.add_trace(line)

    for data in fig1.data:
        fig.add_trace(data)

    for i in range(len(fig.data)):
        if "name" in fig.data[i]:
            if fig.data[i].name == 'Grid Lines':
                fig.data[i].update(showlegend=False)

    for trace in fig.data:
        if trace.name == 'Whales':
            trace.marker.size = 20  
        else:
            trace.marker.size = 5  

    print("\n[INFO] Processing polygons...")

    for polygon in polygons:
        #print(f"\n[INFO] Current Polygon: {polygon}")  # Printing current polygon
        lat_vals, lon_vals = zip(*polygon)
        #print(f"[INFO] Lat values: {lat_vals}")  # Printing lat values
        #print(f"[INFO] Lon values: {lon_vals}")  # Printing lon values

        try:
            fig.add_trace(go.Scattermapbox(
                lat=lat_vals,
                lon=lon_vals,
                mode="lines",
                fill='toself',
                fillcolor='rgba(255,0,0,0.2)',
                line=dict(width=2, color='rgba(255,0,0,1)'),
                name="Polygon",
                hoverinfo='none'
            ))
        except Exception as e:
            print(f"[ERROR] Error while adding polygon trace: {e}")

        # Adding vertices of the polygons with hover information
        fig.add_trace(go.Scattermapbox(
            lat=lat_vals,
            lon=lon_vals,
            mode='markers',
            marker=dict(size=8, color='red'),
            hoverinfo='text',
            hovertext=[f"Lat: {lat_val}, Lon: {lon_val}" for lat_val, lon_val in zip(lat_vals, lon_vals)]
        ))

    fig.update_layout(mapbox=dict(center=dict(lat=(min_lat + max_lat) / 2, lon=(min_lon + max_lon) / 2)),
                      margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.show()


def read_vessel_files(sdate, edate):
    merged_df = pd.DataFrame()
    date_range = pd.date_range(sdate, edate, freq='d')
    filenames = [d.strftime('data/parquet/AIS_%Y_%m_%d.parquet') for d in date_range]

    for filename in filenames:
        if os.path.isfile(filename):
            try:
                temp_df = pd.read_parquet(filename)
                #print(f'{filename} : {temp_df.shape}')
                merged_df = pd.concat([merged_df, temp_df], ignore_index=True)

            except Exception as e:
                print('Error occurred while processing {}: {}'.format(filename, e))
                continue;
        else:
            print('File not found: {}'.format(filename))

    merged_df = filter_vessels_by_name(merged_df, VESSELS_NAMES)

    return merged_df     
def read_whale_data(whale_start_date, whale_end_date):
    #df_whales = load_from_file('merged_whales_minke.parquet')
    # df_whales = load_from_file('merged_whales.parquet')
    df_whales = pd.read_csv('2000_2015ume2016_2023_merged.csv')
    df_whales['Year_new'] = pd.to_datetime(df_whales['Obs_Date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
    # df_whales['Year_new'] = pd.to_datetime(df_whales['Year'], yearfirst=True, format='%Y%m%d').dt.strftime('%Y-%m-%d')
    df_whales_window: pd.DataFrame = df_whales.loc[df_whales["Year_new"].between(whale_start_date, whale_end_date)]
    
    return df_whales_window    
def load_exclude_boats(filename):
    with open(filename, 'r') as f:
        exclude_boats = f.read().splitlines()
    return exclude_boats
def angle_from_centroid(point, centroid):
    return math.atan2(point[1] - centroid[1], point[0] - centroid[0])
def plot_polygon_on_map(fig, points):
    polygon_lat = [point[0] for point in points]
    polygon_lon = [point[1] for point in points]
    # Close the polygon by appending the first point to the end of the list
    polygon_lat.append(points[0][0])
    polygon_lon.append(points[0][1])

    fig.add_trace(go.Scattergeo(
        locationmode='USA-states',
        lon=polygon_lon,
        lat=polygon_lat,
        mode='lines',
        line=dict(color='blue', width=2),
        fill='toself'
    ))
  
def angle_from_centroid(point, centroid):
    return (math.atan2(point[1] - centroid[1], point[0] - centroid[0]) + 2.0 * math.pi) % (2.0 * math.pi)   
    
def whales_in_polygon(polygon, whale_data):
    # Use the DataFrame apply method to filter rows
    in_polygon_df = whale_data[whale_data.apply(lambda row: polygon.contains(Point(row['lat'], row['lon'])), axis=1)]
    
    # Print whale information
    for index, row in in_polygon_df.iterrows():
        print(f"Whale at ({row['lat']}, {row['lon']}) on {row['Year_new']} is inside the polygon.")

    
    return in_polygon_df


def count_and_print_whales_in_polygon(df_whales: pd.DataFrame, polygon: Polygon):
    # Filter the whales that are within the polygon
    whales_within_polygon_df = whales_in_polygon(polygon, df_whales)

    # Count the number of whale points within the polygon
    whale_count = whales_within_polygon_df.shape[0]
    print(f"\nNumber of whale points within the defined polygon: {whale_count}")

    # Print the whale locations and dates inside the polygon
    for index, row in whales_within_polygon_df.iterrows():
        print(f"Whale at ({row['lat']}, {row['lon']}) on {row['Year_new']} is inside the polygon.")
        
        
def read_traffic_data(vessel_start_date: str, 
                      vessel_end_date: str, 
                      exclude_boats: List[str],
                      only_whales: bool = False) -> Tuple[pd.DataFrame, Polygon]:
    
    #print("Function read_traffic_data started.")
    
    # Define the polygon using the provided points
    '''points = [        
        (44.23913714983087, -75.55209601975874), #p1
        (43.73323833689619, -53.7552231032819), #p2
        (29.71887426867729, -54.106785569676696), #p3
        (31.683581855487965, -82.40756411445709), #p4 clockwise
        (44.23913714983087, -75.55209601975874)#p1
    ]'''
    points = [        
        (40.92935243527523, -74.40780238486083), #p1
        (42.22093036497428, -69.37374277494621), #p2
        (37.55218480196642, -71.31305148322379), #p3
        (39.045119888486454, -76.15444350594397), #p4 clockwise
        (40.92935243527523, -74.40780238486083)
    ]
    polygon = Polygon(points)
    print(polygon)
    
    
    if not only_whales:
        #print('if not only_whales')
        sdate = datetime.strptime(vessel_start_date, "%Y-%m-%d")
        edate = datetime.strptime(vessel_end_date, "%Y-%m-%d")   
        vessel_df = read_vessel_files(sdate, edate)
        
        # If there's no vessel data, print a message and return empty vessel dataframe and the polygon
        if vessel_df is None or vessel_df.empty:
            current_date = sdate
            while current_date <= edate:
                print(f"File not found: data/parquet/AIS_{current_date.strftime('%Y_%m_%d')}.parquet")
                current_date += timedelta(days=1)
            return pd.DataFrame(), polygon
        
        vessel_df["Date"] = pd.to_datetime(vessel_df["BaseDateTime"]).dt.strftime('%Y-%m-%d')

        
        
        # Exclude vessels based on specified conditions
        exclude_conditions = [
            (vessel_df['Cargo'] == 80) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 40) | (vessel_df['VesselType'] == 3),
            (vessel_df['Cargo'] == 81) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 82) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 83) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 84) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 85) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 86) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 87) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 88) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 89) | (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 70) & (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 70) & (vessel_df['VesselType'] == 1004),
            (vessel_df['Cargo'] == 70) & (vessel_df['VesselType'] == 0),
            (vessel_df['Cargo'] == 89) & (vessel_df['VesselType'] == 1024),
            (vessel_df['Cargo'] == 79) & (vessel_df['VesselType'] == 1004),
            (vessel_df['Cargo'] == 71) & (vessel_df['VesselType'] == 1004)
        ]
        exclude_mask = pd.concat(exclude_conditions, axis=1).any(axis=1)
        vessel_df = vessel_df[~exclude_mask]
        
        # Remove vessels in the exclude_boats list
        vessel_df = vessel_df[~vessel_df['VesselName'].isin(exclude_boats)]
        df_vessel_window = vessel_df.loc[vessel_df["Date"].between(vessel_start_date, vessel_end_date)]
        
        # Filter vessels within the polygon
        vessels_within_polygon_df = df_vessel_window[df_vessel_window.apply(lambda row: polygon.contains(Point(row['LAT'], row['LON'])), axis=1)]
        
        # Compute and print AIS points
        # ... [Your AIS points computation and printing logic here]
        # Compute and print the number of data points for each vessel within the defined polygon, ordered by AIS points
        print("Number of data AIS points for each vessel within the defined polygon:")
        data_points_per_vessel = vessels_within_polygon_df.groupby('VesselName').size().sort_values(ascending=False)
        for vessel, count in data_points_per_vessel.items():
           print(f"{vessel}: {count} AIS points")
         
        # Compute and print the total number of data points for all vessels within the defined polygon
        total_data_points = vessels_within_polygon_df.shape[0]
        print(f"\nTotal number of data AIS points for all vessels within the defined polygon: {total_data_points}")
        
        
    # Read whale data
    df_whales_window = read_whale_data(vessel_start_date, vessel_end_date)
    
    # Round the 'lat' and 'lon' columns to 6 decimal places (single precision)
    df_whales_window['lat'] = df_whales_window['lat'].round(2)
    df_whales_window['lon'] = df_whales_window['lon'].round(2)
    
    # Drop duplicates
    df_whales_window = df_whales_window.drop_duplicates(subset=['lat', 'lon', 'Year_new'])
    
    # Filter the whales that are within the polygon
    whales_within_polygon_df = df_whales_window[df_whales_window.apply(lambda row: polygon.contains(Point(row['lat'], row['lon'])), axis=1)]
    
    # Count the number of whale points within the polygon
    whale_count = whales_within_polygon_df.shape[0]
    print(f"\nNumber of whale points within the defined polygon: {whale_count}")

    # Print the whale locations and dates inside the polygon
    for index, row in whales_within_polygon_df.iterrows():
        print(f"Whale at ({row['lat']}, {row['lon']}) on {row['Year_new']} is inside the polygon.")
    
    # Plot the vessels and whales on the map
    map_vessels(df_vessel_window, df_whales_window, [points])
    
    #print("Function read_traffic_data finished.")
    return df_vessel_window if not only_whales else pd.DataFrame(), polygon



def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('-e', '--end_date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--only_whales', action='store_true', help='Only visualize whale data')   
    args = parser.parse_args()   
    return {
        'start_date': args.start_date,
        'end_date': args.end_date,
        'only_whales': args.only_whales
    }   
    return vars(parser.parse_args())

    


if __name__ == '__main__':
    args = parse_arg()
    #print(args)
    only_whales = args['only_whales']
    start_date = args['start_date']
    end_date = args['end_date']

    if start_date and end_date:
        exclude_boats = load_exclude_boats('exclude_boats.txt')
        df_whales_window = read_whale_data(start_date, end_date)

        min_lon, max_lon = df_whales_window["lon"].min(), df_whales_window["lon"].max()
        min_lat, max_lat = df_whales_window["lat"].min(), df_whales_window["lat"].max()

        if not only_whales:
            df_vessel_window, polygon_data= read_traffic_data(start_date, end_date, exclude_boats,only_whales)
            #print(df_vessel_window)
            
            vessel_min_lon, vessel_max_lon = df_vessel_window["LON"].min(), df_vessel_window["LON"].max()
            vessel_min_lat, vessel_max_lat = df_vessel_window["LAT"].min(), df_vessel_window["LAT"].max()

            min_lon = min(min_lon, vessel_min_lon)
            max_lon = max(max_lon, vessel_max_lon)
            min_lat = min(min_lat, vessel_min_lat)
            max_lat = max(max_lat, vessel_max_lat)

        else:
            points = [        
                (40.92935243527523, -74.40780238486083),
                (42.22093036497428, -69.37374277494621),
                (37.55218480196642, -71.31305148322379),
                (39.045119888486454, -76.15444350594397)
            ]
            polygon_data = [Polygon(points)]
            print(type(polygon_data))
            print(polygon_data)

        # Your plotting code begins here...

        DARK_BLUE = '#00008B'
        BLACK = '#000000'
        GRAY = '#808080'
        MEDIUM_BLUE = '#0000CD'
        color_map = {
            'Whale, minke': MEDIUM_BLUE,
            'Whale, humpback': BLACK
        }

        fig1 = px.scatter_mapbox(df_whales_window, lat="lat", lon="lon", hover_name="ComName",
                                 hover_data=["State", "ComName", "County", 'Obs_Date', 'Carcass Condition'],
                                 color="ComName", 
                                 color_discrete_map=color_map,
                                 labels={'ComName': 'Whales'},
                                 zoom=4, height=1000, mapbox_style="open-street-map",
                                 size_max=10)

        if not only_whales:
            fig = px.scatter_mapbox(df_vessel_window, lat="LAT", lon="LON", hover_name="IMO",
                                    hover_data=["VesselName", "IMO", "Heading", 'CallSign', 'Cargo', 'COG', 'SOG', 'VesselType', 'MMSI'],
                                    color="VesselName",
                                    zoom=4, height=1000, mapbox_style="open-street-map")
            for data in fig1.data:
                fig.add_trace(data)
        else:
            fig = fig1
        '''df_vessel_window, polygon_data= read_traffic_data(start_date, end_date, exclude_boats,only_whales)'''
        if polygon_data:
            for polygon in polygon_data:
                lat_vals, lon_vals = zip(*polygon.exterior.coords)
                fig.add_trace(go.Scattermapbox(
                    lat=lat_vals,
                    lon=lon_vals,
                    mode="lines",
                    fill='toself',
                    fillcolor='rgba(255,0,0,0.2)',
                    line=dict(width=2, color='rgba(255,0,0,1)'),
                    name="polygon",
                    hoverinfo='none'
                ))

                # Plotting vertices of the polygons with hover information:
                fig.add_trace(go.Scattermapbox(
                    lat=lat_vals,
                    lon=lon_vals,
                    mode='markers',
                    marker=dict(size=8, color='red'),
                    hoverinfo='text',
                    hovertext=[f"Lat: {lat_val}, Lon: {lon_val}" for lat_val, lon_val in zip(lat_vals, lon_vals)]
                ))

        grid_spacing = 0.290  
        grid_lines = generate_grid(min_lon, max_lon, min_lat, max_lat, grid_spacing)

        for line in grid_lines:
            fig.add_trace(line)

        for i in range(len(fig.data)):
            if "name" in fig.data[i]:
                if fig.data[i].name == 'Grid Lines':
                    fig.data[i].update(showlegend=False)

        for trace in fig.data:
            if trace.name == 'Whales':
                trace.marker.size = 20 

        fig.update_layout(mapbox=dict(center=dict(lat=(min_lat + max_lat) / 2, lon=(min_lon + max_lon) / 2)),
                          margin={"r": 0, "t": 0, "l": 0, "b": 0})
        fig.show()

    else:
        print("Please provide start and end dates for both the stranded whales and vessel traffic time windows.")






  
        
        

