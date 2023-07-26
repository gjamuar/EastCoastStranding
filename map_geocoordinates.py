import plotly.graph_objects as go


def display_polygon_on_map(coordinates_list):
    # # Extract the latitude and longitude values separately
    # lats, longs = zip(*coordinates)
    #
    # # Create a trace for the polygon
    # polygon_trace = go.Scattergeo(
    #     lat=lats + (lats[0],),  # Include the first point to close the polygon
    #     lon=longs + (longs[0],),  # Include the first point to close the polygon
    #     mode='lines',
    #     # fill='toself',
    #     line=dict(color='blue')
    # )

    # Create the layout for the map
    layout = go.Layout(
        title='Polygon on North America Map',
        geo=dict(
            showland=True,
            landcolor='rgb(217, 217, 217)',
            subunitcolor='rgb(255, 255, 255)',
            countrycolor='rgb(255, 255, 255)',
            showlakes=True,
            lakecolor='rgb(255, 255, 255)',
            showsubunits=True,
            showcountries=True,
            resolution=110,
            projection=dict(type='azimuthal equal area', scale=1),
            coastlinewidth=0.5,
            lataxis=dict(
                range=[5, 75],
                showgrid=True,
                dtick=10
            ),
            lonaxis=dict(
                range=[-170, -50],
                showgrid=True,
                dtick=20
            )
        )
    )

    # Create the figure
    # fig = go.Figure(data=[polygon_trace], layout=layout)
    fig = go.Figure(data=get_polygon_trace(coordinates_list), layout=layout)

    # Display the figure
    fig.show()


def get_polygon_trace(coordinates_list):
    polygon_trace_list = []
    # Extract the latitude and longitude values separately
    for coord in coordinates_list:
        print(coord)
        if len(coord) < 2:
            continue
        lats, longs = zip(*coord)

        # Create a trace for the polygon
        polygon_trace = go.Scattergeo(
            lat=lats + (lats[0],),  # Include the first point to close the polygon
            lon=longs + (longs[0],),  # Include the first point to close the polygon
            mode='lines',
            # fill='toself',
            line=dict(color='blue')
        )
        polygon_trace_list.append(polygon_trace)
    return polygon_trace_list


if __name__ == '__main__':
    # Example usage
    # List of geographic coordinates in (latitude, longitude) format
    coordinates = [(38.69166666666667, -75.07166666666667),
                   (38.541666666666664, -75.05333333333333),
                   (38.45, -74.985),
                   (38.45, -74.75333333333333),
                   (38.66166666666667, -74.96166666666667)]

    coordinates2 = [(39.672777777777775, -73.9363888888889), (39.261944444444445, -73.94277777777778),
                    (39.144444444444446, -74.09722222222221), (39.275277777777774, -74.24861111111112),
                    (39.58722222222222, -74.04972222222221), (40.5, -73.98416666666667),
                    (40.510555555555555, -73.96472222222222), (40.2075, -73.86888888888888),
                    (39.92611111111111, -73.92861111111111), (39.92611111111111, -73.88027777777778),
                    (39.477222222222224, -73.93305555555555), (39.477222222222224, -73.91027777777778)]

    coordinates_lst = [coordinates, coordinates2]

    display_polygon_on_map(coordinates_lst)
