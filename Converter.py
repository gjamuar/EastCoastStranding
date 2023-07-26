import pyproj
from arcgis import geometry


def convert_arcgis_to_latlon(x, y, spatial_reference):
    # Create a point geometry object using the ArcGIS coordinates
    point = geometry.Point({"x": x, "y": y, "spatialReference": spatial_reference})

    # Convert the point geometry to latitude and longitude
    # geographic_point = geometry.project([point], spatial_reference=4326, in_sr=spatial_reference)
    geographic_point = geometry.project([point], in_sr=spatial_reference, out_sr=4326)


    # Extract the latitude and longitude values from the converted point
    latitude = geographic_point[0]["y"]
    longitude = geographic_point[0]["x"]

    return latitude, longitude


def convert_point_to_latlon(x, y, spatial_reference):
    source_crs = f'epsg:{spatial_reference}'  # Coordinate system of the file
    target_crs = 'epsg:4326'  # Global lat-lon coordinate system

    polar_to_latlon = pyproj.Transformer.from_crs(source_crs, target_crs)
    # print(polar_to_latlon)
    lat, long = polar_to_latlon.transform(x, y)
    return lat, long


if __name__ == '__main__':
    # Example usage
    x = -8360048.1175836176  # Replace with your ArcGIS x-coordinate
    y = 4435649.1571791107  # Replace with your ArcGIS y-coordinate
    spatial_reference = 3857  # Replace with the spatial reference code of your ArcGIS data
    latitude, longitude = convert_point_to_latlon(x, y, spatial_reference)
    # latitude, longitude = convert_arcgis_to_latlon(x, y, spatial_reference)
    print("Latitude:", latitude)
    print("Longitude:", longitude)
