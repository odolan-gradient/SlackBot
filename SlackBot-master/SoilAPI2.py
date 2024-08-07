import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from shapely.wkt import loads as load_wkt


def get_soil_data(polygon_wkt):
    sda_url = "https://sdmdataaccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
    headers = {'Content-Type': 'application/json'}

    # Step 1: Get Map Unit Polygon Keys
    query1 = f"""
    SELECT DISTINCT mupolygonkey
    FROM SDA_Get_Mupolygonkey_from_intersection_with_WktWgs84('{polygon_wkt}')
    """
    request_payload1 = {
        "format": "JSON+COLUMNNAME+METADATA",
        "query": query1
    }
    response1 = requests.post(sda_url, json=request_payload1, headers=headers)

    if response1.status_code != 200:
        print(f"Error: {response1.status_code}, {response1.text}")
        return []

    data1 = response1.json()
    if "Table" not in data1 or not data1["Table"]:
        return []

    mupolygonkeys = [row[0] for row in data1["Table"][2:]]

    if not mupolygonkeys:
        return []

    # Step 2: Get Map Unit Geometries and Soil Data
    mupolygonkeys_str = ','.join(map(str, mupolygonkeys))
    query2 = f"""
    SELECT mu.muname, c.cokey, ch.hzname, mp.geom
    FROM mupolygon AS mp
    JOIN mapunit AS mu ON mu.mukey = mp.mukey
    JOIN component AS c ON c.mukey = mu.mukey
    JOIN chorizon AS ch ON ch.cokey = c.cokey
    WHERE mp.mupolygonkey IN ({mupolygonkeys_str})
    """
    request_payload2 = {
        "format": "JSON+COLUMNNAME+METADATA",
        "query": query2
    }

    response2 = requests.post(sda_url, json=request_payload2, headers=headers)

    if response2.status_code != 200:
        print(f"Error: {response2.status_code}, {response2.text}")
        return []

    data2 = response2.json()
    if "Table" not in data2 or not data2["Table"]:
        return []

    rows = data2["Table"][2:]

    soil_data = []
    for row in rows:
        soil_data.append({
            'muname': row[0],
            'cokey': row[1],
            'hzname': row[2],
            'geom': row[3]
        })

    return soil_data


def visualize_soil_data(soil_data):
    gdf = pd.DataFrame(soil_data)
    gdf['geometry'] = gdf['geom'].apply(load_wkt)
    gdf = gpd.GeoDataFrame(gdf, geometry='geometry')

    fig, ax = plt.subplots(1, 1, figsize=(10, 10))
    gdf.plot(column='muname', cmap='Set1', legend=True, ax=ax)
    plt.title('Soil Types')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.show()


# Example polygon WKT
polygon_wkt = "POLYGON((-121.420000 36.850000, -121.410000 36.850000, -121.410000 36.860000, -121.420000 36.860000, -121.420000 36.850000))"

# Get soil data
soil_data = get_soil_data(polygon_wkt)

# Visualize the soil data
if soil_data:
    visualize_soil_data(soil_data)
else:
    print("No soil data found")
