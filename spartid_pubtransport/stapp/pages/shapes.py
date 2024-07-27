from pathlib import Path

import folium
import geopandas
import streamlit as st
from folium.plugins import Fullscreen
from streamlit_folium import folium_static

st.set_page_config(layout="wide")
st.header("Shapes")
proj_root = Path(__file__).parent.parent.parent.parent
gtfs_root = proj_root / "data/gtfs/rb_norway-aggregated-gtfs"

gdf_read = geopandas.read_parquet(
    gtfs_root / "shapes_linestring_simple.parquet"
).set_crs("epsg:4326")

gdf = gdf_read.rename(
    columns={
        "max(shape_pt_sequence)": "num_points",
        "max(shape_dist_traveled)": "total_distance",
    }
).assign(total_distance_km=lambda df1: df1.total_distance / 1000)

st.dataframe(gdf.drop(columns=["geometry", "total_distance"]))

# location = gdf.dissolve().convex_hull.centroid

map = folium.Map(location=[65.0, 10.0], zoom_start=4, tiles="cartodbpositron")
Fullscreen(position="topleft").add_to(map)

# linear = cm.LinearColormap(["white", "yellow", "red"], vmin=0, vmax=max_speed)
route = folium.GeoJson(
    (gdf.sample(100)),
    # style_function=lambda feature: {
    #     "color": linear(feature["properties"]["km_per_h"]),
    #     "weight": 5,
    # },
    highlight_function=lambda feature: {
        "weight": 10,
        "color": "black",
    },
    tooltip=folium.features.GeoJsonTooltip(
        fields=[
            "shape_id",
            "num_points",
            "total_distance_km",
        ],
        aliases=["Shape", "Number of Points", "Total distance (m)"],
    ),
    zoom_on_click=True,
)


map.add_child(route)
folium_static(map)
