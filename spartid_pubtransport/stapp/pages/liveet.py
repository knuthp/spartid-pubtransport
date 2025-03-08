from pathlib import Path

import folium
import geopandas
import pandas as pd
import streamlit as st
from folium.plugins import Fullscreen
from streamlit_folium import folium_static

from spartid_pubtransport import estimatedtimetable, gtfs

MAP__CENTER__LONG = 10.611268532369081
MAP__CENTER__LAT = 59.669607206900906


st.title("SIRI Live Estimated Timetable Data")
st.write(
    """SIRI is a protocol for public transport. It reports Vehicle Monitoring and
    Estimated Timetable. Ruter and some others does not report Vehicle Monitoring,
    only Estimated Timetable. Explore how we can present live data based on
    Estimated Timetable.
    """
)

@st.cache_data(ttl=5*60)
def gdf_vehicles(gtfs_parquet_root: Path) -> geopandas.GeoDataFrame:
    return estimatedtimetable.get_vehicles(gtfs_parquet_root)

with st.spinner("Downloading GTFS data to cache"):
    gtfs_downloader = gtfs.GtfsDownloader()
    gtfs_downloader.download_and_convert()
    gtfs_parquet_root = gtfs_downloader.gtfs_parquet_root


gdf = gdf_vehicles(gtfs_parquet_root)

map = folium.Map(location=[MAP__CENTER__LAT, MAP__CENTER__LONG], zoom_start=7)
Fullscreen(position="topleft").add_to(map)


def style_function(feature):
    vehicle_type = feature['properties']['vehicle_type']
    if vehicle_type == 100.0: # Train
        return {'color': 'red'}
    elif vehicle_type == 700.0: # Bus
        return {'color': 'green'}
    elif vehicle_type == 1000.0: # Tram
        return {'color': 'yellow'}
    elif vehicle_type == 401.0: # Metro
        return {'color': 'black'}
    elif vehicle_type == 900.0: # Ferry
        return {'color': 'blue'}
    else:
        return {'color': 'white'}

vehicles = folium.GeoJson(
    gdf[["geometry", "LineRef", "DatedVehicleJourneyRef", "DataSource", "stop_name", "vehicle_type"]],
    style_function=style_function,
    highlight_function=lambda feature: {
        "weight": 10,
        "color": "black",
    },
    marker=folium.CircleMarker(radius=5, fill=True),
    tooltip=folium.features.GeoJsonTooltip(
        fields=[
            "LineRef",
            "DatedVehicleJourneyRef",
            "DataSource",
            "stop_name",
            "vehicle_type",
        ],
       # aliases=["Shape", "Number of Points", "Total distance (m)"],
    ),
    popup=folium.GeoJsonPopup(
        fields=[
            "LineRef",
            "DatedVehicleJourneyRef",
            "DataSource",
            "stop_name",
            "vehicle_type",
        ],
    )
)
vehicles.add_to(map)
folium_static(map)


st.dataframe(pd.DataFrame(gdf))
