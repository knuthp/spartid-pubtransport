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

vehicles = folium.GeoJson(
    gdf[["geometry", "LineRef", "DataSource", "stop_name", "vehicle_type"]],
    highlight_function=lambda feature: {
        "weight": 10,
        "color": "black",
    },
    tooltip=folium.features.GeoJsonTooltip(
        fields=[
            "LineRef",
            "DataSource",
            "stop_name",
            "vehicle_type",
        ],
       # aliases=["Shape", "Number of Points", "Total distance (m)"],
    ),
)
vehicles.add_to(map)
folium_static(map)


st.dataframe(pd.DataFrame(gdf))
