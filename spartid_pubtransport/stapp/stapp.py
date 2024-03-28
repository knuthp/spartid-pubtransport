import folium
import streamlit as st
from streamlit_folium import folium_static

from spartid_pubtransport import vehiclemonitoring


@st.cache_data(ttl=5*60)
def get_vm():
    return vehiclemonitoring.get_vehicles()
    


st.title("Spartid Public Transport")

geo_df = get_vm()

center = geo_df.dissolve().centroid

map = folium.Map(location=[center.y, center.x], zoom_start=4)

geo_df_list = [[point.xy[1][0], point.xy[0][0]] for point in geo_df.geometry]

for i, coordinates in enumerate(geo_df_list):
    if geo_df["VehicleMode"].fillna('')[i] == "":
        type_color = "black"
    elif geo_df["VehicleMode"][i] == "bus":
        type_color = "green"
    elif geo_df["VehicleMode"][i] == "ferry":
        type_color = "red"
    elif geo_df["VehicleMode"][i] == "rail":
        type_color = "yellow"
    else:
        type_color = "black"


    # Place the markers with the popup labels and data
    map.add_child(
        folium.Marker(
            location=coordinates,
            popup="LineRef: "
            + str(geo_df["LineRef"][i])
            + "<br>",
            icon=folium.Icon(color="%s" % type_color),
        )
    )

folium_static(map)