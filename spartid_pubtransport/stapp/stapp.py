import logging

import folium
import pandas as pd
import streamlit as st
from folium.plugins import Fullscreen
from streamlit_folium import folium_static

from spartid_pubtransport import vehiclemonitoring

MAP__CENTER__LONG = 10.611268532369081
MAP__CENTER__LAT = 59.669607206900906

logger = logging.getLogger(__name__)


@st.cache_data(ttl=5 * 60)
def get_vm():
    return vehiclemonitoring.get_vehicles()


def _df_bin_delays(df: pd.DataFrame) -> pd.DataFrame:
    bins = [
        pd.Timedelta(days=-1),
        pd.Timedelta(minutes=0),
        pd.Timedelta(minutes=5),
        pd.Timedelta(minutes=10),
        pd.Timedelta(minutes=20),
        pd.Timedelta(minutes=30),
        pd.Timedelta(days=4),
    ]

    labels = ["unreasonable", "0-5min", "5-10min", "10-20min", "20-30min", "30min+"]
    colors = ["gray", "green", "blue", "orange", "red", "darkred"]
    color_map = {label: color for label, color in zip(labels, colors)}

    return df.assign(
        delay_bin=pd.cut(df["Delay"], bins, labels=labels),
    ).assign(delay_color=lambda df1: df1.delay_bin.map(color_map))


st.set_page_config(layout="wide")
st.title("Spartid Public Transport")

geo_df_raw = get_vm()
geo_df = geo_df_raw.pipe(_df_bin_delays).assign(
    icon=geo_df_raw["VehicleMode"].map(
        {
            "unknown": "question-circle",
            "bus": "bus",
            "ferry": "ship",
            "rail": "train",
        }
    ),
)

st.write(f"Monitoring: {len(geo_df_raw)} vehicles")

vehicle_modes = geo_df["VehicleMode"].unique()

st.number_input("Hours since last update:", min_value=0, max_value=24, value=1)
one_hour = pd.Timestamp.now(tz="Europe/Berlin") - pd.Timedelta(hours=1)

limit_to_types = st.multiselect(
    label="Type",
    options=vehicle_modes,
    default=list(vehicle_modes),
)

geo_df_filteres = geo_df.query("VehicleMode in @limit_to_types").query(
    "RecordedAtTime > @one_hour"
)
st.write(f"{len(geo_df)} total filtere {len(geo_df_filteres)}")


map = folium.Map(location=[MAP__CENTER__LAT, MAP__CENTER__LONG], zoom_start=8)
Fullscreen(position="topleft").add_to(map)

for index, row in geo_df_filteres.iterrows():
    coordinates = [row["Latitude"], row["Longitude"]]
    if any(pd.isna(x) for x in coordinates):
        logger.warning(f"Coordinates for vehicle position is nan: {row}")
        break
    # Place the markers with the popup labels and data
    map.add_child(
        folium.Marker(
            location=coordinates,
            popup=f"""
            <table>
            <tr><th>LineRef:</th><td>{str(row["LineRef"])}</td></tr>
            <tr><th>Delay:</th><td>{str(row["Delay"])}</td></tr>
            <tr><th>Delay:</th><td>{str(row["delay_bin"])}{str(row["delay_color"])}</td></tr>
            <tr><th>OriginName:</th><td>{str(row["OriginName"])}</td></tr>
            <tr><th>DestinationName:</th><td>{str(row["DestinationName"])}</td></tr>
            <tr><th>VehicleStatus:</th><td>{str(row["VehicleStatus"])}</td></tr>
            <tr><th>OriginAimedDepartureTime:</th><td>{str(row["OriginAimedDepartureTime"])}</td></tr>
            </table>
            """,
            icon=folium.Icon(
                icon=row["icon"], prefix="fa", color=f"{row['delay_color']}"
            ),
        )
    )

folium_static(map)

st.header("Data")
st.dataframe(geo_df.drop(columns="geometry"))
