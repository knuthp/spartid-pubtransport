import branca.colormap as cm
import folium
import geopandas
import matplotlib.pyplot as plt
import streamlit as st
from folium.plugins import BeautifyIcon, Fullscreen
from shapely.geometry import LineString
from streamlit_folium import folium_static

st.header("History one route")
conn = st.connection("postgresql", type="sql")


if {"data_frame_ref", "dated_vehicle_journey_ref"}.issubset(st.query_params.keys()):
    st.write("Use query params")
    data_frame_ref = st.query_params["data_frame_ref"]
    dated_vehicle_journey_ref = st.query_params["dated_vehicle_journey_ref"]
    st.write(data_frame_ref)
    st.write(dated_vehicle_journey_ref)
else:
    st.write("Ask params")
    df_unique_dates = conn.query(
        """
        SELECT DISTINCT ON ("DataFrameRef") "DataFrameRef"
        FROM "VEHICLE_MONITORING"
        LIMIT 10;
        """
    )
    unique_dates = (df_unique_dates["DataFrameRef"].str.slice(0, 10)).unique().tolist()
    #    st.write(df_unique_dates)
    data_frame_ref = st.selectbox(label="Date", options=unique_dates)
    # TODO sliced data need to be queried
    df_unique_dates_routes = conn.query(
        """
        SELECT DISTINCT ON ("DatedVehicleJourneyRef") "DatedVehicleJourneyRef"
        FROM "VEHICLE_MONITORING"
        WHERE "DataFrameRef" = :selected_date
        LIMIT 10000;
        """,
        params={"selected_date": data_frame_ref},
    )
    unique_journeys = df_unique_dates_routes["DatedVehicleJourneyRef"].tolist()
    dated_vehicle_journey_ref = st.selectbox(label="Journey", options=unique_journeys)


params = {
    "data_frame_ref": data_frame_ref,
    "dated_vehicle_journey_ref": dated_vehicle_journey_ref,
}

df = conn.query(
    """
    SELECT *
    FROM "VEHICLE_MONITORING"
    WHERE "DataFrameRef" = :data_frame_ref AND "DatedVehicleJourneyRef" = :dated_vehicle_journey_ref;
    """,
    params=params,
)


total_time = df["RecordedAtTime"].max() - df["RecordedAtTime"].min()
st.write(total_time)

gdf = geopandas.GeoDataFrame(
    df,
    geometry=geopandas.points_from_xy(df["Longitude"], df["Latitude"]),
    crs="EPSG:4326",
)
gdf = gdf.to_crs(gdf.estimate_utm_crs())
shifted_gdf = gdf.shift(-1)
gdf["time_delta"] = gdf["RecordedAtTime"] - gdf["RecordedAtTime"].shift(-1)
gdf["dist_delta"] = gdf.distance(shifted_gdf)
gdf["m_per_s"] = gdf["dist_delta"] / gdf.time_delta.dt.seconds * 1000
gdf["km_per_h"] = gdf["m_per_s"] * 3.6
gdf["distance"] = gdf["dist_delta"].cumsum()
gdf["time_passed"] = gdf["time_delta"].cumsum()


gdf = gdf.to_crs(epsg=4326)
shifted_gdf = shifted_gdf.to_crs(epsg=4326)
lines = gdf.iloc[:-1].copy()
lines["next_point"] = shifted_gdf["geometry"]
lines["line_segment"] = lines.apply(
    lambda row: LineString([row["geometry"], row["next_point"]]), axis=1
)

lines.set_geometry("line_segment", inplace=True, drop=True)
lines.drop(columns="next_point", inplace=True)
lines.index.names = ["segment_id"]

columns = [
    "RecordedAtTime",
    "time_delta",
    "dist_delta",
    #    "seconds",
    "m_per_s",
    "km_per_h",
    "distance",
    "time_passed",
    "Latitude",
    "Longitude",
    #    "geometry"
]
st.write(f"Distance: {gdf.dist_delta.sum()/1000:.2f} km")
st.write(f"Avg speed: {gdf.km_per_h.mean():.2f} km/h")
st.write(f"Median speed: {gdf.km_per_h.median():.2f} km/h")
st.write(f"Max speed: {gdf.km_per_h.max():.2f} km/h")

location = lines.dissolve().convex_hull.centroid
st.header("Geopandas (lines_notime)")
lines_notime = lines.drop(
    columns=["DataFrameRef", "RecordedAtTime", "time_delta", "time_passed"]
).assign(
    km_per_h=lines["km_per_h"].round(decimals=2),
    distance=(lines["distance"] / 1000).round(decimals=2),
)
st.dataframe(lines_notime.drop(columns="geometry"))
map = folium.Map(
    location=[location.y, location.x], zoom_start=8, tiles="cartodbpositron"
)
Fullscreen(position="topleft").add_to(map)

# Plot the track, color is speed
max_speed = lines["km_per_h"].max()
start = folium.Marker(
    location=[lines_notime.iloc[0]["Latitude"], lines_notime.iloc[0]["Longitude"]],
    popup=str("Start"),
    icon=BeautifyIcon(
        icon="arrow-down",
        icon_shape="marker",
        number=0,
        border_color="white",
        background_color="lightgreen",
        border_width=1,
    ),
)
stop = folium.Marker(
    location=[lines_notime.iloc[-1]["Latitude"], lines_notime.iloc[-1]["Longitude"]],
    popup=str("Start"),
    icon=BeautifyIcon(
        icon="arrow-down",
        icon_shape="marker",
        number=1,
        border_color="white",
        background_color="red",
        border_width=1,
    ),
)
linear = cm.LinearColormap(["white", "yellow", "red"], vmin=0, vmax=max_speed)
route = folium.GeoJson(
    lines_notime.dropna().reset_index(),
    style_function=lambda feature: {
        "color": linear(feature["properties"]["km_per_h"]),
        "weight": 5,
    },
    tooltip=folium.features.GeoJsonTooltip(
        fields=[
            "segment_id",
            "distance",
            "km_per_h",
        ],
        aliases=["index", "Distance (km)", "Speed (km/h)"],
    ),
)


map.add_child(linear)
map.add_child(route)
map.add_child(start)
map.add_child(stop)
folium_static(map)


fig, ax = plt.subplots(nrows=1, ncols=1)
df.plot(x="Latitude", y="Longitude", ax=ax)
st.pyplot(fig=fig)

st.header("Geopandas dataframe")
st.dataframe(
    (gdf[columns].assign(time_delta=gdf.time_delta.dt.seconds)),
    column_config={
        x: st.column_config.NumberColumn(format="%.0f")
        for x in ["time_delta", "dist_delta", "m_per_s", "km_per_h"]
    },
)

st.header("Raw pandas dataframe (df)")
st.write(df)
