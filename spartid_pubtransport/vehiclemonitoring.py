import requests
import pandas as pd

import geopandas
from lxml import etree


base_url = "https://api.entur.io/realtime/v1/rest/vm"
nsmap = {
    'siri': 'http://www.siri.org.uk/siri',
    'ns2': "http://www.ifopt.org.uk/acsb",
    "ns3": "http://www.ifopt.org.uk/ifopt",
    "ns4": "http://datex2.eu/schema/2_0RC1/2_0",
}

def get_vehicles():
    resp = requests.get(f"{base_url}?maxSize=100000")
    assert resp.ok
    prstree = etree.fromstring(resp.content) 

    all_items = []
    for vehicle_activity in prstree.iter(etree.QName(nsmap["siri"], "VehicleActivity")):
        vehicle_activity_dict = {etree.QName(x).localname : x.text for x in vehicle_activity if x.text is not None}
        monitored_journey = vehicle_activity.find(etree.QName(nsmap["siri"], "MonitoredVehicleJourney"))
        monitored_journey_dict = {etree.QName(x).localname : x.text for x in monitored_journey if x.text is not None}
        vehicle_location = monitored_journey.find(etree.QName(nsmap["siri"], "VehicleLocation"))
        if vehicle_location is not None:
            vehicle_location_dict = {etree.QName(x).localname : x.text for x in vehicle_location if x.text is not None}
        else:
            vehicle_location = {}

        all_items.append(vehicle_activity_dict | monitored_journey_dict | vehicle_location_dict)

    df_raw = pd.DataFrame(all_items).convert_dtypes(dtype_backend="pyarrow")

    df = (df_raw
        .assign(
            # RecordedAtTime=pd.to_datetime(df_raw.RecordedAtTime, format="ISO8601"),
            # ValidUntilTime=pd.to_datetime(df_raw.ValidUntilTime, format="ISO8601", errors="coerce"),
            # Bearing=df_raw.Bearing.astype("float32[pyarrow]"),
        #  Delay=pd.to_timedelta(df_raw.Delay),
        )
    )

    gdf = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
    )
    return gdf