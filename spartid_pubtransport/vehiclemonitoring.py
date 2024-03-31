import geopandas
import pandas as pd
import requests
from lxml import etree

base_url = "https://api.entur.io/realtime/v1/rest/vm"
nsmap = {
    "siri": "http://www.siri.org.uk/siri",
    "ns2": "http://www.ifopt.org.uk/acsb",
    "ns3": "http://www.ifopt.org.uk/ifopt",
    "ns4": "http://datex2.eu/schema/2_0RC1/2_0",
}

session = requests.Session()
session.headers.update({"ET-Client-Name": "knuthp-spartid-pubtransport"})


def get_vehicles() -> geopandas.GeoDataFrame:
    """Gets Entur SIRI Vehicle Monitoring as geopandas GeoDataFrame.

    Transforms SIRI XML to pandas DataFrame.
    Cleans up datatypes
    Returns as Geopandas GeoDataframe
    """
    resp = session.get(f"{base_url}?maxSize=100000")
    assert resp.ok
    df_raw = _siri_mv_to_df_raw(resp)
    df = _df_raw_to_clean(df_raw)

    return geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df.Longitude, df.Latitude),
        crs="EPSG:4326",
    )


def _df_raw_to_clean(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Cleans datatypes for easier work in pandas."""
    return df_raw.assign(
        VehicleMode=df_raw.VehicleMode.fillna("unknown"),
        RecordedAtTime=pd.to_datetime(df_raw.RecordedAtTime, format="ISO8601"),
        ValidUntilTime=pd.to_datetime(
            df_raw.ValidUntilTime, format="ISO8601", errors="coerce"
        ),
        Bearing=df_raw.Bearing.astype("float32[pyarrow]"),
        delay_str=df_raw.Delay,
        Delay=pd.to_timedelta(df_raw.Delay),
    )


def _siri_mv_to_df_raw(resp):
    """Entur SIRI XML conversion to DataFrame.

    Flattens XML to a 2D DataFrame.
    """

    def _find_child_and_flatten_to_dict(
        parent: etree._Element, name: str
    ) -> tuple[dict[str, str], etree._Element]:
        child = parent.find(etree.QName(nsmap["siri"], name))
        if child is None:
            return {}, None
        return {
            etree.QName(x).localname: x.text for x in child if x.text is not None
        }, child

    prstree = etree.fromstring(resp.content)

    all_items = []
    for vehicle_activity in prstree.iter(etree.QName(nsmap["siri"], "VehicleActivity")):
        vehicle_activity_dict = {
            etree.QName(x).localname: x.text
            for x in vehicle_activity
            if x.text is not None
        }
        monitored_journey_dict, monitored_journey = _find_child_and_flatten_to_dict(
            vehicle_activity, "MonitoredVehicleJourney"
        )
        framed_vehicle_ref_dict, _ = _find_child_and_flatten_to_dict(
            monitored_journey, "FramedVehicleJourneyRef"
        )
        vehicle_location_dict, _ = _find_child_and_flatten_to_dict(
            monitored_journey, "VehicleLocation"
        )
        monitored_call_dict, _ = _find_child_and_flatten_to_dict(
            monitored_journey, "MonitoredCall"
        )

        all_items.append(
            vehicle_activity_dict
            | framed_vehicle_ref_dict
            | monitored_journey_dict
            | vehicle_location_dict
            | monitored_call_dict
        )

    return pd.DataFrame(all_items).convert_dtypes(dtype_backend="pyarrow")
