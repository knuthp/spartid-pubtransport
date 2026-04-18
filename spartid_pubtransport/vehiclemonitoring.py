import os
import geopandas
import pandas as pd
import requests

# Entur GraphQL API for vehicle positions
base_url = "https://api.entur.io/realtime/v2/vehicles/graphql"

session = requests.Session()
session.headers.update(
    {"ET-Client-Name": os.environ.get("ET_CLIENT_NAME", "knuthp-spartid-pubtransport")}
)

# GraphQL query to fetch vehicle positions with relevant fields
VEHICLES_QUERY = """
{
  vehicles {
    lastUpdated
    vehicleId
    location {
      latitude
      longitude
    }
    bearing
    delay
    vehicleStatus
    mode
    line {
      lineRef
      publicCode
      lineName
    }
    originName
    destinationName
    codespace {
      codespaceId
    }
    serviceJourney {
      id
      date
    }
  }
}
"""


def get_vehicles() -> geopandas.GeoDataFrame:
    """Gets Entur GraphQL Vehicle Monitoring as geopandas GeoDataFrame.

    Transforms GraphQL JSON response to pandas DataFrame.
    Cleans up datatypes.
    Returns as Geopandas GeoDataframe.
    """
    resp = session.post(base_url, json={"query": VEHICLES_QUERY})
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        raise ValueError(f"GraphQL errors: {data['errors']}")

    vehicles = data.get("data", {}).get("vehicles", [])
    df_raw = _graphql_to_df_raw(vehicles)
    df = _df_raw_to_clean(df_raw)

    return geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df.Longitude, df.Latitude),
        crs="EPSG:4326",
    )


def _df_raw_to_clean(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Cleans datatypes for easier work in pandas."""
    if df_raw.empty:
        return df_raw

    return df_raw.assign(
        VehicleMode=df_raw.VehicleMode.fillna("unknown").str.lower(),
        RecordedAtTime=pd.to_datetime(df_raw.RecordedAtTime, format="ISO8601"),
        # Delay from GraphQL is in seconds (float)
        Delay=pd.to_timedelta(df_raw.Delay, unit="s"),
        Bearing=df_raw.Bearing.astype("float32[pyarrow]"),
        Latitude=df_raw.Latitude.astype("float64"),
        Longitude=df_raw.Longitude.astype("float64"),
    )


def _graphql_to_df_raw(vehicles_json: list[dict]) -> pd.DataFrame:
    """Converts GraphQL JSON list to a flattened DataFrame.

    Maps GraphQL fields to SIRI-style column names for parity.
    """
    flattened = []
    for v in vehicles_json:
        line = v.get("line") or {}
        sj = v.get("serviceJourney") or {}
        loc = v.get("location") or {}
        codespace = v.get("codespace") or {}

        item = {
            "RecordedAtTime": v.get("lastUpdated"),
            "VehicleRef": v.get("vehicleId"),
            "Latitude": loc.get("latitude"),
            "Longitude": loc.get("longitude"),
            "Bearing": v.get("bearing"),
            "Delay": v.get("delay"),
            "VehicleStatus": v.get("vehicleStatus"),
            "VehicleMode": v.get("mode"),
            "LineRef": line.get("lineRef"),
            "PublishedLineName": line.get("publicCode"),
            "LineName": line.get("lineName"),
            "OriginName": v.get("originName"),
            "DestinationName": v.get("destinationName"),
            "DataSource": codespace.get("codespaceId"),
            "DatedVehicleJourneyRef": sj.get("id"),
            "DataFrameRef": sj.get("date"),
        }
        flattened.append(item)

    return pd.DataFrame(flattened).convert_dtypes(dtype_backend="pyarrow")
