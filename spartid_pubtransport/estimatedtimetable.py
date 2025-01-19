from pathlib import Path

import duckdb
import geopandas
import pandas as pd
import requests
from lxml import etree

from spartid_pubtransport.siri import PROVIDERS, nsmap

exclude_vm_providers = ",".join([key for key,value in PROVIDERS.items() if value["rt"] == "VM"])


def get_vehicles(gtfs_parquet_root: Path):
    df_stops = get_stops(gtfs_parquet_root)
    resp = requests.get(f"https://api.entur.io/realtime/v1/rest/et?excludedDatasetIds={exclude_vm_providers}")
    assert resp.ok
    df_et_raw = _siri_et_to_df(resp.content)
    df_last_stop_with_info = (df_et_raw
            .pipe(_convert_datatypes)
            .pipe(_get_last_stops)
            .pipe(lambda df1: _merge_stop_info(df1, df_stops))
    )
    return to_geopandas(df_last_stop_with_info)


def get_stops(gtfs_root: Path):
    """Get stops adn related position.

    Use downloaded GTFS data.
    """
    print(gtfs_root)
    assert gtfs_root.exists()
    stops = duckdb.read_parquet(str(gtfs_root / "stops.parquet"))
    return stops.to_df()


def _siri_et_to_df(data: bytes):
    """SIRI ET returns XML data, convert to DataFrame format."""
    prstree = etree.fromstring(data)
    all_items = []

    for journey in prstree.iter(etree.QName(nsmap["siri"], "EstimatedVehicleJourney")):
        journey_level1_dict = {
            etree.QName(x).localname: x.text for x in journey if x.text is not None
        }

        if (dated_vehicle_ref := journey.find(
            etree.QName(nsmap["siri"], "DatedVehicleJourneyRef")
        )) is not None:
            data_str = dated_vehicle_ref.text.split(":")
            framed_vehicle_dict = {
                "DataFrameRef": data_str[-1],
                "DatedVehicleJourneyRef": data_str[0],
            }
        elif (estimated_vehicle_journey_code := journey.find(
            etree.QName(nsmap["siri"], "EstimatedVehicleJourneyCode")
        )) is not None:
            data_str = estimated_vehicle_journey_code.text
            framed_vehicle_dict = {
                "DataFrameRef": data_str,
                "DatedVehicleJourneyRef": data_str,
            }
        else:
            framed_vehicle_ref = journey.find(
                etree.QName(nsmap["siri"], "FramedVehicleJourneyRef")
            )
            framed_vehicle_dict = {
                etree.QName(x).localname: x.text
                for x in framed_vehicle_ref
                if x.text is not None
            }
        journey_dict = journey_level1_dict | framed_vehicle_dict
        for estimated in journey.iter(etree.QName(nsmap["siri"], "EstimatedCall")):
            estimated_dict = {
                etree.QName(x).localname: x.text for x in estimated
            } | journey_dict
            estimated_dict["XType"] = "EstimatedCall"
            all_items.append(estimated_dict)
        for recorded in journey.iter(etree.QName(nsmap["siri"], "RecordedCall")):
            recorded_dict = {
                etree.QName(x).localname: x.text for x in recorded
            } | journey_dict
            recorded_dict["XType"] = "RecordedCall"
            all_items.append(recorded_dict)

    return pd.DataFrame(all_items).convert_dtypes(dtype_backend="pyarrow")


def _convert_datatypes(df_raw: pd.DataFrame):
    """Convert to more friendly data analysis datatypes."""
    return df_raw.assign(
        Order=df_raw.Order.astype("int64[pyarrow]"),
        AimedDepartureTime=pd.to_datetime(df_raw.AimedDepartureTime, format="ISO8601"),
        ActualDepartureTime=pd.to_datetime(df_raw.ActualDepartureTime, format="ISO8601"),
        ExpectedDepartureTime=pd.to_datetime(
            df_raw.ExpectedDepartureTime, format="ISO8601"
        ),
        ExpectedArrivalTime=pd.to_datetime(df_raw.ExpectedArrivalTime, format="ISO8601"),
        RecordedAtTime=pd.to_datetime(df_raw.RecordedAtTime, format="ISO8601"),
    )


def _get_last_stops(df_et: pd.DataFrame):
    """Finds the last stop a vehicle has performed.

    This is last known position.
    """
    return (
        df_et.query("Order > 1")
        .query("XType == 'RecordedCall'")
        .sort_values("Order")
        .groupby(["DatedVehicleJourneyRef"])
        .last()
    )


def _merge_stop_info(df_last_stop: pd.DataFrame, df_stops: pd.DataFrame):
    """Merge last stop stop_id with stop info to get Latitude/Longitude."""
    return (
        df_last_stop
            .merge(df_stops, left_on="StopPointRef", right_on="stop_id")
            .dropna(axis="columns")
    )


def to_geopandas(df_last_stop_with_info: pd.DataFrame):
    """Convert dataframe to Geopandas."""
    return geopandas.GeoDataFrame(
        df_last_stop_with_info,
        geometry=geopandas.points_from_xy(df_last_stop_with_info["stop_lon"], df_last_stop_with_info["stop_lat"]),
        crs="EPSG:4326",
    )
