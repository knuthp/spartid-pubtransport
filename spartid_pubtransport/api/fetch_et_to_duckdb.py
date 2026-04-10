import argparse
import datetime
import sys
from pathlib import Path

import duckdb
import pandas as pd
import requests

DATASET_ID_DEFAULT = "RUT"
CLIENT_NAME = "pub-sparetider-et-duckdb"
DUCKDB_PATH = "data/siri_et.duckdb"


def fetch_siri_et(dataset_id):
    """Fetch SIRI ET data from Entur."""
    url = f"https://api.entur.io/realtime/v1/rest/et?datasetId={dataset_id}"
    headers = {
        "ET-Client-Name": CLIENT_NAME,
        "Accept": "application/json",
    }
    print(f"Fetching data from {url}...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def parse_time(time_str):
    """Parse ISO time string to datetime object or None."""
    if not time_str or not isinstance(time_str, str):
        return None
    try:
        # datetime.fromisoformat handles up to 6 digits of fractional seconds.
        # SIRI sometimes provides more.
        if "." in time_str:
            main_part, tz_part = "", ""
            if "+" in time_str:
                main_part, tz_part = time_str.split("+", 1)
                tz_part = "+" + tz_part
            elif "Z" in time_str:
                main_part = time_str.replace("Z", "")
                tz_part = "Z"
            else:
                main_part = time_str

            if "." in main_part:
                prefix, fraction = main_part.split(".", 1)
                main_part = f"{prefix}.{fraction[:6]}"

            time_str = main_part + tz_part

        return datetime.datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    except Exception:
        return None


def extract_calls(siri_json):
    """Extract all calls (Estimated and Recorded) from SIRI ET JSON."""
    rows = []

    try:
        deliveries = (
            siri_json.get("Siri", {})
            .get("ServiceDelivery", {})
            .get("EstimatedTimetableDelivery", [])
        )
    except AttributeError:
        return []

    for delivery in deliveries:
        for frame in delivery.get("EstimatedJourneyVersionFrame", []):
            for evj in frame.get("EstimatedVehicleJourney", []):
                # Journey level info
                fvj_ref = evj.get("FramedVehicleJourneyRef", {})
                dataframe_ref = fvj_ref.get("DataFrameRef", {}).get("value")
                journey_ref = fvj_ref.get("DatedVehicleJourneyRef")
                line_ref = evj.get("LineRef", {}).get("value")
                direction_ref = evj.get("DirectionRef", {}).get("value")
                vehicle_ref = evj.get("VehicleRef", {}).get("value")
                recorded_at = evj.get("RecordedAtTime")

                # Handle RecordedCalls
                recorded_calls_list = evj.get("RecordedCalls", {}).get(
                    "RecordedCall", []
                )
                if isinstance(recorded_calls_list, dict):
                    recorded_calls_list = [recorded_calls_list]

                for call in recorded_calls_list:
                    rows.append(
                        parse_call(
                            call,
                            dataframe_ref,
                            journey_ref,
                            line_ref,
                            direction_ref,
                            vehicle_ref,
                            recorded_at,
                            is_recorded=True,
                        )
                    )

                # Handle EstimatedCalls
                estimated_calls_list = evj.get("EstimatedCalls", {}).get(
                    "EstimatedCall", []
                )
                if isinstance(estimated_calls_list, dict):
                    estimated_calls_list = [estimated_calls_list]

                for call in estimated_calls_list:
                    rows.append(
                        parse_call(
                            call,
                            dataframe_ref,
                            journey_ref,
                            line_ref,
                            direction_ref,
                            vehicle_ref,
                            recorded_at,
                            is_recorded=False,
                        )
                    )

    return rows


def parse_call(
    call,
    dataframe_ref,
    journey_ref,
    line_ref,
    direction_ref,
    vehicle_ref,
    recorded_at,
    is_recorded,
):
    """Helper to parse a single call object."""
    return {
        "dataframe_ref": dataframe_ref,
        "dated_vehicle_journey_ref": journey_ref,
        "stop_point_ref": call.get("StopPointRef", {}).get("value"),
        "order": call.get("Order"),
        "line_ref": line_ref,
        "direction_ref": direction_ref,
        "vehicle_ref": vehicle_ref,
        "recorded_at_time": parse_time(recorded_at),
        "aimed_arrival_time": parse_time(call.get("AimedArrivalTime")),
        "expected_arrival_time": parse_time(call.get("ExpectedArrivalTime")),
        "actual_arrival_time": parse_time(call.get("ActualArrivalTime")),
        "aimed_departure_time": parse_time(call.get("AimedDepartureTime")),
        "expected_departure_time": parse_time(call.get("ExpectedDepartureTime")),
        "actual_departure_time": parse_time(call.get("ActualDepartureTime")),
        "is_recorded": is_recorded,
    }


def init_db(con):
    """Initialize the DuckDB table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS calls (
            dataframe_ref VARCHAR,
            dated_vehicle_journey_ref VARCHAR,
            stop_point_ref VARCHAR,
            "order" INTEGER,
            line_ref VARCHAR,
            direction_ref VARCHAR,
            vehicle_ref VARCHAR,
            recorded_at_time TIMESTAMP,
            aimed_arrival_time TIMESTAMP,
            expected_arrival_time TIMESTAMP,
            actual_arrival_time TIMESTAMP,
            aimed_departure_time TIMESTAMP,
            expected_departure_time TIMESTAMP,
            actual_departure_time TIMESTAMP,
            is_recorded BOOLEAN,
            PRIMARY KEY (
                dataframe_ref,
                dated_vehicle_journey_ref,
                stop_point_ref,
                "order"
            )
        )
    """)


def upsert_calls(con, rows):
    """Insert or update calls in the DuckDB table."""
    if not rows:
        return

    # Use a temporary table for staging
    con.execute("CREATE TEMP TABLE staging AS SELECT * FROM calls LIMIT 0")

    # Insert rows into staging
    df = pd.DataFrame(rows)
    con.register("staging_df", df)
    con.execute("INSERT INTO staging SELECT * FROM staging_df")

    con.execute("""
        INSERT INTO calls
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY dataframe_ref, dated_vehicle_journey_ref, stop_point_ref, "order"
                    ORDER BY is_recorded DESC
                ) as rn
            FROM staging
        ) WHERE rn = 1
        ON CONFLICT (dataframe_ref, dated_vehicle_journey_ref, stop_point_ref, "order")
        DO UPDATE SET
            line_ref = EXCLUDED.line_ref,
            direction_ref = EXCLUDED.direction_ref,
            vehicle_ref = EXCLUDED.vehicle_ref,
            recorded_at_time = EXCLUDED.recorded_at_time,
            aimed_arrival_time = EXCLUDED.aimed_arrival_time,
            expected_arrival_time = EXCLUDED.expected_arrival_time,
            actual_arrival_time = EXCLUDED.actual_arrival_time,
            aimed_departure_time = EXCLUDED.aimed_departure_time,
            expected_departure_time = EXCLUDED.expected_departure_time,
            actual_departure_time = EXCLUDED.actual_departure_time,
            is_recorded = EXCLUDED.is_recorded
        WHERE
            EXCLUDED.is_recorded = TRUE OR calls.is_recorded = FALSE
    """)

    con.execute("DROP TABLE staging")
    con.unregister("staging_df")


def run_fetch(dataset_id=DATASET_ID_DEFAULT, con=None):
    """
    Fetch SIRI ET data and store in DuckDB.
    If con is provided, it uses that connection and does NOT close it.
    """
    # Ensure output directory exists
    db_path = Path(DUCKDB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    siri_data = fetch_siri_et(dataset_id)
    rows = extract_calls(siri_data)
    print(f"Extracted {len(rows)} calls for {dataset_id}.")

    if not rows:
        print(f"No calls found in the response for {dataset_id}.")
        return

    should_close = False
    if con is None:
        con = duckdb.connect(str(db_path))
        should_close = True

    try:
        init_db(con)
        upsert_calls(con, rows)

        count = con.execute("SELECT count(*) FROM calls").fetchone()[0]
        print(f"Total calls in database after {dataset_id}: {count}")
    finally:
        if should_close:
            con.close()


def main():
    desc = "Fetch SIRI ET data and store in DuckDB."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--dataset-id",
        default=DATASET_ID_DEFAULT,
        help=f"Dataset ID to fetch (default: {DATASET_ID_DEFAULT})",
    )
    args = parser.parse_args()

    try:
        run_fetch(args.dataset_id)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        print(f"Critical error occurred: {e}")


if __name__ == "__main__":
    main()
