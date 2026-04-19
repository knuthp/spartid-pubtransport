import argparse
import datetime
import json
import zoneinfo
from pathlib import Path

import duckdb
import requests

DUCKDB_PATH = "data/siri_et.duckdb"
STOPS_PATH = "data/stops.parquet"
ROUTES_PATH = "data/gtfs/parquet/routes.parquet"
OUT_PATH = "data/positions_interpolated.geojson"


def run_interpolate(time_str=None, output_path=OUT_PATH, con=None):
    """
    Interpolate vehicle positions and save as GeoJSON.
    If con is provided, it uses that connection and does NOT close it.
    """
    if time_str:
        try:
            now = datetime.datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except ValueError as e:
            print(f"Invalid time format: {e}")
            return
    else:
        now = datetime.datetime.now(tz=zoneinfo.ZoneInfo("Europe/Oslo"))

    now_str = now.isoformat()

    db_path = Path(DUCKDB_PATH)
    if not db_path.exists():
        print(f"Database not found at {DUCKDB_PATH}. Run fetch_et_to_duckdb.py first.")
        return

    stops_path = Path(STOPS_PATH)
    if not stops_path.exists():
        print(f"Stops file not found at {STOPS_PATH}.")
        url = "https://huggingface.co/datasets/knuthp/GTFS_Entur/resolve/main/stops.parquet?download=true"
        output_file = Path(STOPS_PATH)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        response = requests.get(url, stream=True)
        response.raise_for_status()

        with output_file.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        print("Downloaded stops.parquet")

    routes_path = Path(ROUTES_PATH)
    if not routes_path.exists():
        print(f"Routes file not found at {ROUTES_PATH}. Downloading GTFS...")
        from spartid_pubtransport.gtfs import GtfsDownloader

        downloader = GtfsDownloader()
        downloader.download_and_convert(["routes"])

    should_close = False
    if con is None:
        con = duckdb.connect(str(db_path))
        should_close = True

    try:
        con.execute("INSTALL spatial; LOAD spatial;")

        # Load stops and routes from parquet
        con.execute(
            f"CREATE OR REPLACE VIEW stops AS SELECT * FROM read_parquet('{STOPS_PATH}')"
        )
        con.execute(
            f"CREATE OR REPLACE VIEW routes AS SELECT route_id, route_type FROM read_parquet('{ROUTES_PATH}')"
        )

        # Get column names of stops to handle different formats
        cols = [c[0] for c in con.execute("DESCRIBE stops").fetchall()]
        lat_proj = "stop_lat" if "stop_lat" in cols else "ST_Y(ST_GeomFromWKB(geometry))"
        lon_proj = "stop_lon" if "stop_lon" in cols else "ST_X(ST_GeomFromWKB(geometry))"

        query = f"""
        WITH enriched_calls AS (
            SELECT
                c.*,
                {lat_proj} as stop_lat,
                {lon_proj} as stop_lon,
                COALESCE(expected_arrival_time, aimed_arrival_time) as arrival_time,
                COALESCE(expected_departure_time, aimed_departure_time) as departure_time,
                FIRST_VALUE("order") OVER (
                    PARTITION BY dataframe_ref, dated_vehicle_journey_ref ORDER BY "order"
                ) as first_order,
                LAST_VALUE("order") OVER (
                    PARTITION BY dataframe_ref, dated_vehicle_journey_ref ORDER BY "order"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as last_order,
                s.stop_name as stop_name
            FROM calls c
            JOIN stops s ON c.stop_point_ref = s.stop_id
        ),
        in_transit AS (
            SELECT
                c1.dataframe_ref,
                c1.dated_vehicle_journey_ref as journey_ref,
                c1.line_ref,
                c1.vehicle_ref,
                (
                    epoch('{now_str}'::TIMESTAMP) - epoch(c1.departure_time)
                ) / NULLIF(
                    epoch(c2.arrival_time) - epoch(c1.departure_time), 0
                ) as progress,
                c1.stop_lat + (c2.stop_lat - c1.stop_lat) * progress as lat,
                c1.stop_lon + (c2.stop_lon - c1.stop_lon) * progress as lon,
                'IN_TRANSIT' as status,
                c1.stop_name,
                c2.stop_name as next_stop_name,
                FALSE as is_stationary
            FROM enriched_calls c1
            JOIN enriched_calls c2 ON
                c1.dataframe_ref = c2.dataframe_ref AND
                c1.dated_vehicle_journey_ref = c2.dated_vehicle_journey_ref AND
                c1."order" < c2."order"
            -- Find the two consecutive stops where the vehicle is currently between
            WHERE '{now_str}'::TIMESTAMP > c1.departure_time
              AND '{now_str}'::TIMESTAMP < c2.arrival_time
              AND NOT EXISTS (
                  SELECT 1 FROM enriched_calls c3
                  WHERE c3.dataframe_ref = c1.dataframe_ref
                    AND c3.dated_vehicle_journey_ref = c1.dated_vehicle_journey_ref
                    AND c3."order" > c1."order" AND c3."order" < c2."order"
              )
        ),
        at_stop AS (
            SELECT
                dataframe_ref,
                dated_vehicle_journey_ref as journey_ref,
                line_ref,
                vehicle_ref,
                0 as progress,
                stop_lat as lat,
                stop_lon as lon,
                'AT_STOP' as status,
                TRUE as is_stationary,
                stop_name,
                '' as next_stop_name
            FROM enriched_calls
            WHERE '{now_str}'::TIMESTAMP >= arrival_time
              AND '{now_str}'::TIMESTAMP <= departure_time
        ),
        at_start AS (
            SELECT
                dataframe_ref,
                dated_vehicle_journey_ref as journey_ref,
                line_ref,
                vehicle_ref,
                0 as progress,
                stop_lat as lat,
                stop_lon as lon,
                'AT_START' as status,
                TRUE as is_stationary,
                stop_name,
                '' as next_stop_name
            FROM enriched_calls
            WHERE "order" = first_order
              AND '{now_str}'::TIMESTAMP >= (
                  COALESCE(arrival_time, departure_time) - INTERVAL 5 MINUTE
              )
              AND '{now_str}'::TIMESTAMP < COALESCE(arrival_time, departure_time)
        ),
        at_end AS (
            SELECT
                dataframe_ref,
                dated_vehicle_journey_ref as journey_ref,
                line_ref,
                vehicle_ref,
                0 as progress,
                stop_lat as lat,
                stop_lon as lon,
                'AT_END' as status,
                TRUE as is_stationary,
                stop_name,
                '' as next_stop_name
            FROM enriched_calls
            WHERE "order" = last_order
              AND '{now_str}'::TIMESTAMP > COALESCE(departure_time, arrival_time)
              AND '{now_str}'::TIMESTAMP <= (
                  COALESCE(departure_time, arrival_time) + INTERVAL 5 MINUTE
              )
        ),
        combined AS (
            SELECT * FROM in_transit
            UNION ALL SELECT * FROM at_stop
            UNION ALL SELECT * FROM at_start
            UNION ALL SELECT * FROM at_end
        )
        SELECT
            c.*,
            CASE
                WHEN r.route_type IN (0) OR (r.route_type >= 900 AND r.route_type < 1000) THEN 'tram'
                WHEN r.route_type IN (1) OR (r.route_type >= 400 AND r.route_type < 500) THEN 'subway'
                WHEN r.route_type IN (2) OR (r.route_type >= 100 AND r.route_type < 200) THEN 'rail'
                WHEN r.route_type IN (4) OR (r.route_type >= 1000 AND r.route_type < 1100) THEN 'ferry'
                ELSE 'bus'
            END as vehicle_mode
        FROM combined c
        LEFT JOIN routes r ON c.line_ref = r.route_id
        """

        print(f"Interpolating positions for {now_str}...")
        # Replacing NaN with None to ensure valid JSON output
        df = con.execute(query).df().replace({float("nan"): None})

        if df.empty:
            print("No vehicles found for the specified time.")
            return

        features = []
        for _, row in df.iterrows():
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row["lon"], row["lat"]]
                },
                "properties": {
                    "dataframe_ref": row["dataframe_ref"],
                    "journey_ref": row["journey_ref"],
                    "line_ref": row["line_ref"],
                    "vehicle_ref": row["vehicle_ref"],
                    "status": row["status"],
                    "is_stationary": bool(row["is_stationary"]),
                    "estimated_at": now_str,
                    "stop_name": row["stop_name"],
                    "next_stop_name": row["next_stop_name"],
                    "progress": row["progress"],
                    "vehicle_mode": row["vehicle_mode"],
                }
            }
            features.append(feature)

        geojson = {
            "type": "FeatureCollection",
            "features": features
        }

        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            # allow_nan=False to catch any remaining serialization issues
            json.dump(geojson, f, ensure_ascii=False, indent=2, allow_nan=False)

        print(f"Exported {len(features)} vehicle positions to {out_path}")
    finally:
        if should_close:
            con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Interpolate vehicle positions from SIRI ET data in DuckDB."
    )
    parser.add_argument(
        "--time",
        help="ISO timestamp for interpolation (default: current UTC time)",
    )
    parser.add_argument(
        "--output",
        default=OUT_PATH,
        help=f"Output GeoJSON file path (default: {OUT_PATH})",
    )
    args = parser.parse_args()

    run_interpolate(time_str=args.time, output_path=args.output)


if __name__ == "__main__":
    main()
