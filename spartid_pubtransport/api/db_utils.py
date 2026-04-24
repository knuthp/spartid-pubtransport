import os
import duckdb
import threading
from urllib.parse import urlparse

_local = threading.local()

def get_ducklake_uri():
    uri = os.getenv("SQLALCHEMY_DATABASE_URI")
    if not uri:
        return None

    # Expected format: postgresql://user:password@host:port/dbname
    try:
        parsed = urlparse(uri)
        dbname = parsed.path.lstrip('/')
        user = parsed.username
        password = parsed.password
        host = parsed.hostname
        port = parsed.port or 5432

        parts = [f"dbname={dbname}"]
        if user: parts.append(f"user={user}")
        if password: parts.append(f"password={password}")
        if host: parts.append(f"host={host}")
        if port: parts.append(f"port={port}")

        return f"ducklake:postgres:{' '.join(parts)}"
    except Exception as e:
        print(f"Error parsing SQLALCHEMY_DATABASE_URI: {e}")
        return None

def get_con(read_only=False):
    # Use thread-local storage to provide one connection per thread
    # Distinguish between read-only and read-write in cache
    cache_key = "con_ro" if read_only else "con_rw"

    con = getattr(_local, cache_key, None)

    # If connection exists but is closed, reset it
    if con:
        try:
            con.execute("SELECT 1")
        except Exception:
            con = None
            setattr(_local, cache_key, None)

    if con is None:
        con = duckdb.connect()
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute("INSTALL spatial; LOAD spatial;")

        uri = get_ducklake_uri()
        data_path = os.getenv("DATA_PATH", "data/ducklake/")

        # Ensure data path exists
        if not os.path.exists(data_path):
            os.makedirs(data_path, exist_ok=True)

        if uri:
            options = f"DATA_PATH '{data_path}'"
            if read_only:
                options += ", READ_ONLY TRUE"
            con.execute(f"ATTACH '{uri}' AS dl ({options})")
            con.execute("USE dl")

        setattr(_local, cache_key, con)

    return con

def init_tables(con):
    # Vehicle Monitoring History
    con.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_monitoring (
            DataFrameRef VARCHAR,
            DatedVehicleJourneyRef VARCHAR,
            RecordedAtTime TIMESTAMP,
            LineRef VARCHAR,
            VehicleMode VARCHAR,
            Delay BIGINT,
            DataSource VARCHAR,
            Latitude DOUBLE,
            Longitude DOUBLE,
            VehicleRef VARCHAR,
            PublishedLineName VARCHAR,
            OriginName VARCHAR,
            DestinationName VARCHAR,
            VehicleStatus VARCHAR,
            Bearing DOUBLE
        )
    """)

    # Vehicle Monitoring Latest
    con.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_monitoring_latest (
            DataFrameRef VARCHAR,
            DatedVehicleJourneyRef VARCHAR,
            RecordedAtTime TIMESTAMP,
            LineRef VARCHAR,
            VehicleMode VARCHAR,
            Delay BIGINT,
            DataSource VARCHAR,
            Latitude DOUBLE,
            Longitude DOUBLE,
            VehicleRef VARCHAR,
            PublishedLineName VARCHAR,
            OriginName VARCHAR,
            DestinationName VARCHAR,
            VehicleStatus VARCHAR,
            Bearing DOUBLE,
            PRIMARY KEY (VehicleRef, DatedVehicleJourneyRef)
        )
    """)

    # ET Calls
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

    # Index for historical queries
    con.execute("CREATE INDEX IF NOT EXISTS idx_vm_recorded_at ON vehicle_monitoring (RecordedAtTime)")
