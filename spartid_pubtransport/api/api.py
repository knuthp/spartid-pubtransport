import asyncio
import contextlib
import os
import time
from pathlib import Path

import duckdb
import geopandas
import pandas as pd
import sqlalchemy
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from shapely.geometry import Point
from sqlalchemy import text

from spartid_pubtransport import vehiclemonitoring
from spartid_pubtransport.api.fetch_et_to_duckdb import DUCKDB_PATH, run_fetch
from spartid_pubtransport.api.interpolate_vehicle_positions import (
    OUT_PATH,
    run_interpolate,
)

# Lock for synchronizing database access
db_lock = asyncio.Lock()

SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")
engine = None
if SQLALCHEMY_DATABASE_URI:
    engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)


def init_postgres():
    if not engine:
        return
    with engine.begin() as conn:
        # Create tables if they don't exist
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS VEHICLE_MONITORING (
                "DataFrameRef" VARCHAR(50),
                "DatedVehicleJourneyRef" VARCHAR(100),
                "RecordedAtTime" TIMESTAMP,
                "LineRef" VARCHAR(100),
                "VehicleMode" VARCHAR(20),
                "Delay" BIGINT,
                "DataSource" VARCHAR(5),
                "Latitude" DOUBLE PRECISION,
                "Longitude" DOUBLE PRECISION,
                "VehicleRef" VARCHAR(100),
                "PublishedLineName" VARCHAR(100),
                "OriginName" VARCHAR(100),
                "DestinationName" VARCHAR(100),
                "VehicleStatus" VARCHAR(50),
                "Bearing" DOUBLE PRECISION
            )
        """)
        )
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS VEHICLE_MONITORING_LATEST (
                "DataFrameRef" VARCHAR(50),
                "DatedVehicleJourneyRef" VARCHAR(100),
                "RecordedAtTime" TIMESTAMP,
                "LineRef" VARCHAR(100),
                "VehicleMode" VARCHAR(20),
                "Delay" BIGINT,
                "DataSource" VARCHAR(5),
                "Latitude" DOUBLE PRECISION,
                "Longitude" DOUBLE PRECISION,
                "VehicleRef" VARCHAR(100),
                "PublishedLineName" VARCHAR(100),
                "OriginName" VARCHAR(100),
                "DestinationName" VARCHAR(100),
                "VehicleStatus" VARCHAR(50),
                "Bearing" DOUBLE PRECISION,
                PRIMARY KEY ("VehicleRef", "DatedVehicleJourneyRef")
            )
        """)
        )

        # Migration: Ensure DataSource exists in both tables if they were created earlier without it
        for table in ["VEHICLE_MONITORING", "VEHICLE_MONITORING_LATEST"]:
            conn.execute(
                text(
                    f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "DataSource" VARCHAR(5)'
                )
            )

        conn.execute(
            text(
                'CREATE INDEX IF NOT EXISTS idx_vm_recorded_at ON VEHICLE_MONITORING ("RecordedAtTime")'
            )
        )


def fetch_and_store_vm():
    if not engine:
        return

    try:
        print("Fetching Vehicle Monitoring data...")
        df_raw = vehiclemonitoring.get_vehicles()
        cols_both = [
            "DataFrameRef",
            "DatedVehicleJourneyRef",
            "RecordedAtTime",
            "LineRef",
            "VehicleMode",
            "Delay",
            "Latitude",
            "Longitude",
        ]
        cols_latest = [
            *cols_both,
            "DataSource",
            "VehicleRef",
            "PublishedLineName",
            "OriginName",
            "DestinationName",
            "VehicleStatus",
            "Bearing",
        ]
        cols = cols_latest
        # Filter columns that actually exist in df_raw and drop geometry
        existing_cols = [c for c in cols if c in df_raw.columns]
        df = df_raw[existing_cols].copy()

        # Ensure datatypes
        if "Delay" in df.columns:
            # Convert Timedelta to seconds as BigInt
            df["Delay"] = df["Delay"].dt.total_seconds().fillna(0).astype(int)

        # Handle nulls in PK for LATEST table
        df["VehicleRef"] = df["VehicleRef"].fillna("unknown")
        df["DatedVehicleJourneyRef"] = df["DatedVehicleJourneyRef"].fillna("unknown")
        df["Latitude"] = df["Latitude"].astype("float64")
        df["Longitude"] = df["Longitude"].astype("float64")

        # Deduplicate for LATEST table
        pk_cols = ["VehicleRef", "DatedVehicleJourneyRef"]
        duplicates = df[df.duplicated(subset=pk_cols, keep=False)]
        if not duplicates.empty:
            print(f"Duplicates found in VM batch ({len(duplicates)} records):")
            log_cols = [
                c
                for c in pk_cols
                + ["RecordedAtTime", "LineRef", "VehicleMode", "DataSource"]
                if c in df.columns
            ]
            print(
                duplicates[log_cols]
                .sort_values(by=pk_cols + ["RecordedAtTime"])
                .to_string()
            )

        df_latest = df.sort_values("RecordedAtTime").drop_duplicates(
            subset=pk_cols, keep="last"
        )

        with engine.begin() as conn:
            # 1. Append to history (all records)
            df[cols_both].to_sql(
                name="VEHICLE_MONITORING",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=1000,
            )

            # 2. Update latest positions (deduplicated)
            df_latest.to_sql("vm_latest_staging", conn, if_exists="replace", index=False)
            conn.execute(
                text("""
                INSERT INTO VEHICLE_MONITORING_LATEST (
                    "DataFrameRef", "DatedVehicleJourneyRef", "RecordedAtTime",
                    "LineRef", "VehicleMode", "Delay", "DataSource",
                    "Latitude", "Longitude", "VehicleRef", "PublishedLineName",
                    "OriginName", "DestinationName", "VehicleStatus", "Bearing"
                )
                SELECT
                    "DataFrameRef", "DatedVehicleJourneyRef", "RecordedAtTime",
                    "LineRef", "VehicleMode", "Delay", "DataSource",
                    "Latitude", "Longitude", "VehicleRef", "PublishedLineName",
                    "OriginName", "DestinationName", "VehicleStatus", "Bearing"
                FROM vm_latest_staging
                ON CONFLICT ("VehicleRef", "DatedVehicleJourneyRef")
                DO UPDATE SET
                    "RecordedAtTime" = EXCLUDED."RecordedAtTime",
                    "Latitude" = EXCLUDED."Latitude",
                    "Longitude" = EXCLUDED."Longitude",
                    "Delay" = EXCLUDED."Delay",
                    "VehicleStatus" = EXCLUDED."VehicleStatus",
                    "Bearing" = EXCLUDED."Bearing",
                    "LineRef" = EXCLUDED."LineRef",
                    "PublishedLineName" = EXCLUDED."PublishedLineName"
                WHERE EXCLUDED."RecordedAtTime" > VEHICLE_MONITORING_LATEST."RecordedAtTime"
            """)
            )
            # Cleanup staging
            conn.execute(text("DROP TABLE vm_latest_staging"))

            # Optional: Cleanup LATEST table from very old entries (e.g. > 1 hour)
            conn.execute(
                text(
                    "DELETE FROM VEHICLE_MONITORING_LATEST WHERE \"RecordedAtTime\" < now() - interval '1 hour'"
                )
            )

        print(f"Stored {len(df)} VM records.")
    except Exception as e:
        print(f"Error in fetch_and_store_vm: {e}")


async def background_scheduler():
    """
    Background task that replicates the logic of runner.sh:
    - Every 60 seconds: Fetch ET data for RUT and BRA.
    - Every 60 seconds: Fetch VM data for all.
    - Every 3 seconds: Interpolate vehicle positions.
    """
    last_fetch_et = 0
    last_fetch_vm = 0
    db_path = Path(DUCKDB_PATH)

    # Init Postgres tables
    await asyncio.to_thread(init_postgres)

    while True:
        try:
            now = time.time()

            # Every 60 seconds: ET Fetch
            if now - last_fetch_et >= 60:
                async with db_lock:
                    print("Background task: Fetching ET data...")

                    def fetch_et_tasks():
                        with duckdb.connect(str(db_path)) as con:
                            run_fetch(dataset_id="RUT", con=con)
                            run_fetch(dataset_id="BRA", con=con)

                    await asyncio.to_thread(fetch_et_tasks)
                last_fetch_et = now

            # Every 60 seconds: VM Fetch
            if now - last_fetch_vm >= 60:
                # VM storage uses Postgres, so it doesn't need db_lock (which is for DuckDB)
                await asyncio.to_thread(fetch_and_store_vm)
                last_fetch_vm = now

            # Every 3 seconds: Interpolate
            async with db_lock:

                def interpolate_task():
                    with duckdb.connect(str(db_path)) as con:
                        run_interpolate(con=con)

                await asyncio.to_thread(interpolate_task)

        except Exception as e:
            print(f"Error in background scheduler: {e}")

        await asyncio.sleep(3)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background task
    task = asyncio.create_task(background_scheduler())
    yield
    # Clean up
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


app = FastAPI(title="spartid-api", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/positions_interpolated.geojson")
async def get_positions():
    geojson_path = Path(OUT_PATH)
    if not geojson_path.exists():
        return {"error": "Data not yet available"}
    return FileResponse(
        geojson_path,
        media_type="application/geo+json",
        filename="positions_interpolated.geojson",
    )


@app.get("/positions_vm.geojson")
async def get_positions_vm():
    if not engine:
        return {"error": "Database not configured"}

    def query_vm():
        with engine.connect() as conn:
            # Only positions from last 3 minutes
            query = text("""
                SELECT * FROM VEHICLE_MONITORING_LATEST
                WHERE "RecordedAtTime" > now() - interval '3 minutes'
            """)
            df = pd.read_sql(query, conn)
            return df

    try:
        df = await asyncio.to_thread(query_vm)
        if df.empty:
            return {"type": "FeatureCollection", "features": []}
        df["RecordedAtTime"] = df["RecordedAtTime"].astype(
            str
        )  # Convert timestamps to string for JSON serialization
        gdf = geopandas.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df.Longitude, df.Latitude)],
            crs="EPSG:4326",
        )
        return Response(content=gdf.to_json(), media_type="application/geo+json")
    except Exception as e:
        return {"error": str(e)}


@app.get("/health")
async def health():
    return {"ok": True}
