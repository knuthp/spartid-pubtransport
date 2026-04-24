import asyncio
import contextlib
import os
import time
from pathlib import Path

import duckdb
import geopandas
import pandas as pd
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from shapely.geometry import Point

from spartid_pubtransport import vehiclemonitoring
from spartid_pubtransport.api.db_utils import get_con, init_tables
from spartid_pubtransport.api.migrate_to_ducklake import migrate
from spartid_pubtransport.api.fetch_et_to_duckdb import run_fetch
from spartid_pubtransport.api.interpolate_vehicle_positions import (
    OUT_PATH,
    run_interpolate,
)

def fetch_and_store_vm(store_history=True):
    con = get_con()
    try:
        print(f"Fetching Vehicle Monitoring data (store_history={store_history})...")
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
        df_latest = (
            df.sort_values("RecordedAtTime")
            .drop_duplicates(subset=pk_cols, keep="last")
            .copy()
        )

        # 1. Append to history (all records) - every 60s
        if store_history:
            con.register("df_history", df)
            con.execute(f"""
                INSERT INTO vehicle_monitoring ({", ".join(df.columns)})
                SELECT * FROM df_history
            """)
            con.unregister("df_history")

        # 2. Update latest positions (deduplicated) - every 10s
        con.register("df_latest_staging", df_latest)
        con.execute(f"""
            INSERT INTO vehicle_monitoring_latest (
                {", ".join(df_latest.columns)}
            )
            SELECT * FROM df_latest_staging
            ON CONFLICT (VehicleRef, DatedVehicleJourneyRef)
            DO UPDATE SET
                RecordedAtTime = EXCLUDED.RecordedAtTime,
                Latitude = EXCLUDED.Latitude,
                Longitude = EXCLUDED.Longitude,
                Delay = EXCLUDED.Delay,
                VehicleStatus = EXCLUDED.VehicleStatus,
                Bearing = EXCLUDED.Bearing,
                LineRef = EXCLUDED.LineRef,
                PublishedLineName = EXCLUDED.PublishedLineName
            WHERE EXCLUDED.RecordedAtTime > vehicle_monitoring_latest.RecordedAtTime
        """)
        con.unregister("df_latest_staging")

        # Cleanup LATEST table from very old entries (e.g. > 1 hour)
        con.execute(
            "DELETE FROM vehicle_monitoring_latest WHERE RecordedAtTime < now() - interval '1 hour'"
        )

        print(f"Stored {len(df)} VM records.")
    except Exception as e:
        print(f"Error in fetch_and_store_vm: {e}")


async def background_scheduler():
    """
    Background task that replicates the logic of runner.sh:
    - Every 60 seconds: Fetch ET data for RUT and BRA.
    - Every 10 seconds: Fetch VM data for all (latest updates).
    - Every 60 seconds: Store VM data to history.
    - Every 3 seconds: Interpolate vehicle positions.
    """
    last_fetch_et = 0
    last_fetch_vm = 0
    last_store_vm_history = 0

    # Init DuckLake and migrate
    def init_dl():
        con = get_con()
        init_tables(con)
        migrate()

    await asyncio.to_thread(init_dl)

    while True:
        try:
            now = time.time()

            # Every 60 seconds: ET Fetch
            if now - last_fetch_et >= 60:
                print("Background task: Fetching ET data...")

                def fetch_et_tasks():
                    con = get_con()
                    run_fetch(dataset_id="RUT", con=con)
                    run_fetch(dataset_id="BRA", con=con)

                await asyncio.to_thread(fetch_et_tasks)
                last_fetch_et = now

            # Every 10 seconds: VM Fetch
            if now - last_fetch_vm >= 10:
                store_history = False
                if now - last_store_vm_history >= 60:
                    store_history = True
                    last_store_vm_history = now

                await asyncio.to_thread(fetch_and_store_vm, store_history=store_history)
                last_fetch_vm = now

            # Every 3 seconds: Interpolate
            def interpolate_task():
                con = get_con()
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
    def query_vm():
        con = get_con(read_only=True)
        # Only positions from last 3 minutes
        df = con.execute("""
            SELECT * FROM vehicle_monitoring_latest
            WHERE RecordedAtTime > now() - interval '3 minutes'
        """).df()
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
