import asyncio
import contextlib
import time
from pathlib import Path

import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from spartid_pubtransport.api.fetch_et_to_duckdb import DUCKDB_PATH, run_fetch
from spartid_pubtransport.api.interpolate_vehicle_positions import OUT_PATH, run_interpolate

# Lock for synchronizing database access
db_lock = asyncio.Lock()


async def background_scheduler():
    """
    Background task that replicates the logic of runner.sh:
    - Every 60 seconds: Fetch ET data for RUT and BRA.
    - Every 3 seconds: Interpolate vehicle positions.
    """
    last_fetch = 0
    db_path = Path(DUCKDB_PATH)

    while True:
        try:
            now = time.time()

            # Every 60 seconds
            if now - last_fetch >= 60:
                async with db_lock:
                    print("Background task: Fetching ET data...")

                    # Run synchronous DB operations in a separate thread
                    def fetch_tasks():
                        with duckdb.connect(str(db_path)) as con:
                            run_fetch(dataset_id="RUT", con=con)
                            run_fetch(dataset_id="BRA", con=con)

                    await asyncio.to_thread(fetch_tasks)
                last_fetch = now

            # Every 3 seconds
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


@app.get("/health")
async def health():
    return {"ok": True}
