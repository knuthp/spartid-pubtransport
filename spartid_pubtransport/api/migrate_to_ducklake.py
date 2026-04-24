import os
import duckdb
import pandas as pd
from spartid_pubtransport.api.db_utils import get_con, init_tables
from spartid_pubtransport.api.fetch_et_to_duckdb import DUCKDB_PATH

def migrate():
    # 1. Connect to DuckLake
    dl_con = get_con()
    init_tables(dl_con)

    # 2. Migrate from Postgres if SQLALCHEMY_DATABASE_URI is set
    pg_uri = os.getenv("SQLALCHEMY_DATABASE_URI")
    if pg_uri:
        # Check if migration already happened for vehicle_monitoring
        count = dl_con.execute("SELECT count(*) FROM vehicle_monitoring").fetchone()[0]
        if count == 0:
            print("Migrating from Postgres...")
            try:
                dl_con.execute(f"ATTACH '{pg_uri}' AS old_pg (TYPE postgres)")

                # Migrate vehicle_monitoring
                # Explicit column names for robustness
                cols_vm = [
                    "DataFrameRef", "DatedVehicleJourneyRef", "RecordedAtTime",
                    "LineRef", "VehicleMode", "Delay", "DataSource",
                    "Latitude", "Longitude", "VehicleRef", "PublishedLineName",
                    "OriginName", "DestinationName", "VehicleStatus", "Bearing"
                ]
                print("Migrating vehicle_monitoring...")
                dl_con.execute(f"""
                    INSERT INTO vehicle_monitoring ({", ".join(cols_vm)})
                    SELECT {", ".join([f'"{c}"' for c in cols_vm])} FROM old_pg.VEHICLE_MONITORING
                """)

                # Migrate vehicle_monitoring_latest
                print("Migrating vehicle_monitoring_latest...")
                dl_con.execute(f"""
                    INSERT INTO vehicle_monitoring_latest ({", ".join(cols_vm)})
                    SELECT {", ".join([f'"{c}"' for c in cols_vm])} FROM old_pg.VEHICLE_MONITORING_LATEST
                    ON CONFLICT (VehicleRef, DatedVehicleJourneyRef) DO NOTHING
                """)

                dl_con.execute("DETACH old_pg")
                print("Postgres migration complete.")
            except Exception as e:
                print(f"Postgres migration failed: {e}")
        else:
            print(f"vehicle_monitoring already has {count} records, skipping Postgres migration.")

    # 3. Migrate from local DuckDB
    if os.path.exists(DUCKDB_PATH):
        # Check if migration already happened for calls
        count_calls = dl_con.execute("SELECT count(*) FROM calls").fetchone()[0]
        if count_calls == 0:
            print(f"Migrating from local DuckDB: {DUCKDB_PATH}...")
            try:
                dl_con.execute(f"ATTACH '{DUCKDB_PATH}' AS old_duck (TYPE duckdb)")

                # Migrate calls
                cols_calls = [
                    "dataframe_ref", "dated_vehicle_journey_ref", "stop_point_ref",
                    "order", "line_ref", "direction_ref", "vehicle_ref",
                    "recorded_at_time", "aimed_arrival_time", "expected_arrival_time",
                    "actual_arrival_time", "aimed_departure_time", "expected_departure_time",
                    "actual_departure_time", "is_recorded"
                ]
                print("Migrating calls...")
                dl_con.execute(f"""
                    INSERT INTO calls ({", ".join(cols_calls)})
                    SELECT {", ".join([f'"{c}"' for c in cols_calls])} FROM old_duck.calls
                    ON CONFLICT (dataframe_ref, dated_vehicle_journey_ref, stop_point_ref, "order") DO NOTHING
                """)

                dl_con.execute("DETACH old_duck")
                print("Local DuckDB migration complete.")
            except Exception as e:
                print(f"Local DuckDB migration failed: {e}")
        else:
            print(f"calls already has {count_calls} records, skipping local DuckDB migration.")

if __name__ == "__main__":
    migrate()
