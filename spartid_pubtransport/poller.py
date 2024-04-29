import logging
import os
import time

import sqlalchemy
from requests.exceptions import ConnectionError
from sqlalchemy.types import TIMESTAMP, BigInteger, Double, String

from spartid_pubtransport import vehiclemonitoring

logger = logging.getLogger(__name__)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG
)

db_schema = {
    "DataFrameRef": String(50),
    "DatedVehicleJourneyRef": String(100),
    "RecordedAtTime": TIMESTAMP,
    "LineRef": String(100),
    "VehicleMode": String(20),
    "Delay": BigInteger,
    "DataSource": String(5),
    "Latitude": Double,
    "Longitude": Double,
}

if __name__ == "__main__":
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "SQLALCHEMY_DATABASE_URI", "sqlite:///data/entur.db"
    )
    logger.info("{SQLALCHEMY_DATABASE_URI=}")
    engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI, echo=True)
    while True:
        try:
            logger.debug("Polling")
            df_raw = vehiclemonitoring.get_vehicles()
            logger.info(df_raw.dtypes)
            df = df_raw[
                [
                    "DataFrameRef",
                    "DatedVehicleJourneyRef",
                    "RecordedAtTime",
                    "LineRef",
                    "VehicleMode",
                    "Delay",
                    "Latitude",
                    "Longitude",
                ]
            ]
            df.to_sql(
                name="VEHICLE_MONITORING",
                con=engine,
                if_exists="append",
                dtype=db_schema,
            )
            time.sleep(1 * 60)
        except ConnectionError:
            logger.warning("Got requests connection error", exc_info=True)
