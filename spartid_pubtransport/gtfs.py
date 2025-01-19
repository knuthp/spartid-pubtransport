import logging
import urllib.request
from pathlib import Path
from zipfile import ZipFile

import duckdb
import geopandas

GTFS_URL = "https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_norway-aggregated-gtfs.zip"
GTFS_ROOT = Path().resolve() / "data/gtfs"
TABLE_NAMES = [
    "feed_info",
    "stop_times",
    "calendar",
    "shapes",
    "agency",
    "transfers",
    "stops",
    "trips",
    "calendar_dates",
    "routes",
]
logger = logging.getLogger(__name__)


class GtfsDownloader:
    def __init__(self, gtfs_url: str = GTFS_URL, gtfs_root: Path = GTFS_ROOT) -> None:
        self.gtfs_url = gtfs_url
        self.gtfs_root = gtfs_root
        self.gtfs_parquet_root = self.gtfs_root / "parquet"
        self.fresh_download = False

    def download(self) -> Path:
        local_gtfs_zip = self.gtfs_root / Path(self.gtfs_url).name
        if not local_gtfs_zip.exists() and not self.fresh_download:
            logger.info(f"Downloading GTFS data from {self.gtfs_url} to {local_gtfs_zip}")
            local_gtfs_zip.parent.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(self.gtfs_url, local_gtfs_zip)
            logger.info(f"Downloaded GTFS data to {local_gtfs_zip}")
        return local_gtfs_zip

    def extract(self, local_gtfs_zip: Path) -> ZipFile:
        myzip = ZipFile(local_gtfs_zip)
        return myzip

    def convert_table(self, csv: duckdb.DuckDBPyRelation, parquet_file: Path) -> None:
        duckdb.sql(
            f"""COPY(SELECT * FROM csv) TO '{parquet_file}' (FORMAT 'parquet'); """
        )

    def download_and_convert(self, table_names: list[str] | None = None) -> None:
        if table_names is None:
            table_names = TABLE_NAMES

        local_gtfs_zip = self.download()
        myzip = self.extract(local_gtfs_zip)
        for table_name in table_names:
            parquet_file = self.gtfs_parquet_root / f"{table_name}.parquet"
            if parquet_file.exists() and not self.fresh_download:
                logger.info(f"Skipping {table_name} as {parquet_file} already exists")
                continue
            csv_file = myzip.extract(f"{table_name}.txt", path=self.gtfs_parquet_root)
            logger.info(f"Extracted {csv_file} from {local_gtfs_zip}")
            csv = duckdb.read_csv(csv_file)
            logger.info(f"Read {csv_file} into DuckDB")
            self.convert_table(csv, parquet_file)
            Path(csv_file).unlink()


class GtfsShapesSimplifier:
    def __init__(self, gtfs_parquet_root: Path = GTFS_ROOT / "parquet") -> None:
        self.gtfs_parquet_root = gtfs_parquet_root
        self.fresh_download = False

    def simplify_shapes(self) -> None:
        shapes_parquet_simplified = self.gtfs_parquet_root / "shapes_linestring_simple.parquet"
        if shapes_parquet_simplified.exists() and not self.fresh_download:
            logger.info(f"Skipping shapes simplification as {shapes_parquet_simplified} already exists")
            return

        duckdb.install_extension("SPATIAL")
        duckdb.load_extension("SPATIAL")

        shapes_parquet = self.gtfs_parquet_root / "shapes.parquet"
        shapes = duckdb.read_parquet(str(shapes_parquet))  # noqa: F841
        shapes_as_point = duckdb.sql(  # noqa: F841
            """
                SELECT
                    shape_id, shape_pt_sequence, shape_dist_traveled, ST_Point(shape_pt_lon, shape_pt_lat) AS geometry
                FROM shapes;
            """
        )
        duckdb.sql(
            f"""
                COPY (
                    SELECT
                        shape_id, MAX(shape_pt_sequence), MAX(shape_dist_traveled ), ST_SimplifyPreserveTopology(ST_MakeLine(list(geometry ORDER BY shape_pt_sequence ASC)), 0.001) as geometry
                    FROM (SELECT * FROM shapes_as_point)
                    GROUP BY shape_id
                ) TO '{self.gtfs_parquet_root / "shapes_linestring_simple.parquet"}';
            """
        )
