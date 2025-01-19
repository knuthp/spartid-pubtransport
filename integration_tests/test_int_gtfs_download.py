import pytest

from spartid_pubtransport.gtfs import GtfsDownloader


@pytest.fixture
def gtfs_downloader(tmp_path):
    return GtfsDownloader(gtfs_root=tmp_path / "gtfs")

def test_download_success(gtfs_downloader):
    gtfs_downloader.download_and_convert()
