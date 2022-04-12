import pytest
import requests
import os
from urllib.request import url2pathname
import datetime
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, create_engine, Session

from isb_lib.sitemaps.sitemap_fetcher import (
    SitemapIndexFetcher,
    SitemapFileFetcher,
    ThingFetcher,
)


class LocalFileSitemapIndexFetcher(SitemapIndexFetcher):
    def prepare_sitemap_file_url(self, file_url: str) -> str:
        """Overridden to reconstruct file urls by subbing in the cwd plus the relative file path"""
        filename = os.path.join(os.getcwd(), file_url)
        return f"file://{filename}"

    def sitemap_file_fetcher(self, url: str) -> SitemapFileFetcher:
        return LocalFileSitemapFileFetcher(
            self.prepare_sitemap_file_url(url),
            self._authority,
            self._last_modified,
            self._session,
        )


class LocalFileSitemapFileFetcher(SitemapFileFetcher):
    def prepare_thing_file_url(self, file_url: str) -> str:
        """Overridden to reconstruct file urls by subbing in the cwd plus the relative file path"""
        filename = os.path.join(os.getcwd(), file_url)
        return f"file://{filename}"


# Taken from https://stackoverflow.com/questions/10123929/fetch-a-file-from-a-local-url-with-python-requests#22989322
class LocalFileAdapter(requests.adapters.BaseAdapter):
    """Protocol Adapter to allow Requests to GET file:// URLs"""

    @staticmethod
    def _chkpath(method, path):
        """Return an HTTP status for the given filesystem path."""
        if method.lower() in ("put", "delete"):
            return 501, "Not Implemented"  # TODO
        elif method.lower() not in ("get", "head"):
            return 405, "Method Not Allowed"
        elif os.path.isdir(path):
            return 400, "Path Not A File"
        elif not os.path.isfile(path):
            return 404, "File Not Found"
        elif not os.access(path, os.R_OK):
            return 403, "Access Denied"
        else:
            return 200, "OK"

    def send(self, req, **kwargs):  # pylint: disable=unused-argument
        """Return the file specified by the given request

        @type req: C{PreparedRequest}
        @todo: Should I bother filling `response.headers` and processing
               If-Modified-Since and friends using `os.stat`?
        """
        path = os.path.normcase(os.path.normpath(url2pathname(req.path_url)))
        response = requests.Response()

        response.status_code, response.reason = self._chkpath(req.method, path)
        if response.status_code == 200 and req.method.lower() != "head":
            try:
                response.raw = open(path, "rb")
            except (OSError, IOError) as err:
                response.status_code = 500
                response.reason = str(err)

        if isinstance(req.url, bytes):
            response.url = req.url.decode("utf-8")
        else:
            response.url = req.url

        response.request = req
        response.connection = self

        return response

    def close(self):
        pass


sitemap_fetcher_test_values = [
    ("test_data/sitemaps/test_sitemap_index.xml", None, 2),
    (
        "test_data/sitemaps/test_sitemap_index.xml",
        datetime.datetime(year=2015, month=11, day=22, hour=12, minute=34),
        2,
    ),
    (
        "test_data/sitemaps/test_sitemap_index.xml",
        datetime.datetime(year=2020, month=11, day=22, hour=12, minute=34),
        1,
    ),
]


@pytest.fixture
def local_file_requests_session():
    requests_session = requests.session()
    requests_session.mount("file://", LocalFileAdapter())
    return requests_session


@pytest.fixture(name="sqlmodel_session")
def session_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.mark.parametrize(
    "sitemap_filename,last_mod_date,expected_num_urls", sitemap_fetcher_test_values
)
def test_sitemap_fetcher(
    sitemap_filename: str,
    last_mod_date: datetime.datetime,
    expected_num_urls: int,
    local_file_requests_session,
    sqlmodel_session,
):
    filename = os.path.join(os.getcwd(), sitemap_filename)
    sitemap_index_file = f"file://{filename}"
    sitemap_fetcher = LocalFileSitemapIndexFetcher(
        sitemap_index_file, "OPENCONTEXT", last_mod_date, local_file_requests_session
    )
    sitemap_fetcher.fetch_index_file()
    assert expected_num_urls == len(sitemap_fetcher.urls_to_fetch)
    # This kicks off the request for each child file
    child_fetchers = sitemap_fetcher.fetch_child_files()
    for child_fetcher in child_fetchers:
        # Note that this data has been doctored to have the same number of qualified urls for the sitemap index
        # and the individual child files, so this assertion is valid -- otherwise this wouldn't necessarily work
        assert expected_num_urls == len(child_fetcher.urls_to_fetch)


def test_thing_fetcher_thing_identifier():
    thing_fetcher = ThingFetcher(
        "https://mars.cyverse.org/thing/ark:/65665/3cb09f2ef-0548-4670-b99c-d4a60bd750c3?full=true&format=original"
    )
    thing_identifier = thing_fetcher.thing_identifier()
    assert "ark:/65665/3cb09f2ef-0548-4670-b99c-d4a60bd750c3" == thing_identifier
