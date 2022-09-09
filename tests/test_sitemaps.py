import asyncio
import json
import os.path
import tempfile

import lxml

from isb_lib.sitemaps import SitemapIndexEntry, ThingSitemapIndexEntry, UrlSetEntry, ThingUrlSetEntry, \
    write_urlset_file, write_sitemap_index_file, INDEX_XML, build_sitemap
from isb_lib.sitemaps.gh_pages_sitemap import GHPagesSitemapIndexIterator


def test_sitemap_index_entry():
    entry = SitemapIndexEntry("sitemap_0.xml", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_thing_sitemap_index_entry():
    entry = ThingSitemapIndexEntry("sitemap_0.xml", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_url_set_entry():
    entry = UrlSetEntry("ark:/21547/DSz2761.json", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_thing_url_set_entry():
    entry = ThingUrlSetEntry("ark:/21547/DSz2761", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def _assert_file_exists_and_is_xml(path):
    assert os.path.exists(path)
    with open(path) as f:
        text = f.read()
        xmlp = lxml.etree.XMLParser(
            recover=True, remove_comments=True, resolve_entities=False
        )
        root = lxml.etree.fromstring(text.encode("UTF-8"), parser=xmlp)
        assert root is not None
    os.remove(path)


def test_write_urlset_file():
    path = os.path.join(tempfile.gettempdir(), "sitemap-0.xml")
    host = "https://hyde.cyverse.org"
    urls = [UrlSetEntry("ark:/21547/DSz2761.json", "1234556")]
    asyncio.get_event_loop().run_until_complete(write_urlset_file(path, host, urls))
    _assert_file_exists_and_is_xml(path)


def test_write_sitemap_index_file():
    host = "https://hyde.cyverse.org"
    filename = "sitemap_0.xml"
    path = os.path.join(tempfile.gettempdir(), INDEX_XML)
    asyncio.get_event_loop().run_until_complete(write_sitemap_index_file(tempfile.gettempdir(), host, [SitemapIndexEntry(filename, "1234556")]))
    _assert_file_exists_and_is_xml(path)


def test_build_sitemap():
    path = tempfile.mkdtemp()
    dummy_thing = {
        "@id": "123456"
    }
    json_object = json.dumps(dummy_thing, indent=4)
    json_path = os.path.join(path, "thing_1.json")
    with open(json_path, "w") as outfile:
        outfile.write(json_object)
        build_sitemap(path, "https://hyde.cyverse.org", GHPagesSitemapIndexIterator(path))
        _assert_file_exists_and_is_xml(os.path.join(path, INDEX_XML))
